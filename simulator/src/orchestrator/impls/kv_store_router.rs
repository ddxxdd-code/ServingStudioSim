//! L6a store placement, session directory, wakeup filtering and targeted replies.
//! Directory misses synthesize replies without waking a store. Receipt-driven
//! evictions invalidate only the matching store, never a newer copy elsewhere.
use crate::common::{PoolId, RequestId, Time, WorkerId};
use crate::log::kv_store_logger::{KvStoreLogger, SharedKvStoreLogger};
use crate::log::rows::KvStoreEventEntry;
use crate::orchestrator::config::{KvStorePlacement, KvStorePoolSpec};
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::iter_worker::{IterWorker, KvStoreLoad};
use crate::worker::types::{KvPushRejectReason, KvRetention, KvStoreEvent, KvStoreMsg};
use crate::worker::workers::kv_store::{build_kv_store_worker, KvStoreWorker};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone, Copy)]
struct KvLocation {
    store: WorkerId,
    tokens: u64,
}
struct PendingPush {
    message: KvStoreMsg,
    candidates: VecDeque<usize>,
}

pub(crate) struct KvStoreRouter {
    logger: Option<SharedKvStoreLogger>,
    cluster: SharedGpuCluster,
    workers: Vec<KvStoreWorker>,
    wakeups: Vec<Option<Time>>,
    directory: HashMap<u32, KvLocation>,
    pushes: HashMap<RequestId, PendingPush>,
    cancelled: HashSet<RequestId>,
    placement: KvStorePlacement,
    backlog_weight_per_ms: f64,
    next: usize,
    replies: Vec<KvStoreEvent>,
}
impl KvStoreRouter {
    pub(crate) fn new(
        spec: &KvStorePoolSpec,
        bytes_per_token: u64,
        cluster: &SharedGpuCluster,
    ) -> Self {
        let workers = (0..spec.replicas)
            .map(|id| match spec.worker {
                crate::worker::config::KvStoreWorkerSel::DistributedKvStore => {
                    build_kv_store_worker(
                        WorkerId(id),
                        PoolId(2),
                        (spec.dram_gb * 1e9) as u64,
                        bytes_per_token,
                        spec.policy,
                        cluster.clone(),
                    )
                }
            })
            .collect();
        Self {
            logger: None,
            cluster: cluster.clone(),
            workers,
            wakeups: vec![None; spec.replicas as usize],
            directory: HashMap::new(),
            pushes: HashMap::new(),
            cancelled: HashSet::new(),
            placement: spec.placement,
            backlog_weight_per_ms: spec.backlog_weight_per_ms,
            next: 0,
            replies: Vec::new(),
        }
    }
    pub(crate) fn attach_logger(&mut self, directory: &std::path::Path) {
        let logger = KvStoreLogger::open(directory);
        for worker in &mut self.workers {
            worker.logger = Some(logger.clone());
        }
        self.cluster.borrow_mut().kv_store_logger = Some(logger.clone());
        self.logger = Some(logger);
    }
    fn route(&mut self, worker: WorkerId, msg: KvStoreMsg) {
        self.workers[worker.0 as usize].enqueue(msg);
        // Incoming protocol must be handled now even if a transfer is in flight.
        self.wakeups[worker.0 as usize] = Some(Time::ZERO);
    }
    pub(crate) fn lookup(
        &mut self,
        origin: WorkerId,
        req: RequestId,
        session: u32,
        requested_tokens: u32,
    ) {
        if let Some(location) = self.directory.get(&session).filter(|l| l.tokens > 0) {
            let worker = location.store;
            self.route(
                worker,
                KvStoreMsg::Lookup {
                    origin,
                    req,
                    session,
                    requested_tokens,
                },
            );
        } else {
            self.replies.push(KvStoreEvent::LookupResult {
                store: WorkerId(0),
                origin,
                req,
                resident_tokens: 0,
                store_send_gid: 0,
            });
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push(
        &mut self,
        origin: WorkerId,
        req: RequestId,
        session: u32,
        tokens: u64,
        send_gid: u16,
        retention: KvRetention,
        now: Time,
    ) {
        let count = self.workers.len();
        let start = if self.placement == KvStorePlacement::SessionHash {
            session as usize % count
        } else {
            self.next
        };
        let mut candidates: Vec<_> = (0..count).map(|offset| (start + offset) % count).collect();
        if self.placement == KvStorePlacement::LeastLoaded {
            candidates.sort_by(|&a, &b| {
                // Explicit heuristic: occupancy ratio + weighted receive backlog.
                // Capacity is checked authoritatively by each store at reservation.
                let score = |i: usize| {
                    self.workers[i].used_tokens() as f64
                        / self.workers[i].capacity_tokens().max(1) as f64
                        + self.backlog_weight_per_ms * self.workers[i].recv_backlog(now).as_ms()
                };
                score(a).total_cmp(&score(b)).then(a.cmp(&b))
            });
        }
        self.next = (candidates[0] + 1) % count;
        self.pushes.insert(
            req,
            PendingPush {
                message: KvStoreMsg::Push {
                    origin,
                    req,
                    session,
                    tokens,
                    send_gid,
                    retention,
                },
                candidates: candidates.into(),
            },
        );
        let events = self.try_next(req, now, false);
        self.process_events(events, now);
    }

    fn try_next(&mut self, req: RequestId, now: Time, retry: bool) -> Vec<KvStoreEvent> {
        let pending = self.pushes.get_mut(&req).expect("registered push");
        let KvStoreMsg::Push {
            origin,
            session,
            tokens,
            ..
        } = pending.message
        else {
            unreachable!()
        };
        let Some(index) = pending.candidates.pop_front() else {
            self.pushes.remove(&req);
            return vec![KvStoreEvent::PushRejected {
                store: WorkerId(0),
                origin,
                req,
                reason: KvPushRejectReason::CapacityExceeded,
            }];
        };
        let message = pending.message;
        if let Some(logger) = &self.logger {
            logger.borrow_mut().record(KvStoreEventEntry {
                time_ms: now.as_ms(),
                store_worker: index as u16,
                request_id: req.0,
                session_id: session,
                operation: if retry { "retry" } else { "placement" },
                reason: "push",
                requested_tokens: tokens,
                placement_policy: Some(match self.placement {
                    KvStorePlacement::LeastLoaded => "least-loaded",
                    KvStorePlacement::RoundRobin => "round-robin",
                    KvStorePlacement::SessionHash => "session-hash",
                }),
                send_backlog_ms: Some(self.workers[index].send_backlog(now).as_ms()),
                recv_backlog_ms: Some(self.workers[index].recv_backlog(now).as_ms()),
                backlog_weight_per_ms: Some(self.backlog_weight_per_ms),
                occupancy_ratio: Some(
                    self.workers[index].used_tokens() as f64
                        / self.workers[index].capacity_tokens() as f64,
                ),
                ..Default::default()
            });
        }
        self.route(WorkerId(index as u16), message);
        let mut events = Vec::new();
        self.wakeups[index] = self.workers[index].tick(now, &mut events);
        events
    }

    pub(crate) fn pending(&self) -> usize {
        self.pushes.len() + self.replies.len() + self.wakeups.iter().filter(|w| w.is_some()).count()
    }

    pub(crate) fn cancel(&mut self, session: u32, req: RequestId) {
        self.cancelled.insert(req);
        self.pushes.remove(&req);
        self.replies.retain(|event| match event {
            KvStoreEvent::LookupResult { req: r, .. }
            | KvStoreEvent::PushComplete { req: r, .. }
            | KvStoreEvent::PushRejected { req: r, .. } => *r != req,
            KvStoreEvent::Evicted { .. } => true,
        });
        // Broadcast is deliberately identity-guarded at SessionStore: a relay
        // may already have landed or its acknowledgement may still be queued.
        for index in 0..self.workers.len() {
            self.route(WorkerId(index as u16), KvStoreMsg::Cancel { session, req });
        }
    }
    pub(crate) fn consumed(&mut self, store: WorkerId, session: u32, req: RequestId) {
        self.route(store, KvStoreMsg::Consumed { session, req });
    }
    fn process_events(&mut self, events: Vec<KvStoreEvent>, now: Time) {
        let mut events: VecDeque<_> = events.into();
        while let Some(event) = events.pop_front() {
            let req = match event {
                KvStoreEvent::LookupResult { req, .. }
                | KvStoreEvent::PushComplete { req, .. }
                | KvStoreEvent::PushRejected { req, .. } => Some(req),
                KvStoreEvent::Evicted { .. } => None,
            };
            if req.is_some_and(|r| self.cancelled.contains(&r)) {
                continue;
            }
            match event {
                KvStoreEvent::PushComplete {
                    store,
                    session,
                    tokens,
                    req,
                    ..
                } => {
                    self.pushes.remove(&req);
                    if tokens > 0 {
                        self.directory.insert(session, KvLocation { store, tokens });
                    } else if self
                        .directory
                        .get(&session)
                        .is_some_and(|location| location.store == store)
                    {
                        self.directory.remove(&session);
                    }
                    self.replies.push(event);
                }
                KvStoreEvent::Evicted { store, session } => {
                    if self
                        .directory
                        .get(&session)
                        .is_some_and(|location| location.store == store)
                    {
                        self.directory.remove(&session);
                    }
                }
                KvStoreEvent::PushRejected { req, .. } if self.pushes.contains_key(&req) => {
                    events.extend(self.try_next(req, now, true));
                }
                _ => self.replies.push(event),
            }
        }
    }
    pub(crate) fn tick(&mut self, now: Time) -> Vec<KvStoreEvent> {
        let mut events = Vec::new();
        for (worker, wakeup) in self.workers.iter_mut().zip(&mut self.wakeups) {
            if wakeup.is_some_and(|at| at <= now) {
                *wakeup = worker.tick(now, &mut events);
            }
        }
        self.process_events(events, now);
        std::mem::take(&mut self.replies)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::test_cluster;
    use crate::worker::kv::PrefixCachePolicy;
    fn router() -> KvStoreRouter {
        KvStoreRouter::new(
            &KvStorePoolSpec {
                worker: Default::default(),
                replicas: 2,
                dram_gb: 0.001,
                backlog_weight_per_ms: 1.0,
                bandwidth_gbps: 1.0,
                latency_us: 1.0,
                placement: KvStorePlacement::LeastLoaded,
                policy: PrefixCachePolicy::Lru,
            },
            1,
            &test_cluster(),
        )
    }
    #[test]
    fn lookup_observes_push_that_is_due_on_the_same_tick() {
        let cluster = test_cluster();
        let gpu = cluster.borrow_mut().allocate(0, 0, 1, "test", "main");
        let send = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
        let mut router = KvStoreRouter::new(
            &KvStorePoolSpec {
                worker: Default::default(),
                replicas: 1,
                dram_gb: 0.001,
                backlog_weight_per_ms: 1.0,
                bandwidth_gbps: 1.0,
                latency_us: 0.0,
                placement: KvStorePlacement::LeastLoaded,
                policy: PrefixCachePolicy::Lru,
            },
            1,
            &cluster,
        );
        router.push(
            WorkerId(0),
            RequestId(0),
            7,
            20,
            send,
            KvRetention::Shared,
            Time::ZERO,
        );
        router.tick(Time::from_us(10));
        router.push(
            WorkerId(0),
            RequestId(1),
            7,
            80,
            send,
            KvRetention::Shared,
            Time::from_us(10),
        );
        router.lookup(WorkerId(0), RequestId(2), 7, 80);
        let events = router.tick(Time::from_us(20));
        assert!(events.iter().any(|e| matches!(
            e,
            KvStoreEvent::LookupResult {
                req: RequestId(2),
                resident_tokens: 80,
                ..
            }
        )));
    }

    #[test]
    fn queued_pin_rejection_retries_an_untried_store() {
        let cluster = test_cluster();
        let gpu = cluster.borrow_mut().allocate(0, 0, 1, "test", "main");
        let send = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
        let mut router = KvStoreRouter::new(
            &KvStorePoolSpec {
                worker: Default::default(),
                replicas: 2,
                dram_gb: 0.0000001,
                backlog_weight_per_ms: 1.0,
                bandwidth_gbps: 1.0,
                latency_us: 0.0,
                placement: KvStorePlacement::SessionHash,
                policy: PrefixCachePolicy::Lru,
            },
            1,
            &cluster,
        );
        router.push(
            WorkerId(0),
            RequestId(0),
            0,
            20,
            send,
            KvRetention::PinnedUntilFetched,
            Time::ZERO,
        );
        router.tick(Time::from_us(10));
        // Session 0 queues behind its existing pin on store 0.
        router.push(
            WorkerId(0),
            RequestId(1),
            0,
            90,
            send,
            KvRetention::PinnedUntilFetched,
            Time::from_us(10),
        );
        router.push(
            WorkerId(0),
            RequestId(2),
            2,
            80,
            send,
            KvRetention::PinnedUntilFetched,
            Time::from_us(10),
        );
        router.tick(Time::from_us(20));
        router.consumed(WorkerId(0), 0, RequestId(0));
        assert!(router.tick(Time::from_us(20)).is_empty());
        let events = router.tick(Time::from_us(30));
        assert!(matches!(
            events.as_slice(),
            [KvStoreEvent::PushComplete {
                req: RequestId(1),
                store: WorkerId(1),
                tokens: 90,
                ..
            }]
        ));
        assert!(router.pushes.is_empty());
        router.cancel(0, RequestId(1));
        router.tick(Time::from_us(30));
        assert_eq!(router.workers[1].available_pinned_tokens(), 100);
    }

    #[test]
    fn cancel_inflight_push_releases_capacity_and_suppresses_late_ack() {
        let cluster = test_cluster();
        let gpu = cluster.borrow_mut().allocate(0, 0, 1, "test", "main");
        let send = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
        let mut router = KvStoreRouter::new(
            &KvStorePoolSpec {
                worker: Default::default(),
                replicas: 1,
                dram_gb: 0.0000001,
                backlog_weight_per_ms: 1.0,
                bandwidth_gbps: 1.0,
                latency_us: 0.0,
                placement: KvStorePlacement::LeastLoaded,
                policy: PrefixCachePolicy::Lru,
            },
            1,
            &cluster,
        );
        router.push(
            WorkerId(0),
            RequestId(0),
            7,
            100,
            send,
            KvRetention::PinnedUntilFetched,
            Time::ZERO,
        );
        assert_eq!(router.workers[0].available_pinned_tokens(), 0);
        router.cancel(7, RequestId(0));
        assert!(router.tick(Time::ZERO).is_empty());
        assert_eq!(router.workers[0].available_pinned_tokens(), 100);
        assert!(router.tick(Time::from_ms(1.0)).is_empty());
        assert!(!router.directory.contains_key(&7));
    }

    #[test]
    fn directory_miss_never_dispatches_to_store() {
        let mut router = router();
        router.lookup(WorkerId(3), RequestId(1), 42, 100);
        assert!(router.wakeups.iter().all(Option::is_none));
        assert!(matches!(
            router.tick(Time::ZERO)[0],
            KvStoreEvent::LookupResult {
                resident_tokens: 0,
                origin: WorkerId(3),
                ..
            }
        ));
    }

    #[test]
    fn stale_directory_lookup_returns_an_ordinary_zero_hit() {
        let mut router = router();
        router.directory.insert(
            7,
            KvLocation {
                store: WorkerId(1),
                tokens: 80,
            },
        );
        router.lookup(WorkerId(3), RequestId(9), 7, 100);
        assert!(router.wakeups[1].is_some());
        assert!(matches!(
            router.tick(Time::ZERO).as_slice(),
            [KvStoreEvent::LookupResult {
                store: WorkerId(1),
                origin: WorkerId(3),
                req: RequestId(9),
                resident_tokens: 0,
                ..
            }]
        ));
    }

    #[test]
    fn concurrent_copies_survive_eviction_and_writeback_during_the_other_read() {
        use crate::common::{RequestStore, SessionInput};
        use crate::test_helpers::{text_request, FakeModel};
        use crate::worker::gpu_cluster::{CostSource, GpuCluster};
        use crate::worker::types::{
            RemoteCacheWorkerEvent as Event, RemoteCacheWorkerMsg as Msg, WorkerConfig,
        };
        use crate::worker::workers::remote_cache_iter::build_remote_cache_unified_worker;
        use std::{cell::RefCell, rc::Rc, sync::Arc};
        // One byte/ms makes both snapshot reads overlap a replacement push.
        let cluster = Rc::new(RefCell::new(GpuCluster::new(CostSource::analytic(
            0.000001,
        ))));
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        requests.borrow_mut().reserve_slots(3);
        let mut workers: Vec<_> = (0..2)
            .map(|id| {
                let mut req =
                    text_request(RequestId(id + 1), 10 * (id + 1), 1, Time::from_ms(100.0));
                req.definition.session = SessionInput::Session {
                    session_id: 7,
                    declared_prefix_tokens: 80,
                    session_start_time: Time::ZERO,
                };
                requests.borrow_mut().insert(req);
                build_remote_cache_unified_worker(
                    WorkerId(id as u16),
                    "main",
                    Arc::new(FakeModel::for_ms(1.0)),
                    requests.clone(),
                    WorkerConfig::default(),
                    None,
                    PoolId(0),
                    "test",
                    cluster.clone(),
                )
            })
            .collect();
        let seed_gpu = cluster.borrow_mut().allocate(9, 0, 1, "seed", "seed");
        let seed_gid = cluster
            .borrow_mut()
            .register_comm_group(seed_gpu, 1, "seed", 0);
        let mut router = KvStoreRouter::new(
            &KvStorePoolSpec {
                worker: Default::default(),
                replicas: 1,
                dram_gb: 0.0000001,
                bandwidth_gbps: 0.000001,
                latency_us: 0.0,
                placement: KvStorePlacement::LeastLoaded,
                backlog_weight_per_ms: 1.0,
                policy: PrefixCachePolicy::Lru,
            },
            1,
            &cluster,
        );
        router.push(
            WorkerId(9),
            RequestId(0),
            7,
            80,
            seed_gid,
            KvRetention::Shared,
            Time::ZERO,
        );
        router.tick(Time::from_ms(100.0));
        for (index, worker) in workers.iter_mut().enumerate() {
            worker.enqueue(Msg::Request(RequestId(index as u32 + 1)));
            let mut events = Vec::new();
            worker.tick(Time::from_ms(100.0), &mut events);
            for event in events {
                let Event::KvLookup {
                    worker,
                    req,
                    session,
                    requested_tokens,
                } = event
                else {
                    panic!("lookup expected")
                };
                router.lookup(worker, req, session, requested_tokens);
            }
        }
        for event in router.tick(Time::from_ms(100.0)) {
            let KvStoreEvent::LookupResult {
                store,
                origin,
                req,
                resident_tokens,
                store_send_gid,
            } = event
            else {
                panic!("lookup reply expected")
            };
            assert_eq!(resident_tokens, 80);
            workers[origin.0 as usize].enqueue(Msg::KvLookupResult {
                req,
                resident_tokens,
                store_send_gid,
                store_worker: store,
            });
        }
        for worker in &mut workers {
            worker.tick(Time::from_ms(100.0), &mut Vec::new());
        }
        assert!(requests.borrow()[RequestId(1)]
            .telemetry
            .prefix_cache_hit_tokens
            .is_none());
        router.push(
            WorkerId(9),
            RequestId(99),
            8,
            80,
            seed_gid,
            KvRetention::Shared,
            Time::from_ms(101.0),
        );
        assert!(
            !router.directory.contains_key(&7),
            "source evicted while both reads are in flight"
        );
        let mut wrote_during_second_read = false;
        for ms in 102..650 {
            let now = Time::from_ms(ms as f64);
            for event in router.tick(now) {
                if let KvStoreEvent::PushComplete {
                    origin,
                    req,
                    tokens,
                    ..
                } = event
                {
                    if req.0 == 1 || req.0 == 2 {
                        workers[origin.0 as usize].enqueue(Msg::KvPushComplete {
                            req,
                            tokens,
                            at: now,
                        });
                    }
                }
            }
            for worker in &mut workers {
                let mut events = Vec::new();
                worker.tick(now, &mut events);
                for event in events {
                    if let Event::KvPushOffer {
                        worker,
                        req,
                        session,
                        tokens,
                        send_gid,
                        retention,
                    } = event
                    {
                        if req == RequestId(1) {
                            wrote_during_second_read = requests.borrow()[RequestId(2)]
                                .telemetry
                                .prefix_cache_hit_tokens
                                .is_none();
                        }
                        router.push(worker, req, session, tokens, send_gid, retention, now);
                    }
                }
            }
        }
        assert!(wrote_during_second_read);
        for id in 1..=2 {
            let store = requests.borrow();
            assert!(store[RequestId(id)].lifecycle.completed);
            assert_eq!(
                store[RequestId(id)].telemetry.prefix_cache_hit_tokens,
                Some(80)
            );
            assert_eq!(
                store[RequestId(id)].progress.prefill_tokens_processed,
                id * 10
            );
        }
        assert_eq!(router.directory[&7].tokens, 100);
        assert_eq!(router.pending(), 0);
    }

    #[test]
    fn push_placement_uses_configurable_receive_backlog_not_sender_backlog() {
        // An incoming transfer occupies store 0's receive link; an outgoing
        // transfer occupies store 1's send link. Equal empty stores isolate cost.
        for (weight, expected) in [(0.0, 0), (1.0, 1), (10.0, 1)] {
            let cluster = test_cluster();
            let gpu = cluster.borrow_mut().allocate(0, 0, 2, "test", "main");
            let send = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
            let recv = cluster
                .borrow_mut()
                .register_comm_group(gpu + 1, 1, "main", 1);
            let mut config = KvStorePoolSpec {
                worker: Default::default(),
                replicas: 2,
                dram_gb: 0.001,
                bandwidth_gbps: 1.0,
                latency_us: 0.0,
                placement: KvStorePlacement::LeastLoaded,
                policy: PrefixCachePolicy::Lru,
                backlog_weight_per_ms: weight,
            };
            for invalid in [-1.0, f64::INFINITY, f64::NAN] {
                config.backlog_weight_per_ms = invalid;
                assert!(config.validate(1).is_err());
            }
            config.backlog_weight_per_ms = weight;
            let mut router = KvStoreRouter::new(&config, 1, &cluster);
            // Construction appends exactly one communication group per store.
            let store0 = 2;
            let store1 = 3;
            cluster
                .borrow_mut()
                .submit_transfer(Time::ZERO, send, store0, 1_000_000, "test", "");
            cluster
                .borrow_mut()
                .submit_transfer(Time::ZERO, store1, recv, 1_000_000, "test", "");
            router.push(
                WorkerId(0),
                RequestId(0),
                7,
                10,
                send,
                KvRetention::Shared,
                Time::ZERO,
            );
            assert_eq!(router.workers[expected].used_tokens(), 10);
            assert_eq!(router.workers[1 - expected].used_tokens(), 0);
        }
    }
    #[test]
    fn stale_eviction_cannot_remove_newer_store_copy() {
        let mut router = router();
        router.directory.insert(
            7,
            KvLocation {
                store: WorkerId(1),
                tokens: 50,
            },
        );
        router.process_events(
            vec![KvStoreEvent::Evicted {
                store: WorkerId(0),
                session: 7,
            }],
            Time::ZERO,
        );
        assert!(router.directory.contains_key(&7));
        router.process_events(
            vec![KvStoreEvent::Evicted {
                store: WorkerId(1),
                session: 7,
            }],
            Time::ZERO,
        );
        assert!(!router.directory.contains_key(&7));
    }

    #[test]
    fn placement_policies_and_pinned_capacity_are_deterministic() {
        for (placement, expected) in [
            (KvStorePlacement::LeastLoaded, [0, 1]),
            (KvStorePlacement::RoundRobin, [0, 1]),
            (KvStorePlacement::SessionHash, [1, 0]),
        ] {
            let cluster = test_cluster();
            let gpu = cluster.borrow_mut().allocate(0, 0, 1, "test", "main");
            let send = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
            let mut router = KvStoreRouter::new(
                &KvStorePoolSpec {
                    worker: Default::default(),
                    replicas: 2,
                    dram_gb: 0.0000001,
                    backlog_weight_per_ms: 1.0,
                    bandwidth_gbps: 1.0,
                    latency_us: 0.0,
                    placement,
                    policy: PrefixCachePolicy::Lru,
                },
                1,
                &cluster,
            );
            for (index, store) in expected.into_iter().enumerate() {
                router.push(
                    WorkerId(0),
                    RequestId(index as u32),
                    index as u32 + 1,
                    60,
                    send,
                    KvRetention::PinnedUntilFetched,
                    Time::from_us(index as u64 * 10),
                );
                let events = router.tick(Time::from_us((index as u64 + 1) * 10));
                assert!(events.iter().any(|event| matches!(event,
                    KvStoreEvent::PushComplete { store: worker, .. } if worker.0 == store)));
            }
            router.push(
                WorkerId(0),
                RequestId(2),
                3,
                60,
                send,
                KvRetention::PinnedUntilFetched,
                Time::from_us(20),
            );
            assert!(matches!(
                router.tick(Time::from_us(20)).as_slice(),
                [KvStoreEvent::PushRejected {
                    reason: KvPushRejectReason::CapacityExceeded,
                    ..
                }]
            ));
            router.consumed(WorkerId(expected[0]), 1, RequestId(0));
            router.tick(Time::from_us(20));
            router.push(
                WorkerId(0),
                RequestId(3),
                4,
                60,
                send,
                KvRetention::PinnedUntilFetched,
                Time::from_us(20),
            );
            assert!(router.tick(Time::from_us(30)).iter().any(|event| matches!(
                event,
                KvStoreEvent::PushComplete {
                    req: RequestId(3),
                    ..
                }
            )));
        }
    }
}
