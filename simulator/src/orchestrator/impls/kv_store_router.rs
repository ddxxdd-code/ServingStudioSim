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
use std::collections::HashMap;

pub(crate) struct KvStoreRouter {
    logger: Option<SharedKvStoreLogger>,
    workers: Vec<KvStoreWorker>,
    wakeups: Vec<Option<Time>>,
    directory: HashMap<u32, (WorkerId, u64)>,
    placement: KvStorePlacement,
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
            workers,
            wakeups: vec![None; spec.replicas as usize],
            directory: HashMap::new(),
            placement: spec.placement,
            next: 0,
            replies: Vec::new(),
        }
    }
    pub(crate) fn attach_logger(&mut self, directory: &std::path::Path) {
        let logger = KvStoreLogger::open(directory);
        for worker in &mut self.workers {
            worker.logger = Some(logger.clone());
        }
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
        if let Some(&(worker, _)) = self.directory.get(&session) {
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
        let eligible = |worker: &KvStoreWorker| {
            retention == KvRetention::Shared || worker.available_pinned_tokens() >= tokens
        };
        let choice = match self.placement {
            KvStorePlacement::LeastLoaded => self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, w)| eligible(w))
                .min_by(|(ia, a), (ib, b)| {
                    // Equal weights: occupancy ratio plus backlog in milliseconds.
                    let score = |w: &KvStoreWorker| {
                        w.used_tokens() as f64 / w.capacity_tokens() as f64
                            + w.send_backlog(now).as_ms()
                    };
                    score(a).total_cmp(&score(b)).then(ia.cmp(ib))
                })
                .map(|(i, _)| i),
            KvStorePlacement::RoundRobin | KvStorePlacement::SessionHash => {
                let start = if self.placement == KvStorePlacement::SessionHash {
                    session as usize % self.workers.len()
                } else {
                    self.next
                };
                (0..self.workers.len())
                    .map(|offset| (start + offset) % self.workers.len())
                    .find(|&i| eligible(&self.workers[i]))
            }
        };
        let Some(index) = choice else {
            self.replies.push(KvStoreEvent::PushRejected {
                store: WorkerId(0),
                origin,
                req,
                reason: KvPushRejectReason::CapacityExceeded,
            });
            return;
        };
        self.next = (index + 1) % self.workers.len();
        if let Some(logger) = &self.logger {
            logger.borrow_mut().record(KvStoreEventEntry {
                time_ms: now.as_ms(),
                store_worker: index as u16,
                request_id: req.0,
                session_id: session,
                operation: "placement",
                reason: "push",
                requested_tokens: tokens,
                placement_policy: Some(match self.placement {
                    KvStorePlacement::LeastLoaded => "least-loaded",
                    KvStorePlacement::RoundRobin => "round-robin",
                    KvStorePlacement::SessionHash => "session-hash",
                }),
                send_backlog_ms: Some(self.workers[index].send_backlog(now).as_ms()),
                occupancy_ratio: Some(
                    self.workers[index].used_tokens() as f64
                        / self.workers[index].capacity_tokens() as f64,
                ),
                ..Default::default()
            });
        }
        self.route(
            WorkerId(index as u16),
            KvStoreMsg::Push {
                origin,
                req,
                session,
                tokens,
                send_gid,
                retention,
            },
        );
        // Process acceptance immediately, so a subsequent placement sees reserved
        // capacity rather than overbooking all same-tick offers onto one store.
        let mut events = Vec::new();
        self.wakeups[index] = self.workers[index].tick(now, &mut events);
        self.process_events(events);
    }
    pub(crate) fn consumed(&mut self, store: WorkerId, session: u32, req: RequestId) {
        self.route(store, KvStoreMsg::Consumed { session, req });
    }
    fn process_events(&mut self, events: Vec<KvStoreEvent>) {
        for event in events {
            match event {
                KvStoreEvent::PushComplete {
                    store,
                    session,
                    tokens,
                    ..
                } => {
                    if tokens > 0 {
                        self.directory.insert(session, (store, tokens));
                    } else if self
                        .directory
                        .get(&session)
                        .is_some_and(|(w, _)| *w == store)
                    {
                        self.directory.remove(&session);
                    }
                    self.replies.push(event);
                }
                KvStoreEvent::Evicted { store, session } => {
                    if self
                        .directory
                        .get(&session)
                        .is_some_and(|(w, _)| *w == store)
                    {
                        self.directory.remove(&session);
                    }
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
        self.process_events(events);
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
    fn stale_eviction_cannot_remove_newer_store_copy() {
        let mut router = router();
        router.directory.insert(7, (WorkerId(1), 50));
        router.process_events(vec![KvStoreEvent::Evicted {
            store: WorkerId(0),
            session: 7,
        }]);
        assert!(router.directory.contains_key(&7));
        router.process_events(vec![KvStoreEvent::Evicted {
            store: WorkerId(1),
            session: 7,
        }]);
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
