//! Store-mediated unified and PD flows. Typed producer streams determine reply
//! destinations; request ids correlate outstanding writes across store latency.
use super::kv_store_router::KvStoreRouter;
use super::simple_dp::{SimpleDpPoolConfig, SimpleDpPoolController};
use crate::common::{Request, RequestId, SharedRequests, Time, WorkerId};
use crate::orchestrator::config::KvStorePoolSpec;
use crate::orchestrator::{Flow, OrchAction, WorkerFactory};
use crate::worker::types::*;
use crate::worker::{CostSource, GpuCluster, IterWorker, SharedGpuCluster};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub(crate) struct KvStoreUnifiedFlow<
    W: IterWorker<Msg = RemoteCacheWorkerMsg, Event = RemoteCacheWorkerEvent>,
> {
    requests: SharedRequests,
    pool: SimpleDpPoolController<W>,
    router: KvStoreRouter,
    cluster: SharedGpuCluster,
    writes: HashSet<RequestId>,
    failure: Option<String>,
}
impl<W: IterWorker<Msg = RemoteCacheWorkerMsg, Event = RemoteCacheWorkerEvent>>
    KvStoreUnifiedFlow<W>
{
    pub(crate) fn attach_logger(&mut self, directory: &std::path::Path) {
        self.router.attach_logger(directory);
        self.cluster.borrow_mut().attach_logger(directory);
    }
    pub(crate) fn new<F: WorkerFactory<W>>(
        cfg: &SimpleDpPoolConfig,
        factory: &F,
        spec: &KvStorePoolSpec,
        bytes_per_token: u64,
    ) -> Self {
        let cluster = Rc::new(RefCell::new(GpuCluster::new(
            CostSource::analytic_with_latency(
                spec.bandwidth_gbps,
                Time::from_ms(spec.latency_us / 1000.0),
            ),
        )));
        let pool = SimpleDpPoolController::new(cfg, factory, &cluster);
        let router = KvStoreRouter::new(spec, bytes_per_token, &cluster);
        Self {
            requests: factory.requests().clone(),
            pool,
            router,
            cluster,
            writes: HashSet::new(),
            failure: None,
        }
    }
}
impl<W: IterWorker<Msg = RemoteCacheWorkerMsg, Event = RemoteCacheWorkerEvent>> Flow
    for KvStoreUnifiedFlow<W>
{
    fn on_arrival(&mut self, request: Request) {
        let req = request.core.id;
        self.requests.borrow_mut().insert(request);
        self.pool.admit(req);
    }
    fn cluster(&self) -> &SharedGpuCluster {
        &self.cluster
    }
    fn background_pending(&self) -> usize {
        self.writes.len() + self.router.pending() + self.pool.pending_wakeups()
    }
    fn check_health(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.failure.is_none(),
            "{}",
            self.failure.as_deref().unwrap_or("")
        );
        Ok(())
    }
    fn cancel(&mut self, req: RequestId, now: Time) {
        let session = self.requests.borrow()[req]
            .request
            .definition
            .session
            .session_id();
        if let Some(session) = session {
            self.router.cancel(session, req);
        }
        self.writes.remove(&req);
        self.pool
            .broadcast_now(RemoteCacheWorkerMsg::Cancel { req, at: now });
    }
    fn tick(&mut self, now: Time) -> Vec<OrchAction> {
        let mut actions = Vec::new();
        for event in self.router.tick(now) {
            match event {
                KvStoreEvent::LookupResult {
                    store,
                    origin,
                    req,
                    resident_tokens,
                    store_send_gid,
                } => self.pool.route_msg_now(
                    origin,
                    RemoteCacheWorkerMsg::KvLookupResult {
                        req,
                        resident_tokens,
                        store_send_gid,
                        store_worker: store,
                    },
                ),
                KvStoreEvent::PushComplete {
                    origin,
                    req,
                    tokens,
                    ..
                } => {
                    self.pool.route_msg_now(
                        origin,
                        RemoteCacheWorkerMsg::KvPushComplete {
                            req,
                            tokens,
                            at: now,
                        },
                    );
                    self.writes.remove(&req);
                }
                KvStoreEvent::PushRejected { req, reason, .. } => {
                    self.cancel(req, now);
                    self.failure = Some(format!("distributed KV push rejected for {req:?}: {reason:?}; all stores exhausted"));
                }
                KvStoreEvent::Evicted { .. } => unreachable!("router consumes invalidations"),
            }
        }
        let mut events = Vec::new();
        self.pool.tick_collect(now, &mut events);
        for event in events {
            match event {
                RemoteCacheWorkerEvent::RequestComplete { req, .. } => {
                    self.cluster
                        .borrow()
                        .record_kv_event(crate::log::rows::KvStoreEventEntry {
                            time_ms: now.as_ms(),
                            request_id: req.0,
                            operation: "request-complete",
                            reason: "foreground",
                            background_writes: Some((self.writes.len()) as u64),
                            ..Default::default()
                        });
                    actions.push(OrchAction::Complete { req });
                }
                RemoteCacheWorkerEvent::KvLookup {
                    worker,
                    req,
                    session,
                    requested_tokens,
                } => self.router.lookup(worker, req, session, requested_tokens),
                RemoteCacheWorkerEvent::KvPushOffer {
                    worker,
                    req,
                    session,
                    tokens,
                    send_gid,
                    retention,
                } => {
                    self.writes.insert(req);
                    self.router
                        .push(worker, req, session, tokens, send_gid, retention, now);
                }
            }
        }
        actions
    }
}

pub(crate) struct KvStorePdFlow<P, D>
where
    P: IterWorker<Msg = RemotePdPrefillMsg, Event = RemotePdPrefillEvent>,
    D: IterWorker<Msg = RemotePdDecodeMsg, Event = RemotePdDecodeEvent>,
{
    requests: SharedRequests,
    prefill: SimpleDpPoolController<P>,
    decode: SimpleDpPoolController<D>,
    router: KvStoreRouter,
    cluster: SharedGpuCluster,
    prefill_writes: HashMap<RequestId, WorkerId>,
    decode_writes: HashMap<RequestId, WorkerId>,
    failure: Option<String>,
}
impl<P, D> KvStorePdFlow<P, D>
where
    P: IterWorker<Msg = RemotePdPrefillMsg, Event = RemotePdPrefillEvent>,
    D: IterWorker<Msg = RemotePdDecodeMsg, Event = RemotePdDecodeEvent>,
{
    pub(crate) fn attach_logger(&mut self, directory: &std::path::Path) {
        self.router.attach_logger(directory);
        self.cluster.borrow_mut().attach_logger(directory);
    }
    pub(crate) fn new<PF: WorkerFactory<P>, DF: WorkerFactory<D>>(
        pcfg: &SimpleDpPoolConfig,
        pf: &PF,
        dcfg: &SimpleDpPoolConfig,
        df: &DF,
        spec: &KvStorePoolSpec,
        bytes_per_token: u64,
        cost: CostSource,
    ) -> Self {
        let cluster = Rc::new(RefCell::new(GpuCluster::new(cost)));
        cluster
            .borrow_mut()
            .set_host_transfer_cost(CostSource::analytic_with_latency(
                spec.bandwidth_gbps,
                Time::from_ms(spec.latency_us / 1000.0),
            ));
        let prefill = SimpleDpPoolController::new(pcfg, pf, &cluster);
        let decode = SimpleDpPoolController::new(dcfg, df, &cluster);
        let router = KvStoreRouter::new(spec, bytes_per_token, &cluster);
        Self {
            requests: pf.requests().clone(),
            prefill,
            decode,
            router,
            cluster,
            prefill_writes: HashMap::new(),
            decode_writes: HashMap::new(),
            failure: None,
        }
    }
}
impl<P, D> Flow for KvStorePdFlow<P, D>
where
    P: IterWorker<Msg = RemotePdPrefillMsg, Event = RemotePdPrefillEvent>,
    D: IterWorker<Msg = RemotePdDecodeMsg, Event = RemotePdDecodeEvent>,
{
    fn on_arrival(&mut self, request: Request) {
        let req = request.core.id;
        self.requests.borrow_mut().insert(request);
        self.prefill.admit(req);
    }
    fn cluster(&self) -> &SharedGpuCluster {
        &self.cluster
    }
    fn background_pending(&self) -> usize {
        self.prefill_writes.len()
            + self.decode_writes.len()
            + self.router.pending()
            + self.prefill.pending_wakeups()
            + self.decode.pending_wakeups()
    }
    fn check_health(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.failure.is_none(),
            "{}",
            self.failure.as_deref().unwrap_or("")
        );
        Ok(())
    }
    fn cancel(&mut self, req: RequestId, now: Time) {
        let session = self.requests.borrow()[req]
            .request
            .definition
            .session
            .session_id();
        if let Some(session) = session {
            self.router.cancel(session, req);
        }
        self.prefill_writes.remove(&req);
        self.decode_writes.remove(&req);
        self.prefill
            .broadcast_now(RemotePdPrefillMsg::Cancel { req, at: now });
        self.decode.broadcast_now(RemotePdDecodeMsg::Cancel { req });
    }
    fn tick(&mut self, now: Time) -> Vec<OrchAction> {
        let mut actions = Vec::new();
        let mut pevents = Vec::new();
        self.prefill.tick_collect(now, &mut pevents);
        for event in pevents {
            match event {
                RemotePdPrefillEvent::RequestComplete { req, .. } => {
                    self.cluster
                        .borrow()
                        .record_kv_event(crate::log::rows::KvStoreEventEntry {
                            time_ms: now.as_ms(),
                            request_id: req.0,
                            operation: "request-complete",
                            reason: "foreground",
                            background_writes: Some(
                                (self.prefill_writes.len() + self.decode_writes.len()) as u64,
                            ),
                            ..Default::default()
                        });
                    actions.push(OrchAction::Complete { req });
                }
                RemotePdPrefillEvent::KvLookup {
                    worker,
                    req,
                    session,
                    requested_tokens,
                } => self.router.lookup(worker, req, session, requested_tokens),
                RemotePdPrefillEvent::KvPushOffer {
                    worker,
                    req,
                    session,
                    tokens,
                    send_gid,
                    retention,
                } => {
                    self.prefill_writes.insert(req, worker);
                    self.router
                        .push(worker, req, session, tokens, send_gid, retention, now);
                }
                RemotePdPrefillEvent::StandaloneHandoff {
                    worker,
                    req,
                    send_gid,
                    tokens,
                } => self.decode.admit_msg(RemotePdDecodeMsg::StandaloneHandoff {
                    req,
                    send_gid,
                    tokens,
                    prefill_worker: worker,
                }),
            }
        }
        for event in self.router.tick(now) {
            match event {
                KvStoreEvent::LookupResult {
                    store,
                    origin,
                    req,
                    resident_tokens,
                    store_send_gid,
                } => self.prefill.route_msg_now(
                    origin,
                    RemotePdPrefillMsg::KvLookupResult {
                        req,
                        resident_tokens,
                        store_send_gid,
                        store_worker: store,
                    },
                ),
                KvStoreEvent::PushComplete {
                    store,
                    req,
                    tokens,
                    retention,
                    store_send_gid,
                    ..
                } => {
                    if let Some(origin) = self.prefill_writes.remove(&req) {
                        self.prefill.route_msg_now(
                            origin,
                            RemotePdPrefillMsg::KvPushComplete {
                                req,
                                tokens,
                                at: now,
                            },
                        );
                        if retention == KvRetention::PinnedUntilFetched {
                            self.decode.admit_msg(RemotePdDecodeMsg::Handoff {
                                req,
                                store_send_gid,
                                tokens,
                                store_worker: store,
                            });
                        }
                    } else {
                        let origin = self
                            .decode_writes
                            .remove(&req)
                            .expect("typed decode producer registered its push");
                        self.decode.route_msg_now(
                            origin,
                            RemotePdDecodeMsg::KvPushComplete {
                                req,
                                tokens,
                                at: now,
                            },
                        );
                    }
                }
                KvStoreEvent::PushRejected { req, reason, .. } => {
                    self.cancel(req, now);
                    self.failure = Some(format!("distributed KV push rejected for {req:?}: {reason:?}; all stores exhausted"));
                }
                KvStoreEvent::Evicted { .. } => unreachable!("router consumes invalidations"),
            }
        }
        let mut devents = Vec::new();
        self.decode.tick_collect(now, &mut devents);
        for event in devents {
            match event {
                RemotePdDecodeEvent::RequestComplete { req, .. } => {
                    self.cluster
                        .borrow()
                        .record_kv_event(crate::log::rows::KvStoreEventEntry {
                            time_ms: now.as_ms(),
                            request_id: req.0,
                            operation: "request-complete",
                            reason: "foreground",
                            background_writes: Some(
                                (self.prefill_writes.len() + self.decode_writes.len()) as u64,
                            ),
                            ..Default::default()
                        });
                    actions.push(OrchAction::Complete { req });
                }
                RemotePdDecodeEvent::KvConsumed {
                    req,
                    session,
                    store_worker,
                    ..
                } => self.router.consumed(store_worker, session, req),
                RemotePdDecodeEvent::KvPushOffer {
                    worker,
                    req,
                    session,
                    tokens,
                    send_gid,
                    retention,
                } => {
                    self.decode_writes.insert(req, worker);
                    self.router
                        .push(worker, req, session, tokens, send_gid, retention, now);
                }
                RemotePdDecodeEvent::StandalonePullComplete {
                    req,
                    prefill_worker,
                    ..
                } => self.prefill.route_msg_now(
                    prefill_worker,
                    RemotePdPrefillMsg::KvPushComplete {
                        req,
                        tokens: 0,
                        at: now,
                    },
                ),
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{PoolId, RequestStore, SessionInput};
    use crate::orchestrator::config::KvStorePlacement;
    use crate::orchestrator::{DpPlacementPolicy, UnifiedWorkerFactory};
    use crate::test_helpers::{text_request, FakeModel};
    use crate::worker::kv::PrefixCachePolicy;
    use crate::worker::workers::remote_cache_iter::{
        build_remote_cache_pd_prefill_worker, build_remote_cache_unified_worker,
    };
    use crate::worker::workers::remote_pd_decode::build_remote_cache_pd_decode_worker;
    use std::sync::Arc;

    fn spec() -> KvStorePoolSpec {
        KvStorePoolSpec {
            worker: Default::default(),
            replicas: 2,
            dram_gb: 0.000001,
            backlog_weight_per_ms: 1.0,
            bandwidth_gbps: 0.001,
            latency_us: 100.0,
            placement: KvStorePlacement::RoundRobin,
            policy: PrefixCachePolicy::Lru,
        }
    }
    fn pool(id: u16, workers: u16) -> SimpleDpPoolConfig {
        SimpleDpPoolConfig {
            pool: PoolId(id),
            num_workers: workers,
            placement: DpPlacementPolicy::RoundRobin,
        }
    }
    fn request(id: u32, prefix: u32, output: u32, now: Time) -> Request {
        let mut req = text_request(RequestId(id), 20, output, now);
        req.definition.session = SessionInput::Session {
            session_id: 7,
            session_start_time: Time::ZERO,
            declared_prefix_tokens: prefix,
        };
        req
    }
    fn drive(flow: &mut impl Flow, clock: &mut u64, req: RequestId) {
        for _ in 0..500 {
            let actions = flow.tick(Time::from_us(*clock * 100));
            *clock += 1;
            if actions.contains(&OrchAction::Complete { req }) {
                return;
            }
        }
        panic!("request {req:?} did not complete");
    }
    #[test]
    fn next_turn_can_use_local_prefix_while_shared_writes_are_outstanding() {
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        let factory = UnifiedWorkerFactory::new(
            Arc::new(FakeModel::for_ms(1.0)),
            requests.clone(),
            WorkerConfig {
                attn_kv_bytes: 1000,
                ..Default::default()
            },
            None,
            "test".into(),
            "main",
            build_remote_cache_unified_worker::<FakeModel>,
        );
        let mut slow = spec();
        slow.latency_us = 100_000.0;
        let mut flow = KvStoreUnifiedFlow::new(&pool(0, 1), &factory, &slow, 1);
        let mut clock = 0;
        flow.on_arrival(request(0, 0, 3, Time::ZERO));
        drive(&mut flow, &mut clock, RequestId(0));
        assert_eq!(flow.writes.len(), 1);
        flow.on_arrival(request(1, 20, 3, Time::from_us(clock * 100)));
        drive(&mut flow, &mut clock, RequestId(1));
        assert_eq!(flow.writes.len(), 2);
        assert_eq!(
            requests.borrow()[RequestId(1)]
                .telemetry
                .prefix_cache_hit_tokens,
            Some(20)
        );
        assert!(
            clock < 1000,
            "both completions precede the first store landing"
        );
        for _ in 0..4000 {
            assert!(flow.tick(Time::from_us(clock * 100)).is_empty());
            clock += 1;
            if flow.background_pending() == 0 {
                break;
            }
        }
        assert_eq!(flow.background_pending(), 0);
    }

    #[test]
    fn relay_capacity_exhaustion_is_an_explicit_error_and_single_token_is_background() {
        for output in [1, 3] {
            let requests = Rc::new(RefCell::new(RequestStore::new()));
            let config = WorkerConfig {
                attn_kv_bytes: 1000,
                ..Default::default()
            };
            let pf = UnifiedWorkerFactory::new(
                Arc::new(FakeModel::for_ms(1.0)),
                requests.clone(),
                config,
                None,
                "test".into(),
                "prefill",
                build_remote_cache_pd_prefill_worker::<FakeModel>,
            );
            let df = UnifiedWorkerFactory::new(
                Arc::new(FakeModel::for_ms(1.0)),
                requests,
                config,
                None,
                "test".into(),
                "decode",
                build_remote_cache_pd_decode_worker::<FakeModel>,
            );
            let mut tiny = spec();
            tiny.dram_gb = 0.000000001;
            tiny.latency_us = 100_000.0;
            let mut flow = KvStorePdFlow::new(
                &pool(0, 1),
                &pf,
                &pool(1, 1),
                &df,
                &tiny,
                1,
                CostSource::analytic(1.0),
            );
            flow.on_arrival(request(0, 0, output, Time::ZERO));
            let mut complete = false;
            for step in 0..100 {
                complete |= !flow.tick(Time::from_us(step * 100)).is_empty();
                if complete || flow.check_health().is_err() {
                    break;
                }
            }
            if output == 1 {
                assert!(complete);
                assert!(flow.background_pending() > 0);
                assert!(flow.check_health().is_ok());
            } else {
                assert!(!complete);
                assert!(flow
                    .check_health()
                    .unwrap_err()
                    .to_string()
                    .contains("CapacityExceeded"));
                assert!(flow.prefill_writes.is_empty());
            }
        }
    }

    #[test]
    fn pd_cancel_at_each_protocol_phase_drains_and_allows_next_session_turn() {
        for cancel_step in [0, 10, 11, 12, 13] {
            let requests = Rc::new(RefCell::new(RequestStore::new()));
            let config = WorkerConfig {
                attn_kv_bytes: 1000,
                ..Default::default()
            };
            let pf = UnifiedWorkerFactory::new(
                Arc::new(FakeModel::for_ms(1.0)),
                requests.clone(),
                config,
                None,
                "test".into(),
                "prefill",
                build_remote_cache_pd_prefill_worker::<FakeModel>,
            );
            let df = UnifiedWorkerFactory::new(
                Arc::new(FakeModel::for_ms(1.0)),
                requests,
                config,
                None,
                "test".into(),
                "decode",
                build_remote_cache_pd_decode_worker::<FakeModel>,
            );
            let mut flow = KvStorePdFlow::new(
                &pool(0, 1),
                &pf,
                &pool(1, 1),
                &df,
                &spec(),
                1,
                CostSource::analytic(1.0),
            );
            flow.on_arrival(request(0, 0, 8, Time::ZERO));
            for step in 0..=cancel_step {
                assert!(flow.tick(Time::from_us(step * 100)).is_empty());
            }
            let now = Time::from_us(cancel_step * 100);
            flow.cancel(RequestId(0), now);
            for step in cancel_step..100 {
                assert!(flow.tick(Time::from_us(step * 100)).is_empty());
            }
            assert_eq!(flow.background_pending(), 0, "cancel step {cancel_step}");
            flow.on_arrival(request(1, 0, 3, Time::from_ms(10.0)));
            let mut clock = 100;
            drive(&mut flow, &mut clock, RequestId(1));
            for step in clock..clock + 100 {
                assert!(flow.tick(Time::from_us(step * 100)).is_empty());
            }
            assert_eq!(flow.background_pending(), 0);
            assert!(flow.check_health().is_ok());
        }
    }

    #[test]
    fn unified_shared_prefix_survives_reads_on_different_workers() {
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        let factory = UnifiedWorkerFactory::new(
            Arc::new(FakeModel::for_ms(1.0)),
            requests.clone(),
            WorkerConfig {
                attn_kv_bytes: 1000,
                ..Default::default()
            },
            None,
            "test".into(),
            "main",
            build_remote_cache_unified_worker::<FakeModel>,
        );
        let mut flow = KvStoreUnifiedFlow::new(&pool(0, 3), &factory, &spec(), 1);
        let mut clock = 0;
        for id in 0..3 {
            flow.on_arrival(request(
                id,
                if id == 0 { 0 } else { 20 },
                3,
                Time::from_us(clock * 100),
            ));
            drive(&mut flow, &mut clock, RequestId(id));
            assert!(flow.background_pending() > 0);
            for _ in 0..500 {
                if flow.background_pending() == 0 {
                    break;
                }
                assert!(flow.tick(Time::from_us(clock * 100)).is_empty());
                clock += 1;
            }
            assert_eq!(flow.background_pending(), 0);
        }
        assert_eq!(flow.cluster.borrow().num_gpus(), 3);
        let requests = requests.borrow();
        for id in 1..3 {
            assert_eq!(
                requests[RequestId(id)].telemetry.prefix_cache_hit_tokens,
                Some(20)
            );
            assert_eq!(
                requests[RequestId(id)].progress.prefill_tokens_processed,
                20
            );
        }
    }
    #[test]
    fn pd_relay_unpins_and_decode_push_is_not_relayed_again() {
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        let config = WorkerConfig {
            attn_kv_bytes: 1000,
            ..Default::default()
        };
        let pf = UnifiedWorkerFactory::new(
            Arc::new(FakeModel::for_ms(1.0)),
            requests.clone(),
            config,
            None,
            "test".into(),
            "prefill",
            build_remote_cache_pd_prefill_worker::<FakeModel>,
        );
        let df = UnifiedWorkerFactory::new(
            Arc::new(FakeModel::for_ms(1.0)),
            requests.clone(),
            config,
            None,
            "test".into(),
            "decode",
            build_remote_cache_pd_decode_worker::<FakeModel>,
        );
        let mut flow = KvStorePdFlow::new(
            &pool(0, 2),
            &pf,
            &pool(1, 1),
            &df,
            &spec(),
            1,
            CostSource::analytic(1.0),
        );
        let dir = tempfile::tempdir().unwrap();
        flow.attach_logger(dir.path());
        let mut clock = 0;
        for id in 0..4 {
            flow.on_arrival(request(
                id,
                if id == 0 { 0 } else { 20 },
                4,
                Time::from_us(clock * 100),
            ));
            drive(&mut flow, &mut clock, RequestId(id));
            assert!(flow.background_pending() > 0);
            for _ in 0..500 {
                if flow.background_pending() == 0 {
                    break;
                }
                assert!(flow.tick(Time::from_us(clock * 100)).is_empty());
                clock += 1;
            }
            assert_eq!(flow.background_pending(), 0);
            assert_eq!(
                requests.borrow()[RequestId(id)]
                    .progress
                    .output_tokens_emitted,
                4
            );
        }
        assert!(flow.prefill_writes.is_empty() && flow.decode_writes.is_empty());
        assert_eq!(
            requests.borrow()[RequestId(1)]
                .telemetry
                .prefix_cache_hit_tokens,
            Some(20)
        );
        drop(flow);
        let path = dir.path().join("raw/kv_store_event.parquet");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(path).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let mut pins = 0;
        let mut unpins = 0;
        let mut previous = HashMap::new();
        for batch in reader {
            let batch = batch.unwrap();
            let ops = batch
                .column_by_name("operation")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let stores = batch
                .column_by_name("store_worker")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt16Array>()
                .unwrap();
            let before = batch
                .column_by_name("cache_used_before")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let after = batch
                .column_by_name("cache_used_after")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let eb = batch
                .column_by_name("entry_tokens_before")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let ea = batch
                .column_by_name("entry_tokens_after")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                match ops.value(i) {
                    "pin" => pins += 1,
                    "unpin" => unpins += 1,
                    "retain" | "evict" => {
                        assert_eq!(after.value(i), before.value(i) - eb.value(i) + ea.value(i));
                        assert_eq!(
                            *previous.entry(stores.value(i)).or_insert(0),
                            before.value(i)
                        );
                        previous.insert(stores.value(i), after.value(i));
                    }
                    _ => {}
                }
            }
        }
        assert_eq!((pins, unpins), (4, 4));
    }

    #[test]
    fn standalone_unified_matches_local_worker_token_times_and_actions() {
        use crate::orchestrator::{SimpleDpConfig, SimpleDpFlow};
        use crate::worker::build_hp_worker;
        let local_requests = Rc::new(RefCell::new(RequestStore::new()));
        let remote_requests = Rc::new(RefCell::new(RequestStore::new()));
        let config = WorkerConfig {
            log_output_token_times: true,
            attn_kv_bytes: 100,
            max_batch_tokens: Some(24),
            ..Default::default()
        };
        let model = Arc::new(FakeModel {
            ms: 0.3,
            dp_groups: 2,
        });
        let local_factory = UnifiedWorkerFactory::new(
            model.clone(),
            local_requests.clone(),
            config,
            None,
            "test".into(),
            "main",
            build_hp_worker::<FakeModel>,
        );
        let remote_factory = UnifiedWorkerFactory::new(
            model,
            remote_requests.clone(),
            config,
            None,
            "test".into(),
            "main",
            build_remote_cache_unified_worker::<FakeModel>,
        );
        let mut local = SimpleDpFlow::new(
            SimpleDpConfig {
                dp_pool: pool(0, 2),
            },
            local_factory,
        );
        let mut remote = KvStoreUnifiedFlow::new(&pool(0, 2), &remote_factory, &spec(), 1);
        for step in 0..200 {
            let now = Time::from_us(step * 100);
            if step % 13 == 0 && step < 100 {
                let req = text_request(RequestId((step / 13) as u32), 20, 4, now);
                local.on_arrival(req.clone());
                remote.on_arrival(req);
            }
            assert_eq!(local.tick(now), remote.tick(now), "step {step}");
        }
        for id in 0..8 {
            let local = local_requests.borrow();
            let remote = remote_requests.borrow();
            assert_eq!(
                local[RequestId(id)].telemetry.first_output_time,
                remote[RequestId(id)].telemetry.first_output_time
            );
            assert_eq!(
                local[RequestId(id)].progress.output_tokens_emitted,
                remote[RequestId(id)].progress.output_tokens_emitted
            );
        }
    }

    #[test]
    fn standalone_pd_matches_direct_link_and_single_token_completes_on_prefill() {
        use crate::orchestrator::PdFlow;
        use crate::worker::{build_pd_decode_worker, build_pd_prefill_worker};
        let a = Rc::new(RefCell::new(RequestStore::new()));
        let b = Rc::new(RefCell::new(RequestStore::new()));
        let config = WorkerConfig {
            attn_kv_bytes: 1000,
            ..Default::default()
        };
        let model = Arc::new(FakeModel::for_ms(1.0));
        let pa = UnifiedWorkerFactory::new(
            model.clone(),
            a.clone(),
            config,
            None,
            "test".into(),
            "prefill",
            build_pd_prefill_worker::<FakeModel>,
        );
        let da = UnifiedWorkerFactory::new(
            model.clone(),
            a,
            config,
            None,
            "test".into(),
            "decode",
            build_pd_decode_worker::<FakeModel>,
        );
        let pb = UnifiedWorkerFactory::new(
            model.clone(),
            b.clone(),
            config,
            None,
            "test".into(),
            "prefill",
            build_remote_cache_pd_prefill_worker::<FakeModel>,
        );
        let db = UnifiedWorkerFactory::new(
            model,
            b,
            config,
            None,
            "test".into(),
            "decode",
            build_remote_cache_pd_decode_worker::<FakeModel>,
        );
        let mut local = PdFlow::new(
            &pool(0, 1),
            &pa,
            &pool(1, 1),
            &da,
            CostSource::analytic(1.0),
        );
        let mut remote = KvStorePdFlow::new(
            &pool(0, 1),
            &pb,
            &pool(1, 1),
            &db,
            &spec(),
            1,
            CostSource::analytic(1.0),
        );
        for step in 0..200 {
            let now = Time::from_us(step * 100);
            if step == 0 || step == 90 {
                let req = text_request(
                    RequestId(u32::from(step > 0)),
                    20,
                    if step == 0 { 4 } else { 1 },
                    now,
                );
                local.on_arrival(req.clone());
                remote.on_arrival(req);
            }
            assert_eq!(local.tick(now), remote.tick(now), "step {step}");
        }
    }
}
