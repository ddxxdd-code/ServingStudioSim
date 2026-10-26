//! RFC step 3: isolate shell/cadence with a synchronous always-miss service.
//! The real remote lifecycle still issues lookups for session requests. The stub
//! supplies zero hits and immediately acknowledges writes, removing network and
//! storage timing so every modeled output can be compared with barebone.
use super::RemoteCacheIterWorker;
use crate::common::{PoolId, RequestId, RequestStore, SharedRequests, Time, WorkerId};
use crate::log::{LoggerSession, PrefixCacheLogger};
use crate::orchestrator::{
    DpPlacementPolicy, SimpleDpConfig, SimpleDpFlow, SimpleDpPoolConfig, UnifiedWorkerFactory,
};
use crate::sim::frontend::{
    ArrivalSchedule, CapacityLimit, InputFileFormat, InputFileSchema, TraceFrontend,
};
use crate::sim::run::{run_sim, TickCfg};
use crate::test_helpers::FakeModel;
use crate::worker::admission::{
    IterAdmission, LoadBalance, PendingOrder, RemoteCachePrefillDecodeAdmission, RemoteKvAdmission,
};
use crate::worker::execution::UnifiedIterExecution;
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::kv::KvCacheStore;
use crate::worker::shared::context::WorkerContext;
use crate::worker::types::{
    RemoteCacheWorkerEvent as Event, RemoteCacheWorkerMsg as Msg, RemoteReadPlan, WorkerConfig,
    WorkerEventCommon, WorkerMsgCommon,
};
use crate::worker::workers::iter::IterBatchWorker;
use crate::worker::workers::iter_build_essentials::{
    kv_cache_store, prepare_iter_build_essentials, resolve_kv_accounting,
};
use std::{
    cell::RefCell,
    fs::File,
    path::PathBuf,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

static MISSES: AtomicUsize = AtomicUsize::new(0);
struct AlwaysMiss(RemoteCachePrefillDecodeAdmission<PendingOrder>);
impl IterAdmission<KvCacheStore> for AlwaysMiss {
    type Msg = WorkerMsgCommon;
    type Event = WorkerEventCommon;
    fn accept_message(&mut self, kv: &mut KvCacheStore, msg: Self::Msg, context: &WorkerContext) {
        let WorkerMsgCommon::Request(req) = msg;
        self.0.accept_message(kv, Msg::Request(req), context);
    }
    fn form_batch(
        &mut self,
        kv: &mut KvCacheStore,
        context: &WorkerContext,
        batch_plan: &mut crate::worker::types::IterBatchPlan,
        now: Time,
    ) -> bool {
        loop {
            let active = self.0.form_batch(kv, context, batch_plan, now);
            let mut lookups = Vec::new();
            <RemoteCachePrefillDecodeAdmission<PendingOrder> as RemoteKvAdmission<KvCacheStore>>::drain_lookups(&mut self.0, context, &mut lookups);
            if lookups.is_empty() {
                return active;
            }
            for event in lookups {
                let Event::KvLookup { req, .. } = event else {
                    panic!("expected lookup")
                };
                MISSES.fetch_add(1, Ordering::Relaxed);
                self.0.accept_message(
                    kv,
                    Msg::KvLookupResult {
                        req,
                        resident_tokens: 0,
                        store_send_gid: 0,
                        store_worker: WorkerId(0),
                    },
                    context,
                );
            }
        }
    }
    fn complete_iteration(
        &mut self,
        kv: &mut KvCacheStore,
        context: &WorkerContext,
        batch_plan: &crate::worker::types::IterBatchPlan,
        events: &mut Vec<Self::Event>,
        now: Time,
    ) {
        let mut remote = Vec::new();
        self.0
            .complete_iteration(kv, context, batch_plan, &mut remote, now);
        for event in remote {
            match event {
                Event::KvPushOffer { req, tokens, .. } => self.0.accept_message(
                    kv,
                    Msg::KvPushComplete {
                        req,
                        tokens,
                        at: now,
                    },
                    context,
                ),
                Event::RequestComplete { worker, req } => {
                    events.push(WorkerEventCommon::RequestComplete { worker, req })
                }
                Event::KvLookup { .. } => unreachable!(),
            }
        }
    }
    fn queued_requests(&self) -> u32 {
        <RemoteCachePrefillDecodeAdmission<PendingOrder> as IterAdmission<KvCacheStore>>::queued_requests(&self.0)
    }
    fn cancel_pending(&mut self, req: RequestId) -> bool {
        <RemoteCachePrefillDecodeAdmission<PendingOrder> as IterAdmission<KvCacheStore>>::cancel_pending(&mut self.0, req)
    }
}
impl RemoteKvAdmission<KvCacheStore> for AlwaysMiss {
    fn drain_reuse(&mut self) -> Vec<crate::log::rows::KvStoreEventEntry> {
        <RemoteCachePrefillDecodeAdmission<PendingOrder> as RemoteKvAdmission<KvCacheStore>>::drain_reuse(&mut self.0)
    }
    fn drain_lookups(&mut self, _: &WorkerContext, _: &mut Vec<Self::Event>) {}
    fn take_read(&mut self, kv: &KvCacheStore) -> Option<(RequestId, RemoteReadPlan)> {
        let plan = self.0.take_read(kv);
        assert!(plan.is_none(), "zero-hit service must never transfer bytes");
        plan
    }
    fn read_landed(&mut self, _: &mut KvCacheStore, _: RequestId, _: Time) {
        panic!("always-miss read")
    }
}

#[allow(clippy::too_many_arguments)]
fn build_stub(
    id: WorkerId,
    tag: &'static str,
    model: Arc<FakeModel>,
    requests: SharedRequests,
    config: WorkerConfig,
    log: Option<PathBuf>,
    pool: PoolId,
    gpu: &str,
    cluster: SharedGpuCluster,
) -> RemoteCacheIterWorker<KvCacheStore, AlwaysMiss, UnifiedIterExecution<FakeModel>> {
    use crate::arch::contract::IterwiseUnifiedModel;
    let accounting = resolve_kv_accounting(&model.kv_cache_layout(), 1, &config);
    let prefix_logger = PrefixCacheLogger::open_opt(log.as_deref(), tag, id);
    let essentials = prepare_iter_build_essentials(
        id,
        tag,
        requests,
        &config,
        log,
        pool,
        gpu,
        &cluster,
        model.gpus_per_replica(),
        model.cost_log_manifest(),
        &accounting,
        1,
    );
    let kv = kv_cache_store(accounting, 1, 1, &config, essentials.sampler, prefix_logger);
    let admission = AlwaysMiss(RemoteCachePrefillDecodeAdmission::new(
        PendingOrder::new(config.pending_order),
        (),
        LoadBalance::Single,
        config.max_batch_tokens,
        16,
        0,
    ));
    RemoteCacheIterWorker::new(
        IterBatchWorker::from_components(
            essentials.context,
            kv,
            admission,
            UnifiedIterExecution::new(model, essentials.cost),
        ),
        cluster,
        1,
    )
}

#[test]
fn always_miss_shell_matches_barebone_on_the_same_chained_session_fixture() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let mut summaries = Vec::new();
    MISSES.store(0, Ordering::Relaxed);
    for (index, dir) in dirs.iter().enumerate() {
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        let cfg = WorkerConfig {
            attn_kv_bytes: 10_000,
            log_stage_transitions: true,
            log_output_token_times: true,
            ..Default::default()
        };
        let model = Arc::new(FakeModel::for_ms(1.0));
        let flow_config = SimpleDpConfig {
            dp_pool: SimpleDpPoolConfig {
                pool: PoolId(0),
                num_workers: 2,
                placement: DpPlacementPolicy::RoundRobin,
            },
        };
        let mut flow: Box<dyn crate::orchestrator::Flow> = if index == 0 {
            Box::new(SimpleDpFlow::new(
                flow_config,
                UnifiedWorkerFactory::new(
                    model,
                    requests.clone(),
                    cfg,
                    None,
                    "test".into(),
                    "main",
                    crate::worker::build_barebone_worker::<FakeModel>,
                ),
            ))
        } else {
            Box::new(SimpleDpFlow::new(
                flow_config,
                UnifiedWorkerFactory::new(
                    model,
                    requests.clone(),
                    cfg,
                    None,
                    "test".into(),
                    "main",
                    build_stub,
                ),
            ))
        };
        let schema = InputFileSchema::new(
            InputFileFormat::TextGenerationSessionExecutionV2,
            Vec::new(),
        )
        .unwrap();
        let trace =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/distributed_kv.csv");
        let mut frontend = TraceFrontend::load(
            &[trace],
            &schema,
            ArrivalSchedule::trace_timed(1.0).unwrap(),
            CapacityLimit::unlimited(),
        )
        .unwrap();
        requests
            .borrow_mut()
            .reserve_slots(frontend.expected_count());
        let mut logger = LoggerSession::open(dir.path(), true, true).unwrap();
        let summary = run_sim(
            flow.as_mut(),
            &requests,
            &mut frontend,
            &mut logger,
            &TickCfg::new(100.0, true, 1),
        )
        .unwrap();
        let mut summary = serde_json::to_value(summary).unwrap();
        summary.as_object_mut().unwrap().remove("wall_s");
        summary.as_object_mut().unwrap().remove("realtime_x");
        summaries.push(summary);
    }
    assert!(
        MISSES.load(Ordering::Relaxed) >= 2,
        "session fixture must exercise real lookup transitions"
    );
    assert_eq!(summaries[0], summaries[1]);
    for file in ["request_state.parquet", "request_slo.parquet"] {
        let read = |dir: &tempfile::TempDir| {
            ParquetRecordBatchReaderBuilder::try_new(
                File::open(dir.path().join("raw").join(file)).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };
        assert_eq!(
            read(&dirs[0]),
            read(&dirs[1]),
            "all modeled columns must match exactly: {file}"
        );
    }
}
