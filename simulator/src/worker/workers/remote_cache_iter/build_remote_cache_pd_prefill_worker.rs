//! Remote pd_prefill recipe: KvCacheStore × RemoteCachePrefillHandoffAdmission × unified execution.
use super::RemoteCacheIterWorker;
use crate::arch::contract::IterwiseUnifiedModel;
use crate::common::{PoolId, SharedRequests, WorkerId};
use crate::log::PrefixCacheLogger;
use crate::worker::admission::{LoadBalance, PendingOrder, RemoteCachePrefillHandoffAdmission};
use crate::worker::execution::UnifiedIterExecution;
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::kv::KvCacheStore;
use crate::worker::types::WorkerConfig;
use crate::worker::workers::iter::IterBatchWorker;
use crate::worker::workers::iter_build_essentials::{
    kv_cache_store, prepare_iter_build_essentials, resolve_kv_accounting,
};
use std::path::PathBuf;
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_remote_cache_pd_prefill_worker<M: IterwiseUnifiedModel>(
    id: WorkerId,
    pool_tag: &'static str,
    model: Arc<M>,
    requests: SharedRequests,
    config: WorkerConfig,
    cost_log_dir: Option<PathBuf>,
    pool: PoolId,
    gpu_name: &str,
    cluster: SharedGpuCluster,
) -> RemoteCacheIterWorker<
    KvCacheStore,
    RemoteCachePrefillHandoffAdmission<PendingOrder>,
    UnifiedIterExecution<M>,
> {
    let logger = PrefixCacheLogger::open_opt(cost_log_dir.as_deref(), pool_tag, id);
    let partitions = model.num_attn_dp_groups().max(1) as usize;
    let shards = model.num_attn_shards().max(1);
    let accounting = resolve_kv_accounting(&model.kv_cache_layout(), shards, &config);
    let bytes_per_token = accounting.transfer_bytes(1);
    let essentials = prepare_iter_build_essentials(
        id,
        pool_tag,
        requests,
        &config,
        cost_log_dir,
        pool,
        gpu_name,
        &cluster,
        model.gpus_per_replica(),
        model.cost_log_manifest(),
        &accounting,
        partitions,
    );
    let group = cluster.borrow_mut().register_comm_group(
        essentials.allocation_base,
        shards,
        pool_tag,
        id.0,
    );
    let kv = kv_cache_store(
        accounting,
        partitions,
        shards,
        &config,
        essentials.sampler,
        logger,
    );
    let balance = match config.balance {
        LoadBalance::Single if partitions > 1 => LoadBalance::RoundRobin { next: 0 },
        configured => configured,
    };
    let admission = RemoteCachePrefillHandoffAdmission::new(
        PendingOrder::new(config.pending_order),
        (),
        balance,
        config.max_batch_tokens,
        config.max_inflight_remote_reads,
        group,
    );
    let iteration = IterBatchWorker::from_components(
        essentials.context,
        kv,
        admission,
        UnifiedIterExecution::new(model, essentials.cost),
    );
    RemoteCacheIterWorker::new(iteration, cluster, bytes_per_token)
}
