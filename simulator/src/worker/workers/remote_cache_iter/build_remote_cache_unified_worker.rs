//! Remote unified recipe: FullAttnKv × RemoteCachePrefillDecodeAdmission × unified execution.
use super::RemoteCacheIterWorker;
use crate::arch::contract::IterwiseUnifiedModel;
use crate::common::{PoolId, SharedRequests, WorkerId};
use crate::log::PrefixCacheLogger;
use crate::worker::admission::{LoadBalance, PendingOrder, RemoteCachePrefillDecodeAdmission};
use crate::worker::execution::UnifiedIterExecution;
use crate::worker::gpu_cluster::SharedGpuCluster;
use crate::worker::kv::FullAttnKv;
use crate::worker::types::WorkerConfig;
use crate::worker::workers::iter::IterBatchWorker;
use crate::worker::workers::unified_iter_build_essentials::{
    full_attention_token_capacity, prepare_unified_iter_build_essentials,
};
use std::path::PathBuf;
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_remote_cache_unified_worker<M: IterwiseUnifiedModel>(
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
    FullAttnKv,
    RemoteCachePrefillDecodeAdmission<PendingOrder>,
    UnifiedIterExecution<M>,
> {
    let logger = PrefixCacheLogger::open_opt(cost_log_dir.as_deref(), pool_tag, id);
    let partitions = model.num_attn_dp_groups().max(1) as usize;
    let shards = model.num_attn_shards().max(1);
    let bytes_per_token = model.total_kv_bytes_per_token();
    let capacity = full_attention_token_capacity(model.as_ref(), &config);
    let cache = config
        .prefix_cache
        .resolve_tokens(capacity, bytes_per_token, shards);
    let essentials = prepare_unified_iter_build_essentials(
        id,
        pool_tag,
        model,
        requests,
        &config,
        cost_log_dir,
        pool,
        gpu_name,
        &cluster,
        capacity,
        partitions,
    );
    let group = cluster.borrow_mut().register_comm_group(
        essentials.allocation_base,
        shards,
        pool_tag,
        id.0,
    );
    let kv = FullAttnKv::with_prefix_cache(partitions, capacity, cache, essentials.sampler, logger);
    let balance = match config.balance {
        LoadBalance::Single if partitions > 1 => LoadBalance::RoundRobin { next: 0 },
        configured => configured,
    };
    let admission = RemoteCachePrefillDecodeAdmission::new(
        PendingOrder::new(config.pending_order),
        (),
        balance,
        config.max_batch_tokens,
        config.max_inflight_remote_reads,
        group,
    );
    let iteration =
        IterBatchWorker::from_components(essentials.context, kv, admission, essentials.execution);
    RemoteCacheIterWorker::new(iteration, cluster, bytes_per_token)
}
