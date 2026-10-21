//! `pd` deployment — prefill/decode disaggregation. Two pools: a prefill pool of
//! `PdPrefillWorker`s and a decode pool of `PdDecodeWorker`s, wired into a
//! [`PdFlow`]. A prefill worker prefills then hands off (`PrefillDone`); the flow
//! resolves physical GPU endpoints, builds a `TransferPlan`, and the decode pool
//! pulls the KV across the shared `GpuCluster` before decoding to completion.
//!
//! The KV transfer cost comes from the `p2p_inter` L1 kernel (per-link bandwidth
//! profiled vs message size); `build_transfer_cost` wires it. Wired pairings are
//! `llama3_dense_tp` → `llama3_dense_tp` and `llama3_dense_tp` →
//! `llama3_dp_attn_tp_ffn`; other pairings bail until a concrete experiment
//! needs them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, ensure, Context};

use crate::arch::build as arch_build;
use crate::arch::contract::IterwiseUnifiedModel;
use crate::arch::IterArchSel;
use crate::common::{Fabric, SharedRequests};
use crate::deployment::config::PdConfig;
use crate::orchestrator::common::WorkerBuildFn;
use crate::orchestrator::config::{GroupSpec, PoolSpec};
use crate::orchestrator::{
    DpPlacementPolicy, Flow, PdFlow, PlacementPolicy, SimpleDpPoolConfig, UnifiedWorkerFactory,
    PD_DECODE_POOL, PD_PREFILL_POOL,
};
use crate::timing::kernels::{P2pInterKernel, P2pInterKernelConfig};
use crate::timing::PerfApiBridge;
use crate::worker::{
    build_pd_decode_worker, build_pd_prefill_worker, resolve_prefix_cache_config, CostSource,
    IterWorkerSel, PdDecodeWorker, PdPrefillWorker, PrefixCacheMode, PrefixCachePolicy,
    WorkerConfig,
};

use super::Deployment;
use crate::orchestrator::config::KvStorePoolSpec;
use crate::orchestrator::impls::kv_store_flows::KvStorePdFlow;
use crate::worker::workers::remote_cache_iter::build_remote_cache_pd_prefill_worker;
use crate::worker::workers::remote_pd_decode::build_remote_cache_pd_decode_worker;

pub struct PdDeployment;

/// Both pools' models share this dotted-leaf prefix (e.g. `pd.embedding`).
const MODEL_NAME: &str = "pd";

impl Deployment for PdDeployment {
    const NAME: &'static str = "pd";
    type Config = PdConfig;

    fn build(
        cfg: &PdConfig,
        bridge: &PerfApiBridge,
        store: SharedRequests,
    ) -> anyhow::Result<Box<dyn Flow>> {
        let pg = single_group(&cfg.pools.prefill, "prefill")?;
        let dg = single_group(&cfg.pools.decode, "decode")?;

        ensure_pd_prefill(&pg.worker)?;
        ensure_pd_decode(&dg.worker)?;
        let remote_prefill = matches!(pg.worker, IterWorkerSel::RemoteCachePdPrefill { .. });
        let remote_decode = matches!(dg.worker, IterWorkerSel::RemoteCachePdDecode { .. });
        ensure!(
            remote_prefill == remote_decode && remote_prefill == cfg.kv_store.is_some(),
            "pd: kv_store requires both remote_cache_pd_prefill and remote_cache_pd_decode"
        );
        if let Some(spec) = &cfg.kv_store {
            spec.validate(1)?;
        }

        // Prefill and decode must point at the same model config path for the
        // handed-off KV to be meaningful. The arch match below decides which
        // parallel layouts are supported; it is the only cross-arch gate here.
        ensure!(
            pg.arch.model().model_config == dg.arch.model().model_config,
            "pd: prefill and decode pools must share a model_config \
             (prefill={:?}, decode={:?})",
            pg.arch.model().model_config,
            dg.arch.model().model_config,
        );

        let prefill_cfg = pool_cfg(PD_PREFILL_POOL, pg.replicas, cfg.pools.prefill.placement);
        let decode_cfg = pool_cfg(PD_DECODE_POOL, dg.replicas, cfg.pools.decode.placement);

        let prefill_wc = worker_config(
            &pg.worker,
            &cfg.io.log_dir,
            cfg.io.log_output_token_times,
            cfg.io.log_stage_transitions,
            cfg.io.kv_log_stride,
        )?;
        let decode_wc = worker_config(
            &dg.worker,
            &cfg.io.log_dir,
            cfg.io.log_output_token_times,
            cfg.io.log_stage_transitions,
            cfg.io.kv_log_stride,
        )?;
        let log_dir: Option<PathBuf> = Some(cfg.io.log_dir.clone());

        // The KV-transfer cost source: the profiled inter-node p2p curve, keyed
        // on the decode GPU (receiver). Built once here (the bridge lives at L7)
        // and handed to the flow's shared `GpuCluster`.
        let cost = build_transfer_cost(&dg.gpu, bridge)?;

        // Only the pairs actually exercised are wired (add a new arm when a new
        // pairing is needed). Each arm builds both pools' concrete models (own
        // dims/TP) then assembles the flow. Today: tp→tp and tp→dp_attn.
        match (&pg.arch, &dg.arch) {
            (
                IterArchSel::Llama3DenseTp { tp_size: ptp, .. },
                IterArchSel::Llama3DenseTp { tp_size: dtp, .. },
            ) => {
                let prefill_model = {
                    let _scope =
                        bridge.with_backend_overrides("prefill", cfg.backends.get("prefill"));
                    Arc::new(arch_build::dense_tp(
                        pg.arch.model(),
                        *ptp,
                        &pg.gpu,
                        MODEL_NAME,
                        bridge,
                    )?)
                };
                let decode_model = {
                    let _scope =
                        bridge.with_backend_overrides("decode", cfg.backends.get("decode"));
                    Arc::new(arch_build::dense_tp(
                        dg.arch.model(),
                        *dtp,
                        &dg.gpu,
                        MODEL_NAME,
                        bridge,
                    )?)
                };
                assemble_pd_flow(
                    cfg.kv_store.as_ref(),
                    prefill_model,
                    decode_model,
                    store,
                    prefill_wc,
                    decode_wc,
                    log_dir,
                    pg.gpu.clone(),
                    dg.gpu.clone(),
                    prefill_cfg,
                    decode_cfg,
                    cost,
                )
            }
            // Cross-arch PD: TP prefill hands off to a DP-attention decode. Same
            // llama3 weights, only the parallel layout differs (the KV is logically
            // one tensor, re-sharded across the handoff), so it is a valid pair.
            (
                IterArchSel::Llama3DenseTp { tp_size: ptp, .. },
                IterArchSel::Llama3DpAttnTpFfn {
                    attn_tp_size,
                    ffn_tp_size,
                    ..
                },
            ) => {
                let prefill_model = {
                    let _scope =
                        bridge.with_backend_overrides("prefill", cfg.backends.get("prefill"));
                    Arc::new(arch_build::dense_tp(
                        pg.arch.model(),
                        *ptp,
                        &pg.gpu,
                        MODEL_NAME,
                        bridge,
                    )?)
                };
                let decode_model = {
                    let _scope =
                        bridge.with_backend_overrides("decode", cfg.backends.get("decode"));
                    Arc::new(arch_build::dp_attn_tp_ffn(
                        dg.arch.model(),
                        *attn_tp_size,
                        *ffn_tp_size,
                        &dg.gpu,
                        MODEL_NAME,
                        bridge,
                    )?)
                };
                assemble_pd_flow(
                    cfg.kv_store.as_ref(),
                    prefill_model,
                    decode_model,
                    store,
                    prefill_wc,
                    decode_wc,
                    log_dir,
                    pg.gpu.clone(),
                    dg.gpu.clone(),
                    prefill_cfg,
                    decode_cfg,
                    cost,
                )
            }
            (p, d) => bail!(
                "pd: unsupported prefill/decode arch pairing \
                 (got prefill={p:?}, decode={d:?}); wired pairs: \
                 dense_tp→dense_tp, dense_tp→dp_attn_tp_ffn"
            ),
        }
    }
}

// ── per-pool helpers ───────────────────────────────────────────────────────────

fn single_group<'a>(
    pool: &'a PoolSpec<IterArchSel, IterWorkerSel>,
    role: &str,
) -> anyhow::Result<&'a GroupSpec<IterArchSel, IterWorkerSel>> {
    ensure!(
        pool.groups.len() == 1,
        "pd: {role} pool supports a single homogeneous group (got {})",
        pool.groups.len()
    );
    Ok(&pool.groups[0])
}

fn ensure_pd_prefill(worker: &IterWorkerSel) -> anyhow::Result<()> {
    match worker {
        IterWorkerSel::PdPrefill { .. } | IterWorkerSel::RemoteCachePdPrefill { .. } => Ok(()),
        other => bail!("pd: prefill pool requires worker `pd_prefill`, got {other:?}"),
    }
}

fn ensure_pd_decode(worker: &IterWorkerSel) -> anyhow::Result<()> {
    match worker {
        IterWorkerSel::PdDecode { .. } | IterWorkerSel::RemoteCachePdDecode { .. } => Ok(()),
        other => bail!("pd: decode pool requires worker `pd_decode`, got {other:?}"),
    }
}

fn worker_config(
    worker: &IterWorkerSel,
    _log_dir: &Path,
    log_output_token_times: bool,
    log_stage_transitions: bool,
    kv_log_stride: u32,
) -> anyhow::Result<WorkerConfig> {
    anyhow::ensure!(
        !matches!(
            worker,
            IterWorkerSel::RemoteCachePdPrefill {
                prefix_cache_mode: crate::worker::PrefixCacheMode::Disabled,
                ..
            }
        ),
        "remote shared-prefix lookup requires an enabled local prefix cache"
    );
    let (attn_gpu_memory_gb, gpu_time_multiplier, prefix_cache) = match worker {
        IterWorkerSel::RemoteCachePdPrefill {
            attn_gpu_memory_gb,
            gpu_time_multiplier,
            prefix_cache_mode,
            prefix_cache_policy,
            prefix_cache_max_gpu_memory_gb,
            ..
        }
        | IterWorkerSel::PdPrefill {
            attn_gpu_memory_gb,
            gpu_time_multiplier,
            prefix_cache_mode,
            prefix_cache_policy,
            prefix_cache_max_gpu_memory_gb,
        } => (
            *attn_gpu_memory_gb,
            *gpu_time_multiplier,
            resolve_prefix_cache_config(
                "pd prefill",
                *prefix_cache_mode,
                *prefix_cache_policy,
                *prefix_cache_max_gpu_memory_gb,
                *attn_gpu_memory_gb,
            )?,
        ),
        IterWorkerSel::RemoteCachePdDecode {
            attn_gpu_memory_gb,
            gpu_time_multiplier,
        }
        | IterWorkerSel::PdDecode {
            attn_gpu_memory_gb,
            gpu_time_multiplier,
        } => (
            *attn_gpu_memory_gb,
            *gpu_time_multiplier,
            resolve_prefix_cache_config(
                "pd decode",
                PrefixCacheMode::Disabled,
                PrefixCachePolicy::Lru,
                None,
                *attn_gpu_memory_gb,
            )?,
        ),
        // ensure_* gates the worker tag before this is reached.
        _ => (
            80.0,
            1.0,
            resolve_prefix_cache_config(
                "pd",
                PrefixCacheMode::Disabled,
                PrefixCachePolicy::Lru,
                None,
                80.0,
            )?,
        ),
    };
    let (max_inflight_remote_reads, max_batch_tokens, pending_order) = match worker {
        IterWorkerSel::RemoteCachePdPrefill {
            max_inflight_remote_reads,
            max_batch_tokens,
            pending_order,
            ..
        } => (
            *max_inflight_remote_reads,
            *max_batch_tokens,
            *pending_order,
        ),
        _ => (16, None, crate::worker::PendingOrderKind::default()),
    };
    ensure!(
        max_inflight_remote_reads > 0,
        "max_inflight_remote_reads must be positive"
    );
    Ok(WorkerConfig {
        max_inflight_remote_reads,
        max_batch_tokens,
        pending_order,
        attn_kv_bytes: (attn_gpu_memory_gb * 1e9) as u64,
        log_output_token_times,
        log_stage_transitions,
        kv_log_stride,
        gpu_time_multiplier,
        prefix_cache,
        ..WorkerConfig::default()
    })
}

fn pool_cfg(
    pool: crate::common::PoolId,
    replicas: u16,
    placement: PlacementPolicy,
) -> SimpleDpPoolConfig {
    SimpleDpPoolConfig {
        pool,
        num_workers: replicas,
        placement: placement_into(placement),
    }
}

/// Assemble a [`PdFlow`] from two built models. Generic over each pool's concrete
/// model type; the prefill pool stamps `PdPrefillWorker`, the decode pool
/// `PdDecodeWorker`. The returned `Box<dyn Flow>` is the only `dyn` erasure point.
#[allow(clippy::too_many_arguments)]
fn assemble_pd_flow<MP, MD>(
    kv_store: Option<&KvStorePoolSpec>,
    prefill_model: Arc<MP>,
    decode_model: Arc<MD>,
    store: SharedRequests,
    prefill_wc: WorkerConfig,
    decode_wc: WorkerConfig,
    log_dir: Option<PathBuf>,
    prefill_gpu_name: String,
    decode_gpu_name: String,
    prefill_cfg: SimpleDpPoolConfig,
    decode_cfg: SimpleDpPoolConfig,
    cost: CostSource,
) -> anyhow::Result<Box<dyn Flow>>
where
    MP: IterwiseUnifiedModel + 'static,
    MD: IterwiseUnifiedModel + 'static,
{
    // Kept for the post-build `attach_logger` — `log_dir` itself is moved into the
    // two worker factories below (each opens its own per-worker `cost_log`).
    let net_log_dir = log_dir.clone();
    if let Some(spec) = kv_store {
        let bytes = prefill_model.total_kv_bytes_per_token();
        ensure!(
            bytes == decode_model.total_kv_bytes_per_token(),
            "PD relay KV layouts must match"
        );
        spec.validate(bytes)?;
        let pf = UnifiedWorkerFactory::new(
            prefill_model,
            store.clone(),
            prefill_wc,
            log_dir.clone(),
            prefill_gpu_name,
            "prefill",
            build_remote_cache_pd_prefill_worker::<MP>,
        );
        let df = UnifiedWorkerFactory::new(
            decode_model,
            store,
            decode_wc,
            log_dir,
            decode_gpu_name,
            "decode",
            build_remote_cache_pd_decode_worker::<MD>,
        );
        let mut flow = KvStorePdFlow::new(&prefill_cfg, &pf, &decode_cfg, &df, spec, bytes, cost);
        if let Some(dir) = net_log_dir {
            flow.attach_logger(&dir);
        }
        return Ok(Box::new(flow));
    }

    let prefill_factory: UnifiedWorkerFactory<MP, PdPrefillWorker<MP>> = UnifiedWorkerFactory::new(
        prefill_model,
        std::rc::Rc::clone(&store),
        prefill_wc,
        log_dir.clone(),
        prefill_gpu_name,
        "prefill",
        build_pd_prefill_worker::<MP> as WorkerBuildFn<MP, PdPrefillWorker<MP>>,
    );
    let decode_factory: UnifiedWorkerFactory<MD, PdDecodeWorker<MD>> = UnifiedWorkerFactory::new(
        decode_model,
        store,
        decode_wc,
        log_dir,
        decode_gpu_name,
        "decode",
        build_pd_decode_worker::<MD> as WorkerBuildFn<MD, PdDecodeWorker<MD>>,
    );
    let flow = PdFlow::new(
        &prefill_cfg,
        &prefill_factory,
        &decode_cfg,
        &decode_factory,
        cost,
    );
    // The shared cluster is built inside `PdFlow::new`; attach the `gpu_cluster`
    // log now that its workers have self-registered their comm groups (with owner
    // identity). Runs without a log dir leave the cluster logger-less.
    if let Some(dir) = net_log_dir.as_deref() {
        flow.cluster().borrow_mut().attach_logger(dir);
    }
    Ok(Box::new(flow))
}

/// Build the PD KV-transfer cost source: the profiled `p2p_inter` curve for the
/// receiver GPU. v1 assumes a cross-node (Infiniband) handoff over the `nccl`
/// backend; the curve is size-keyed (comm is modeled by message bytes, not
/// dtype), so the KV handoff cost rides on the transferred byte count. Fabric
/// becomes config when PD placement grows fabric awareness.
fn build_transfer_cost(gpu_name: &str, bridge: &PerfApiBridge) -> anyhow::Result<CostSource> {
    let kernel = P2pInterKernel::build(
        "pd_kv_transfer".to_string(),
        P2pInterKernelConfig {
            backends: vec!["nccl"],
            gpu_name: gpu_name.to_string(),
            fabric: Fabric::Infiniband,
        },
        bridge,
    )
    .with_context(|| format!("building p2p_inter kernel for PD KV transfer on {gpu_name}"))?;
    Ok(CostSource::Kernel(kernel))
}

fn placement_into(p: PlacementPolicy) -> DpPlacementPolicy {
    match p {
        PlacementPolicy::LeastQueued => DpPlacementPolicy::LeastQueued,
        PlacementPolicy::RoundRobin => DpPlacementPolicy::RoundRobin,
    }
}

#[cfg(test)]
mod remote_config_tests {
    use super::*;

    #[test]
    fn remote_selector_maps_budget_order_and_capacity_to_worker_configuration() {
        let worker: IterWorkerSel = serde_json::from_value(serde_json::json!({
            "type": "remote_cache_pd_prefill", "attn_gpu_memory_gb": 0.000001,
            "gpu_time_multiplier": 1.5, "max_batch_tokens": 32,
            "pending_order": "fifo", "max_inflight_remote_reads": 3,
            "prefix_cache_policy": "lfu"
        }))
        .unwrap();
        ensure_pd_prefill(&worker).unwrap();
        let config = worker_config(&worker, Path::new("unused"), true, true, 4).unwrap();
        assert_eq!(config.attn_kv_bytes, 1000);
        assert_eq!(config.max_batch_tokens, Some(32));
        assert_eq!(config.max_inflight_remote_reads, 3);
        assert_eq!(config.gpu_time_multiplier, 1.5);
        assert_eq!(config.pending_order, crate::worker::PendingOrderKind::Fifo);
        assert_eq!(config.kv_log_stride, 4);
        let decode: IterWorkerSel = serde_json::from_value(serde_json::json!({
            "type": "remote_cache_pd_decode", "attn_gpu_memory_gb": 0.000002
        }))
        .unwrap();
        ensure_pd_decode(&decode).unwrap();
        let config = worker_config(&decode, Path::new("unused"), false, false, 1).unwrap();
        assert_eq!(config.attn_kv_bytes, 2000);
        assert_eq!(
            config.prefix_cache,
            crate::worker::kv::PrefixCacheConfig::Disabled
        );
    }

    #[test]
    fn assembled_remote_pd_reads_previous_turn_and_drains_both_writebacks() {
        use crate::common::{RequestId, RequestStore, SessionInput, Time};
        use crate::test_helpers::{text_request, FakeModel};
        use std::{cell::RefCell, rc::Rc};
        let requests = Rc::new(RefCell::new(RequestStore::new()));
        requests.borrow_mut().reserve_slots(2);
        let directory = tempfile::tempdir().unwrap();
        let spec = KvStorePoolSpec {
            worker: Default::default(),
            replicas: 2,
            dram_gb: 0.000001,
            bandwidth_gbps: 1.0,
            latency_us: 1.0,
            placement: crate::orchestrator::config::KvStorePlacement::RoundRobin,
            backlog_weight_per_ms: 1.0,
            policy: PrefixCachePolicy::Lru,
        };
        let mut flow = assemble_pd_flow(
            Some(&spec),
            Arc::new(FakeModel::for_ms(1.0)),
            Arc::new(FakeModel::for_ms(1.0)),
            requests.clone(),
            WorkerConfig::default(),
            WorkerConfig::default(),
            Some(directory.path().to_path_buf()),
            "test".into(),
            "test".into(),
            pool_cfg(PD_PREFILL_POOL, 2, PlacementPolicy::RoundRobin),
            pool_cfg(PD_DECODE_POOL, 1, PlacementPolicy::RoundRobin),
            CostSource::analytic(1.0),
        )
        .unwrap();
        assert_eq!(flow.cluster().borrow().num_gpus(), 3);
        let mut clock = 0;
        for id in 0..2 {
            let mut request = text_request(
                RequestId(id),
                if id == 0 { 20 } else { 10 },
                3,
                Time::from_us(clock * 100),
            );
            request.definition.session = SessionInput::Session {
                session_id: 7,
                session_start_time: Time::ZERO,
                declared_prefix_tokens: if id == 0 { 0 } else { 20 },
            };
            flow.on_arrival(request);
            let mut completions = 0;
            for _ in 0..150 {
                completions += flow.tick(Time::from_us(clock * 100)).len();
                clock += 1;
                if completions == 1 && flow.background_pending() == 0 {
                    break;
                }
            }
            flow.check_health().unwrap();
            assert_eq!(completions, 1);
            assert_eq!(flow.background_pending(), 0);
        }
        let records = requests.borrow();
        assert_eq!(
            records[RequestId(1)].telemetry.prefix_cache_hit_tokens,
            Some(20)
        );
        assert_eq!(records[RequestId(1)].progress.prefill_tokens_processed, 10);
        assert_eq!(records[RequestId(1)].progress.output_tokens_emitted, 3);
    }
    #[test]
    fn remote_prefill_rejects_disabled_local_prefix_cache() {
        let worker = IterWorkerSel::RemoteCachePdPrefill {
            attn_gpu_memory_gb: 80.0,
            gpu_time_multiplier: 1.0,
            prefix_cache_mode: crate::worker::PrefixCacheMode::Disabled,
            prefix_cache_policy: crate::worker::PrefixCachePolicy::Lru,
            prefix_cache_max_gpu_memory_gb: None,
            max_inflight_remote_reads: 16,
            max_batch_tokens: None,
            pending_order: crate::worker::PendingOrderKind::SessionStart,
        };
        let error = worker_config(&worker, Path::new("unused"), false, false, 1)
            .err()
            .unwrap();
        assert!(error.to_string().contains("enabled local prefix cache"));
    }

    #[test]
    fn kv_store_requires_both_remote_selectors_and_matching_relay_layouts() {
        use crate::arch::contract::UnifiedArchInput;
        use crate::common::RequestStore;
        use crate::deployment::config::RunConfig;
        use crate::test_helpers::FakeModel;
        use crate::timing::LeafMetrics;
        use std::{cell::RefCell, rc::Rc};
        // Selector guards run before any model file is read.
        let build_error = |prefill: &str, decode: &str, kv_store: bool| {
            let group = |worker: &str| {
                format!(
                    r#"{{ gpu: "H200", replicas: 1, arch: {{ type: llama3_dense_tp, model_config: "m.json", fp8: false, tp_size: 1 }}, worker: {worker} }}"#
                )
            };
            let yaml = format!(
                r#"
deployment: pd
{store}workload: {{ trace_files: ["t.csv"], input_file_format: text-generation-independent, arrival_mode: trace_timed, session_dependency: independent, duration_ms: 1.0, run_to_end: true, request_rate: 1.0 }}
io: {{ log_dir: "unused", log_level: info, quiet: true, force_cache_build: false, log_output_token_times: false }}
pools:
  prefill:
    placement: least-queued
    groups:
      - {prefill}
  decode:
    placement: least-queued
    groups:
      - {decode}
"#,
                store = if kv_store {
                    "kv_store: { replicas: 1, dram_gb: 1.0, bandwidth_gbps: 1.0, latency_us: 1.0 }\n"
                } else {
                    ""
                },
                prefill = group(prefill),
                decode = group(decode),
            );
            let RunConfig::Pd(cfg) = serde_yaml::from_str(&yaml).unwrap() else {
                panic!("expected a pd config");
            };
            let requests = Rc::new(RefCell::new(RequestStore::new()));
            match PdDeployment::build(&cfg, &PerfApiBridge::new_uninit_for_test(), requests) {
                Ok(_) => panic!("accepted {prefill} + {decode} with kv_store={kv_store}"),
                Err(error) => error.to_string(),
            }
        };
        let (direct_prefill, remote_prefill) = (
            "{ type: pd_prefill, attn_gpu_memory_gb: 80.0 }",
            "{ type: remote_cache_pd_prefill, attn_gpu_memory_gb: 80.0 }",
        );
        let (direct_decode, remote_decode) = (
            "{ type: pd_decode, attn_gpu_memory_gb: 80.0 }",
            "{ type: remote_cache_pd_decode, attn_gpu_memory_gb: 80.0 }",
        );
        let pairing =
            "pd: kv_store requires both remote_cache_pd_prefill and remote_cache_pd_decode";
        for (prefill, decode, kv_store) in [
            (remote_prefill, direct_decode, true),
            (direct_prefill, remote_decode, true),
            (direct_prefill, direct_decode, true),
            (remote_prefill, remote_decode, false),
        ] {
            assert!(build_error(prefill, decode, kv_store).contains(pairing));
        }

        // A relay copies prefill KV bytes into decode, so per-token sizes must match.
        struct TwoBytesPerToken(FakeModel);
        impl IterwiseUnifiedModel for TwoBytesPerToken {
            fn eval_iter(
                &self,
                batch: &UnifiedArchInput,
                slots: &mut Vec<LeafMetrics>,
                scratch: &mut Vec<LeafMetrics>,
            ) -> LeafMetrics {
                self.0.eval_iter(batch, slots, scratch)
            }
            fn total_kv_bytes_per_token(&self) -> u64 {
                2
            }
            fn gpus_per_replica(&self) -> u16 {
                1
            }
        }
        let spec = KvStorePoolSpec {
            worker: Default::default(),
            replicas: 1,
            dram_gb: 1.0,
            bandwidth_gbps: 1.0,
            latency_us: 1.0,
            placement: Default::default(),
            backlog_weight_per_ms: 1.0,
            policy: PrefixCachePolicy::Lru,
        };
        let mismatch = assemble_pd_flow(
            Some(&spec),
            Arc::new(FakeModel::for_ms(1.0)),
            Arc::new(TwoBytesPerToken(FakeModel::for_ms(1.0))),
            Rc::new(RefCell::new(RequestStore::new())),
            WorkerConfig::default(),
            WorkerConfig::default(),
            None,
            "test".into(),
            "test".into(),
            pool_cfg(PD_PREFILL_POOL, 1, PlacementPolicy::RoundRobin),
            pool_cfg(PD_DECODE_POOL, 1, PlacementPolicy::RoundRobin),
            CostSource::analytic(1.0),
        );
        match mismatch {
            Ok(_) => panic!("accepted a 1-byte prefill relay into a 2-byte decode layout"),
            Err(error) => assert!(error.to_string().contains("PD relay KV layouts must match")),
        }
    }
}
