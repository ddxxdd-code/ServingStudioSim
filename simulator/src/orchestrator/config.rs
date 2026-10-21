//! Pool + group topology config (L6) — new-interface-design §6.1 / §8.
//!
//! A pool is always `{ placement, groups: [...] }` — never inlined. A homogeneous
//! pool has a `groups` list of length 1; heterogeneous = several groups on
//! different GPUs/arch (parse-only this round — `build()` accepts a single
//! group). The `Arch` / `Worker` type parameters ARE the contract constraint: a
//! layer-wise arch cannot be paired with an iter-wise worker. They are generic
//! here, so this layer holds the topology shape without depending on the concrete
//! L4/L5 selector types (the per-deployment configs in L7 instantiate them).
//!
//! `placement` is the only pool-level policy (L6 routing among replicas);
//! everything provider-specific lives on the arch/worker tags. The launcher
//! schema is derived: `#[derive(ParamStruct)]` on [`GroupSpec`] emits the flat
//! group fields (gpu / replicas) and on [`PoolSpec`] the flat pool fields
//! (placement); `groups` / `arch` / `worker` are `#[param(skip)]` (they are
//! nested sub-trees, not scalar params). The params do not touch `Arch` /
//! `Worker`, so `dump` reads them off a `<(), ()>` instantiation.

use serde::Deserialize;

use schema_derive::ParamStruct;

/// Writes choose a store; reads always follow the session directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KvStorePlacement {
    #[default]
    LeastLoaded,
    RoundRobin,
    SessionHash,
}

/// Dedicated DRAM pool, independent of compute arch/group configuration.
#[derive(Debug, Clone, Deserialize, ParamStruct)]
#[serde(deny_unknown_fields)]
pub struct KvStorePoolSpec {
    /// Storage-only worker composition (no arch or model).
    #[serde(default)]
    #[param(string, default = "distributed_kv_store", choices = KV_STORE_WORKER_CHOICES)]
    pub worker: crate::worker::config::KvStoreWorkerSel,
    /// Number of dedicated DRAM nodes.
    #[param(default = 1)]
    pub replicas: u16,
    /// Capacity of each node, in decimal GB.
    pub dram_gb: f64,
    /// Per-link transfer bandwidth in decimal GB/s.
    pub bandwidth_gbps: f64,
    /// Per-transfer setup latency in microseconds.
    pub latency_us: f64,
    /// Placement acts on writes; reads follow the directory.
    #[serde(default)]
    #[param(string, default = "least-loaded", choices = KV_PLACEMENT_CHOICES)]
    pub placement: KvStorePlacement,
    /// Occupancy-score penalty per millisecond of receive-link backlog.
    #[serde(default = "default_backlog_weight")]
    #[param(default = 1.0)]
    pub backlog_weight_per_ms: f64,
    /// Eviction policy for shared entries only.
    #[serde(default)]
    #[param(string, default = "lru", choices = KV_EVICTION_CHOICES)]
    pub policy: crate::worker::kv::PrefixCachePolicy,
}
const KV_PLACEMENT_CHOICES: [&str; 3] = ["least-loaded", "round-robin", "session-hash"];
const KV_STORE_WORKER_CHOICES: [&str; 1] = ["distributed_kv_store"];
const KV_EVICTION_CHOICES: [&str; 4] = ["lru", "fifo", "lfu", "largest-first"];

fn default_backlog_weight() -> f64 {
    1.0
}

impl KvStorePoolSpec {
    pub fn validate(&self, bytes_per_token: u64) -> anyhow::Result<()> {
        anyhow::ensure!(self.replicas > 0, "kv_store.replicas must be positive");
        anyhow::ensure!(
            self.backlog_weight_per_ms.is_finite() && self.backlog_weight_per_ms >= 0.0,
            "kv_store.backlog_weight_per_ms must be finite and nonnegative"
        );
        anyhow::ensure!(
            self.dram_gb.is_finite()
                && self.dram_gb > 0.0
                && self.dram_gb * 1e9 >= bytes_per_token as f64,
            "kv_store.dram_gb must hold at least one KV token"
        );
        anyhow::ensure!(
            self.bandwidth_gbps.is_finite() && self.bandwidth_gbps > 0.0,
            "kv_store.bandwidth_gbps must be finite and positive"
        );
        anyhow::ensure!(
            self.latency_us.is_finite() && self.latency_us >= 0.0,
            "kv_store.latency_us must be finite and nonnegative"
        );
        Ok(())
    }
}

/// L6 per-pool worker selection policy. Closed set → serde enum (kebab-case so
/// `least-queued` / `round-robin` match the wire spelling).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlacementPolicy {
    LeastQueued,
    RoundRobin,
}

const PLACEMENT_CHOICES: [&str; 2] = ["least-queued", "round-robin"];

/// One pool: a placement policy plus one-or-more homogeneous groups.
#[derive(Debug, Clone, Deserialize, ParamStruct)]
#[serde(deny_unknown_fields)]
pub struct PoolSpec<Arch, Worker> {
    /// Worker placement policy within the pool.
    #[param(string, default = "least-queued", choices = PLACEMENT_CHOICES)]
    pub placement: PlacementPolicy,
    #[param(skip)]
    pub groups: Vec<GroupSpec<Arch, Worker>>,
}

/// One homogeneous group: a GPU type, a replica count (= DP fan-out), and the
/// arch (L4) + worker (L5) providers — both tagged enums constrained to the
/// pool's contract class.
#[derive(Debug, Clone, Deserialize, ParamStruct)]
#[serde(deny_unknown_fields)]
pub struct GroupSpec<Arch, Worker> {
    /// GPU type this group runs on (profile.db key).
    pub gpu: String,
    /// Data-parallel replica count for this group.
    #[param(default = 1)]
    pub replicas: u16,
    #[param(skip)]
    pub arch: Arch,
    #[param(skip)]
    pub worker: Worker,
}
