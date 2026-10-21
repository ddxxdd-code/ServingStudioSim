# Distributed session KV cache

The optional dedicated DRAM pool implements issue #16. It supports full-attention
unified workers (including HP partitions) and PD. AFD and recurrent-state models
remain on their existing paths. Omitting `kv_store` preserves the direct-link
compositions; remote selectors require the block and the block requires remote
selectors.

## Configuration

Add this top-level block to a unified or PD preset:

```yaml
kv_store:
  worker: distributed_kv_store
  replicas: 2
  dram_gb: 64
  bandwidth_gbps: 25
  latency_us: 10
  placement: least-loaded
  policy: lru
```

Capacity and bandwidth use decimal GB. The bandwidth is per link; latency is
charged once per transfer. Capacity must hold at least one token of the model's
KV layout. PD prefill/decode must have matching KV bytes per token. No GPU or
model is allocated to a store.

Use `remote_cache_unified` as the unified worker type, or
`remote_cache_pd_prefill` / `remote_cache_pd_decode` for the corresponding PD
pools. The prefill-capable selectors expose the existing prefix-cache, pending
order, memory, multiplier and token-budget parameters, plus
`max_inflight_remote_reads` (default 16, positive). Enable the local prefix cache
to retain landed remote prefixes. Decode uses the existing memory/multiplier
parameters and never performs a prefix lookup. Store replicas and placement
are normal sweep leaves; store on/off is a structural preset variant.

Worked presets: `presets/distributed_kv_smoke.yaml` and
`presets/distributed_kv_pd_smoke.yaml` sweep 1/2 stores and all three placement
policies over `tests/fixtures/distributed_kv.csv`, a small chained session.

## Ownership and protocol

| Selector | KV ownership | Admission | Execution | Shell |
| --- | --- | --- | --- | --- |
| `remote_cache_unified` | `FullAttnKv` | `RemoteCachePrefillDecodeAdmission` | `UnifiedIterExecution` | `RemoteCacheIterWorker` |
| `remote_cache_pd_prefill` | `FullAttnKv` + held | `RemoteCachePrefillHandoffAdmission` | `UnifiedIterExecution` | `RemoteCacheIterWorker` |
| `remote_cache_pd_decode` | `FullAttnKv` + held | Landed ingress in `DecodeCore` | `UnifiedIterExecution` | `RemotePullDecodeWorker` |
| `distributed_kv_store` | `SessionStore` | None | None | `KvStoreWorker` |

`KvStoreWorker` is a storage-only `IterWorker`. Its `SessionStore` owns the
existing `PrefixCache`, incoming reservations and request-owned relay pins.
It has no request admission, model execution, or generated tokens. Its request
queue/active counts are always zero. `KvStoreLoad` separately reports capacity,
occupancy including reservations, and send backlog.

The L6a `KvStoreRouter` owns a session-to-store directory. Lookup dispatches to
exactly one store; an absent directory entry generates a zero-hit reply without
waking any store. Writes choose a destination independently. `least-loaded`
minimizes occupancy/capacity plus send backlog in milliseconds, breaking ties
by store index. `round-robin` cycles through eligible stores; `session-hash`
starts at session ID modulo replica count. Relay writes filter out stores that
cannot hold the entire pinned entry. Eviction invalidates the directory only
when its recorded store matches the eviction's source.

Shared lookup is a copy: it leaves the store entry resident. LRU/LFU reads
refresh their policy metadata; FIFO does not change insertion order. The GPU
receiver submits `kv_store_read` using the returned communication group. The
store receiver submits `kv_store_push` after reserving capacity. Both use the
existing `GpuCluster` contention timeline; there is no sender-side duplicate
transfer or explicit pull message. A shared copy remains valid if its source
entry is evicted while bytes are in flight.

Unified/prefill admission selects a request once. Sufficient local prefixes
take the normal gate chain. Otherwise the request moves through lookup/read
states, bounded by `max_inflight_remote_reads`, then resumes the normal
preview, token budget, fit and reserve gates. Arrival inserts the copy into the
selected local partition; intervening pressure can evict it and cause normal
recomputation. These states never re-enter the pending selection policy.
`RemoteCacheIterWorker` composes `IterBatchWorker`; it does not duplicate the
iteration FSM or model input lowering.

PD relay is prefill → store → decode. Prefill holds KV until the store's
`PushComplete`, then decode receives a handoff pointing at the store. The relay
entry stays pinned until decode's transfer lands or decode cancellation emits
`KvConsumed`. The acknowledgement includes request identity, so an old consume
cannot unpin a newer relay. Decode composes the same `DecodeCore` used by the
direct worker. After decoding it writes a shared entry. Output-length-one
requests write shared KV directly from prefill. Requests without a session skip
lookup/writeback and use the existing standalone direct path, including PD.

## Capacity and completion details

Incoming pushes reserve capacity before submitting bytes. Shared entries may
be evicted or truncated; pinned entries cannot. A relay that cannot fit emits
`CapacityExceeded` and fails the run explicitly. Same-session writes serialize
while an earlier push is in flight or pinned. Shared copy reads remain allowed.

Two details refine the RFC's pseudocode to keep resource accounting sound:

* Source KV moves resident → held → retained on push acknowledgement. It is
  never charged to both held and local cache ledgers simultaneously.
* The flow releases terminal completion after final writeback acknowledgement,
  so a run cannot exit with an unfinished push and chained rounds see committed
  store state. Token completion telemetry retains the actual compute time.

## Observability and validation

`raw/kv_store_event.parquet` is a sparse stream of placement, lookup,
retain/evict and pin/unpin rows, with a run-wide sequence and before/after
occupancy receipts. Placement records its policy, occupancy ratio and send backlog. Transfers
retain the `kv_store_push` / `kv_store_read` labels in the existing comm stream.
Run metadata schema v6 adds `devices` with explicit `host_memory` versus `gpu`;
the existing `gpus` and `gpu_ids` remain GPU-only and communication groups also
expose `device_ids`. This avoids counting DRAM nodes as GPUs in Analyzer.

The Rust library tests cover arrival-before-ack, shared copy retention,
reservations/pinning, directory misses/stale eviction, cross-worker reuse,
PD relay/pin-log replay, and standalone direct-path action equivalence. Launcher
tests cover optional-block omission/null, defaults, sweeps, and invalid blocks.
Run `just test-all` for the repository gate; its dependency sync and GPU tier
require the documented CUDA environment.
