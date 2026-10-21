# Distributed session KV cache

The optional dedicated DRAM pool implements [issue #16](https://github.com/SyFI-ServingStudio/ServingStudioSim/issues/16). It supports full-attention
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
  backlog_weight_per_ms: 1.0
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
to retain landed remote prefixes; remote unified/prefill reject `prefix_cache_mode: disabled`. Decode uses the existing memory/multiplier
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
occupancy including reservations, and send/receive backlogs.

The L6a `KvStoreRouter` owns a session-to-store directory. Lookup dispatches to
exactly one store; an absent directory entry generates a zero-hit reply without
waking any store. Writes choose a destination independently. `least-loaded`
orders candidates by `occupancy/capacity + backlog_weight_per_ms * recv_backlog_ms`,
breaking ties by store index. The nonnegative finite weight defaults to 1.0;
zero ignores backlog. Receive backlog represents the link contended by pushes.
This remains a first-cut heuristic, not a calibrated Mooncake placement policy.
Sweep the weight before interpreting placement comparisons. `round-robin` rotates the candidate
list; `session-hash` starts at session ID modulo replica count. Placement is a
hint: the store authoritatively reserves capacity when a queued push can start.
On `CapacityExceeded`, the router tries each remaining candidate once; only
exhausting the list produces a simulation error. Eviction invalidates the directory only
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
preview, token budget, fit and reserve gates. Immediately before submitting a
read, admission reselects the best current local partition and skips the transfer
if its usable prefix is already at least the remote hit. Otherwise it transfers
the whole hit, as specified by the RFC, rather than only the missing suffix.
Arrival inserts the copy into the
selected local partition after rechecking the current local prefix. A shorter
response cannot replace a newer, longer entry. If a better local partition
appeared during the read, landing uses that partition. Intervening pressure can evict it and cause normal
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

Foreground completion and background writeback are separate:

* Unified completion, final PD decode completion, and single-token PD prefill
  completion immediately notify the frontend. A chained next turn may overlap
  the write and hit local KV, hit the store, or miss depending on actual timing.
* A PD prefill relay waits for its pinned write to land before admitting decode.
  Decode cannot compute from a read before that read lands.
* The simulation keeps ticking background writes after foreground work finishes.
  Request completion timestamps, the summary's `sim_ms`, and throughput use the
  foreground horizon; network/store logs can extend beyond it. A drain that
  exceeds `stuck_threshold` fails explicitly. A duration cutoff with unfinished
  foreground requests still follows the existing partial-run behavior.

At push offer, resident KV becomes a held source and a separate evictable local
prefix copy is retained. Both copies are charged during the transfer window.
The retained copy is capped/truncated by actual physical slack and normal cache
policy; no extra GPU capacity is invented. Acknowledgement releases the held
copy, then attempts to restore the capacity-truncated prefix using the released
space. A request-owned restoration right is revoked by any later activation or
replacement of that session, even a shorter replacement; stale acknowledgements
cannot restore old data over it. The remote PD decode
worker has no local prefix cache, so it holds only the source. This conservative
copy model can later be replaced by aliasing inside the KV owner.

`Flow::cancel(req, now)` cancels remote admission/active/held state and sends an
identity-guarded store cancellation. Store cancellation releases queued work,
incoming reservations and relay pins. Already charged network time is not
rolled back. A late `Consumed` cannot unpin a newer request and does not emit a
successful `unpin` row. The caller owns frontend cancellation bookkeeping; the
current trace frontend does not generate cancellation events itself.

The replaceable storage boundary remains `SessionStore` operations (copy,
reserve, land, consume, cancel, load observations). `KvLocation` keeps directory
fields named. Retention remains explicit (`Shared`, `PinnedUntilFetched`), and
remote/direct PD retain separate named worker compositions. Network cost remains
parameterized latency plus bytes/bandwidth, suitable for later measurement.

## Observability and validation

`raw/kv_store_event.parquet` is a sparse stream of placement, lookup,
retain/evict and pin/unpin rows, with a run-wide sequence and before/after
occupancy receipts. Placement/retry records its policy, occupancy ratio, send
and receive backlogs, and backlog weight. `push-rejected` counts failed reservations (including ones later
successfully retried). Store rows carry `reserved_tokens` and `pinned_tokens`;
cache receipt columns remain exact per-mutation transitions. `transfer` rows
record bytes, endpoint pool names, submission time, queue time, and transfer end.
`prefix-reuse` records actual admitted local/remote hit tokens: a hit is remote
only for the usable extension beyond the local prefix at landing, and only when
this request fetched the entry it actually consumed. The overlapping prefix and
subsequent local reuse of that entry are local. Evicted or superseded reads are not credited.
`request-complete.background_writes` counts all outstanding writes in the flow
at that completion, including the completing request's offer. Transfers
retain the `kv_store_push` / `kv_store_read` labels in the existing comm stream.
Run metadata schema v6 adds `devices` with explicit `host_memory` versus `gpu`;
the existing `gpus` and `gpu_ids` remain GPU-only and communication groups also
expose `device_ids`. This avoids counting DRAM nodes as GPUs in Analyzer.

`kv_snapshot.held_kv` and `non_cache_kv` are independent per-window maxima.
The latter measures resident plus held KV, excluding retained prefix cache.
They preserve short transfer peaks even when the largest total occupancy occurs
at another instant. Existing `active_kv` is total committed KV including retained
cache; its paired `retained_prefix_kv` describes that same total-peak sample.
Do not subtract independent maxima or sum per-worker peaks to claim a
simultaneous cluster peak. Analyzer's KV occupancy report exposes raw-window
`peak_held_max_tokens` and `peak_non_cache_max_tokens` per series without averaging
away spikes. On old logs these new metrics are unavailable, not zero.

On protocol failure L7 finalizes partial request records and flushes logging
before returning the original error, preserving evidence of capacity failures.

The Rust library tests cover arrival-before-ack, shared copy retention,
reservations/pinning, directory misses/stale eviction, cross-worker reuse,
PD relay/pin-log replay, and standalone direct-path action equivalence. Launcher
tests cover optional-block omission/null, defaults, sweeps, and invalid blocks.
Run `just test-all` for the repository gate; its dependency sync and GPU tier
require the documented CUDA environment.

## Fair comparison and resolution

`presets/distributed_kv_pd_validation.yaml` defines 16 cases.
`presets/distributed_kv_pd_direct_validation.yaml` supplies five paired
store-disabled controls (baseline, 10/100 µs ticks, no reusable prefix, load64). Both use
identical model/GPU profiles, pool placements, request traces and foreground
completion definitions. The seven-conversation fixtures preserve each round's
total input context when turning declared reuse off. This is a 0% versus full
*declared reuse opportunity* comparison, not a measured production hit rate.

`distributed_kv_unified_validation.yaml` and
`distributed_kv_unified_direct_validation.yaml` provide matching unified on/off
controls. The unified and PD `load64` cases use 64 concurrent sessions with eight
chained turns each (512 requests), generated reproducibly by
`uv run python tests/fixtures/gen_distributed_kv_trace.py tests/fixtures/distributed_kv_load.csv`.
The PD weight sweep
tests 0, 1 and 10 per millisecond.

The remote cases vary store count, DRAM capacity, latency, bandwidth, placement,
read concurrency, declared reuse and tick resolution independently. Copy the
presets into a dated `logs/YYYYMMDD_N_distributed_kv` directory and rewrite their
`io.log_dir` prefixes there, then run from the repository root:

```bash
uv run python -m launcher <remote-preset.yaml> --dry-run
uv run python -m launcher <direct-preset.yaml> --dry-run
uv run python -m launcher <remote-preset.yaml>
uv run python -m launcher <direct-preset.yaml>
```

Launch the two presets separately so each sweep keeps its own `case` coordinate
space; pair runs by the matching case label. Combining standalone presets with
identical labels into one launcher batch currently gives duplicate aggregate
coordinates, though individual simulations and reports remain valid.

Use 1 µs ticks for the 10 µs latency baseline. Compare 1/10/100 µs results before
making a latency claim; a 100 µs tick can hide or amplify a 10 µs dependency.
This is experiment-resolution validation, not a new simulator clock. The
one-factor grid checks sensitivity; expand workload size/concurrency and trace
mixes before drawing throughput conclusions. Direct PD uses its existing
profiled GPU link; the remote pool uses the configured host-link alpha/beta.
Calibrate those inputs before interpreting the result as a hardware comparison.

| Quantity | Evidence |
| --- | --- |
| TTFT, TPOT, E2E, prefill compute tokens | `request_slo.parquet`; E2E = logging time minus arrival time |
| Local/remote admitted prefix-hit tokens | `kv_store_event`, operation `prefix-reuse` |
| GPU total/retained, held and non-cache peaks | per-worker `kv_snapshot`; independent maxima as defined above |
| Store resident, reserved, pinned occupancy | cache receipts and reserved/pinned columns in `kv_store_event` |
| Bytes and queue time per direction | `transfer` rows grouped by source/destination pool; endpoint charges also in `gpu_cluster` |
| Push rejection/retry counts | `push-rejected` / `retry` rows |
| Background writes at completion | `request-complete.background_writes` |

Store-disabled regression compares modeled summaries (excluding wall time and
realtime speed) and all pre-existing raw Parquet columns against the base commit.
This is exact modeled-result equivalence, not byte identity of entire files:
metadata schema v6's additive device fields and the new held/non-cache columns
are explicit exceptions. The unchanged baseline's
HashMap admission order can permute simultaneous-completion rows; compare
`request_slo` by request ID, without rounding values or relaxing tolerances. GPU tests
calibrate model/kernel costs and hardware links; the protocol correctness tests
use deterministic CPU models and do not require CUDA. Simulated performance
with an existing profile database is not a physical Mooncake benchmark.

### Reproducible gates and accepted deviations

`just test-distributed-kv` is a CPU-only regression gate run by
`.github/workflows/distributed-kv.yml`. Its always-miss stub wraps the real remote
admission protocol, answers every actual lookup synchronously with zero, and
compares the chained session fixture's modeled summary and complete request
event/SLO tables to barebone. This proves shell cadence equivalence; it does not
remove the real store protocol's latency. Regressions also exercise local
50 → 100 growth against a remote 80 hit, stale-directory misses, source eviction
mid-read, two snapshot reads overlapping writeback, guarded push restoration,
held sampling, and partial-log preservation on failure.

After launcher simulations and standard Analyzer reports finish:

```bash
uv run python tools/validate_distributed_kv.py audit <experiment-root> --output <audit.json>
uv run python tools/validate_distributed_kv.py equivalence <base-run> <store-off-run>
uv run python tools/validate_distributed_kv.py pair <direct-run> <remote-run>
```

The audit replays capacity receipts, reservations, pin identities, read/decode
dependencies, transfer endpoints and foreground completion. CI exercises these
checks with valid and deliberately corrupted evidence; the profile-backed
performance matrix remains a separate gate requiring a populated profile DB.
The pair command verifies workload/model/placement equality and compares
Analyzer latency, throughput, prefill work, prefix-hit totals (from workload
conservation), and held/non-cache peaks without
assuming a universal performance direction.

Accepted RFC deviations and interpretation corrections:

* `complete_push` handles store acknowledgement instead of overloading direct
  `complete_handoff`; it releases the source and applies guarded cache restoration.
* PD acknowledgements from the shared router are demultiplexed using request-owned
  prefill/decode write maps at runtime. Worker compositions remain statically typed.
* Push placement uses configurable receive backlog rather than the RFC's send
  backlog, because a push occupies the destination's receive link.
* Two physical copies are charged during push, with safe restoration on ack.
  Zero-copy shared ownership remains future work; transient pressure can still
  reduce reuse compared to an aliasing backend.
* TTFT is recorded at prefill completion, before the PD relay. The relay can
  delay decode admission/TPOT/E2E; it need not worsen TTFT or delay an otherwise
  busy decode worker. Measure those effects instead of asserting the RFC's
  unconditional TTFT prediction.
* Store-off equality permits additive schemas and simultaneous SLO row ordering
  as described above; it does not permit changes to modeled values.

## Review correction validation (2026-09-30)

The follow-up patch adds regressions for monotonic prefix landing/acknowledgement,
local reuse during outstanding writes, double-copy capacity, queued placement
retry, cancellation in each PD phase, stale consume protection, same-tick store
landing before lookup, single-token completion, and foreground/background time
separation. Validation: 906 Rust library tests passed (6 ignored), 163 launcher
tests passed, and three binary negative cases rejected invalid local-cache
configurations or exhausted pinned capacity without panicking.

The 29-case CPU simulation matrix and its protocol receipt checks use existing
H200 model profiles. They validate causal/resource behavior, not physical GPU
performance or a production Mooncake implementation. Store-disabled unified/PD
modeled summaries and every raw-table value match base `4886453`; the baseline
itself produces two orders for simultaneous `request_slo` rows, reproduced in
six unchanged-base runs, so that table is compared by request ID.

### Combined review validation (2026-09-30)

The subsequent modeling/observability correction passed 915 simulator tests
(6 ignored), 219 Analyzer tests, and the dedicated CPU CI commands. The full
pytest rerun reported 2827 passed, 17 skipped and three failures already
reproduced on the unmodified baseline: render-input mtime invalidation, the
documented profiling-environment registry, and immutable-profile artifact files.

All 37 simulations completed, including 30 remote runs passing the checked-in
protocol audit. Seven matching direct/remote pairs passed configuration and
workload-conservation checks and produced Analyzer comparisons, including the
512-request unified and PD workloads. Local artifacts are under
`logs/20260930_3_distributed_kv_combined/`: `protocol-validation.json`,
`paired-comparison.json`, and `equivalence/comparison.json`. Each of the six
presets has its own launcher sweep manifest and standard Analyzer outputs.
These paths are generated evidence, not committed fixtures.

Unified and PD store-off modeled values match base `4886453` exactly under the
documented schema/order exceptions. Both base and feature HP/AFD attempts stop
at the same missing grouped-GEMM profile row. `just test-all` stops during
DeepGEMM dependency installation because `CUDA_HOME` is absent. A CUDA/profile
host must complete those gates before claiming full issue acceptance. The new
GitHub workflow is committed and its commands passed locally; a hosted CI run
requires pushing the branch.
