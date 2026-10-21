# Validation of `distributed_kv_audit_zh.md`

Date: 2026-10-14. Subject: `doc/distributed_kv_audit_zh.md` (14 chapters, 94 lesson
steps, #16 requirement map, industry comparison).

## Scope and method

- **Code baseline:** `distributed-kv` at `cf56bfd`. The working tree also has
  uncommitted changes. Another session kept editing it during this review, adding
  `tools/check_kv_coverage.py`, the oracle files, and new tests. Every uncommitted
  Rust hunk is inside a `#[cfg(test)]` module, so the production code is identical to
  `cf56bfd`. That held when this report was written, and the edits by then included
  new test modules in `deployment/{pd,unified}.rs` and `orchestrator/config.rs`.
  Those new tests may close some of the "untested" gaps listed below. Line numbers
  refer to production code. Tests are cited by name because their line numbers are
  still moving.
- **Method:** I split each "why correct" cell into its separate factual claims and
  checked each one against the source. Six reviewers did the first pass, one per
  group of chapters. I then re-checked every ❌ and every substantive ⚠️ in this
  report directly in the source; the main text names the evidence for each. Where I
  disagreed with a reviewer, the verdict here is mine: one claim was downgraded from
  ❌ to ⚠️ and one from ⚠️ to ✅.
- **Gate re-run:** `just test-distributed-kv` in tmux session
  `kv-audit-validation-gate`, log `tmp/distributed-kv-audit-validation-gate.log`.
  Result: **pass**. 46 Rust tests passed, and the Python suite had 14 passed and
  1 skipped (the optional upstream regeneration). Separately, one reviewer re-ran the
  oracle generator against the pinned checkouts in `/tmp/mooncake-reference` and
  `/tmp/kvcached-reference`. The output matched the fixture byte-for-byte, and the
  recorded source SHA-256 values also match.

Verdicts:

- ✅ **correct** as written. Small caveats may be noted.
- ⚠️ **partly correct or overstated.** The mechanism exists, but the wording claims
  more than the code or tests support.
- ❌ **contains a false factual claim.**

Abbreviations: `flows` = `orchestrator/impls/kv_store_flows.rs`, `router` =
`orchestrator/impls/kv_store_router.rs`, `ss` = `worker/workers/kv_store/session_store.rs`,
`sw` = `worker/workers/kv_store/kv_store_worker.rs`, `rps` =
`worker/admission/remote_prefill_state.rs`, `fa` = `worker/kv/full_attn.rs`, `pc` =
`worker/kv/shared/prefix_cache.rs`, `val` = `tools/validate_distributed_kv.py`. All Rust
paths are under `simulator/src/`.

## Summary

| Section | ✅ | ⚠️ | ❌ |
| --- | ---: | ---: | ---: |
| 01 Unified flow | 4 | 1 | 0 |
| 02 PD flow | 4 | 2 | 0 |
| 03 Router | 4 | 1 | 1 |
| 04 SessionStore / PrefixCache | 4 | 1 | 0 |
| 05 Store worker | 4 | 3 | 0 |
| 06 Remote admission | 5 | 0 | 1 |
| 07 Prefill lifecycles | 2 | 4 | 0 |
| 08 DecodeCore / PD shells | 3 | 4 | 0 |
| 09 GPU KV ownership | 6 | 0 | 0 |
| 10 Config / deployment | 10 | 0 | 0 |
| 11 Network calendar / L7 | 3 | 1 | 0 |
| 12 Logs / Analyzer | 5 | 3 | 0 |
| 13 Regressions / audits / oracles | 5 | 5 | 1 |
| 14 Presets / limits | 5 | 2 | 0 |
| **Steps total (94)** | **64** | **27** | **3** |
| #16 requirement map (9 rows) | 8 | 1 | 0 |

**Bottom line.**

- The audit's headline conclusion holds. I found nothing that breaks the core #16
  protocol:
  - the directory is published only after landing;
  - the store makes an authoritative reservation before any transfer;
  - the relay stays pinned until decode reads it on the normal path;
  - each transfer is submitted once, by the receiver;
  - the foreground horizon is separate from background writeback.
- The issue-number finding (#18 is unrelated, #16 is the RFC) is correct.
- The audit's honesty about the CUDA/HP/AFD gap is also correct.
- Three step-level claims are false. The 27 overstatements fall into four groups:
  - claims that are true only for pinned relays or only on the normal path;
  - tests that prove less than the audit says;
  - log and oracle evidence that is weaker than described;
  - "only / never / all" wording that the code does not support.

### The three incorrect claims

1. **`local-first`: "then look up only the shortfall" (再对不足部分发 lookup) is
   wrong.**
   - The lookup asks for the whole declared prefix
     (`rps:220 self.lookups.push((req, session, declared))`).
   - The read transfers the whole remote hit
     (`remote_cache_iter_worker.rs:212-217`, `plan.tokens * bytes_per_token`).
   - A partial local hit is not used while the request waits.
   - `doc/distributed_kv.md:86` states the real design: "transfers the whole hit …
     rather than only the missing suffix".
   - Consequence: `kv_store_read` bytes are not one-to-one with `remote_hit_tokens`,
     which counts only the added increment.
2. **`directory-lookup`: "a store's 0-hit reply can correct a stale entry" is wrong.**
   - The lookup reply falls through to `router:278 _ => self.replies.push(event)` and
     never touches the directory.
   - Stale entries are removed only by `Evicted` receipts or a zero-token
     `PushComplete` from the same store (`router:255-273`).
   - The test `stale_directory_lookup_returns_an_ordinary_zero_hit` checks only that
     0 is returned. The entry stays in the directory.
3. **`audit-attribution`: there is no "background count" cross-check.**
   - The validator never reads `background_writes`; the word "background" does not
     appear in `val`.
   - The admitted-token check and the completion-horizon check do exist
     (`val:104-118`, `val:154-156`).

### The most important overstatements

- **Capacity failure handling is pinned-only.**
  - `ss:68-71` rejects only `PinnedUntilFetched`. A shared push is truncated to the
    available space, possibly 0, on the first-choice store, and is never retried on
    another store.
  - Unified writes are always `Shared`
    (`remote_cache_prefill_decode_admission.rs:131`). So the unified "exhausted
    candidates → health error" path cannot run; unified capacity pressure loses data
    silently instead.
  - Affects: `unified-replies`, `placement-candidates`, and the "atomic allocation"
    similarity to kvcached.
- **Cancellation is mostly unreachable and only partly tested.**
  - `Flow::cancel` (`orchestrator/mod.rs:37`) has no L7 caller. Its only production
    trigger is a PD relay `PushRejected` (`flows:379-381`).
  - The unified cancel path has no test.
  - The staged PD cancel test does not reach the lookup, decode backlog/active, or
    held-write stages.
  - On cancel, `KvConsumed` and the store unpin are sent before the read lands
    (`remote_pull_decode_worker.rs:136-144`). `types.rs:223` documents this, which
    contradicts the audit's "only after landing".
- **Tests are weaker than described.**
  - The "action-equivalence" test compares the direct `PdFlow` against the remote
    standalone flow, and both run the new `DecodeCore`
    (`flows:967-980`, `Complete` actions only). It does not lock behavior against the
    code before the refactor.
  - The peak regression test does not stagger held and non-cache peaks against each
    other. They peak on the same submit in both windows (70/80, then 50/60).
  - No test puts a cancel and a landing on the same store tick.
  - The committed negative tests corrupt or rename rows and delete whole streams.
    They never delete a single event. The concurrent uncommitted edits are adding
    such tests.
- **Failure evidence covers request logs only.** `run_sim` finalizes and flushes only
  `LoggerSession`, which holds the request state/SLO logs (`run.rs:282-290`,
  `session.rs:175-183`). The store and cluster logs are flushed by `Drop` when `main`
  drops `flow`. The negative test uses a flow with no KV store.
- **The oracles are narrower than "directly executes Mooncake policy / KVCacheD
  manager".**
  - The Mooncake `LRUEvictionStrategy`/`FIFOEvictionStrategy` classes are referenced
    only by `mooncake-store/tests/eviction_strategy_test.cpp`. Production Mooncake
    evicts through `MasterService::BatchEvict`, driven by leases and watermarks.
  - The project is **kvcached**, not "KVCacheD". Its real `alloc`/`free` methods run on
    heavy scaffolding: `torch`/`posix_ipc` are `MagicMock`s, `vmm_ops` is stubbed, and
    `__init__` is bypassed with `object.__new__` plus hand-set private fields.
  - In Rust, the kvcached release row is a literal
    (`allocator_observation("release", 5, 5, true, …)`) and the traces are
    hard-coded rather than read from the fixture.
  - Upstream regeneration is skipped in both the gate and CI.
  - The checkouts live in `/tmp`, and no task record mentions that location.

## Section-by-section verdicts

### Intro and summary

| Claim | Verdict | Note |
| --- | --- | --- |
| #18 is a merged, unrelated alignment PR; #16 is the distributed-KV RFC | ✅ | Checked with the GitHub API. PR #18 "Improve alignment analysis and kernel workflow guidance" was merged on 2026-08-25. Issue #16, "[RFC]: distributed KV cache…", is open, and its scope matches the audit. |
| 14 chapters, 94 steps (92 + 2 oracle steps) | ✅ | Counted by parsing the lesson YAML. |
| Core invariants (publish after landing, reserve before transfer, pin until read, identity-scoped cancel, foreground horizon) | ✅ | Each one holds on the normal path. The pin is released early only on cancel; see `remote-decode-consume`. |
| "Four commits" added monotonic landing, two-copy accounting, retry, failure evidence and negative audits | ⚠️ minor | `git log -S` attributes these to 64b4068, d781bae and cf56bfd. edc6556 contributed none of them. |
| Only `session → token count` is modeled; no Mooncake bytes/replicas/leases; no kvcached VMM | ✅ | `ss:10-16`. |

### 01 · Unified flow

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| unified-state | ✅ | `flows:17-22, 37-44`, one shared `GpuCluster` calendar. Nit: `cancel` also removes an entry from `writes` (`flows:86`), not only the ack. |
| unified-entry | ✅ | Record before admit at `flows:58-62`; `background_pending` at `flows:66-68`; drain at `run.rs:241-251`. |
| unified-cancel | ✅ | Call order is store, then write, then broadcast (`flows:83-88`; `simple_dp.rs:121-129`). Caveats: no L7 caller and no unified cancel test, so this is confirmed by reading only. The store-side release is queued until the next router tick. |
| unified-replies | ⚠️ | Router-before-compute order and ack routing are correct. The "exhausted candidates → health error + cancel" path (`flows:125-128`) **cannot run in unified**: shared writes are never rejected (`ss:68-71`) and are truncated, possibly to 0. |
| unified-events | ✅ | Offer before `router.push` (`flows:163-165`); same-tick `RequestComplete`; test `next_turn_can_use_local_prefix_while_shared_writes_are_outstanding`. |

### 02 · PD flow

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| pd-state | ✅ | Two pools and two producer maps (`flows:178-185`); separate direct and host link costs (`flows:205-211`). |
| pd-entry | ✅ | Arrivals go to prefill only; the idle check covers prefill, decode and store (`flows:232-246`). |
| pd-cancel | ✅ | Every map is cleaned, and identity guards hold (`ss:101-120`, `router:243`). Caveats: the only trigger is a relay `PushRejected`, and test `pd_cancel_at_each_protocol_phase…` covers neither the lookup phase (prefix 0) nor the final push. |
| pd-prefill-events | ⚠️ minor | There are **four** arms, not three. Session-less + output=1 releases locally (`remote_cache_prefill_handoff_admission.rs:110-111`). The direct-path semantics are preserved. |
| pd-ack-dispatch | ⚠️ | Routing by map membership is correct (`flows:347-377`). An unknown ack **panics** (`.expect(...)`, `flows:365-368`) instead of going through `failure`/`check_health`, which skips the failure-evidence flush. A duplicate prefill ack that arrives while a decode write is outstanding is taken as the decode ack, not rejected. The unified flow ignores unknown acks entirely. |
| pd-decode-events | ✅ | On the normal path, `KvConsumed` follows landing and the final write is a `Shared` push (test `pd_relay_unpins_and_decode_push_is_not_relayed_again`). For the cancel exception, see ch08. |

### 03 · Router

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| router-state | ✅ | Directory and pending pushes live in the router (`router:14-30`). |
| directory-lookup | ❌ | A miss returns 0 without waking any store (test `directory_miss_never_dispatches_to_store`), and a hit goes to exactly one store. **False:** "a store's 0-hit reply corrects the stale entry". The reply is passed through unchanged (`router:278`); only `Evicted` receipts or a zero-token `PushComplete` clear an entry. |
| placement-candidates | ⚠️ | The policies only order candidates, and load includes reservations (`router:123-141`; `ss:29-31`). However, "the store reservation decides whether it fits" applies to pinned writes only. A shared write never fails to fit; it is truncated on the first candidate. Pushes queued but not yet reserved are not counted in load. |
| authoritative-retry | ✅ | Each request keeps its own `VecDeque` of untried candidates, and each is tried at most once (`router:154-180, 275-277`; test `queued_pin_rejection_retries_an_untried_store`). Scope: pinned relays only. |
| directory-receipts | ✅ | The only production insert is on `PushComplete` (`router:256`); eviction is guarded by matching the store (`router:266-273`). |
| router-cancel-wakeup | ✅ | Cancel is sent to every store with an immediate wakeup, and replies for cancelled requests are filtered out (`router:216-245`). Nit: the `cancelled` set is never pruned. |
| Prose: directory vs candidate ownership | ✅ | This is a design rationale and agrees with the code. |

### 04 · SessionStore / PrefixCache

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| store-ledger | ✅ | Load = `used_charge + reserved` (`ss:29-31`). Nit: a pin is a flag on resident data, not a third amount added on top. |
| cache-copy-pin | ✅ | `copy` is non-destructive, and pinned entries are excluded from victims (`pc:271-280, 425`). Nit: `copy` updates the metadata the same way under every policy; the policies differ only in which field `victim()` reads. |
| store-reserve | ✅ | The transfer is submitted only after the reservation, and a pinned reservation is all-or-nothing (`ss:68-70`; `sw:211-257`). |
| store-land | ⚠️ | The reservation is converted before insertion. But truncation happens at **reserve** (`ss:71`); land asserts an exact fit (`ss:90-93`, "reserved bytes must fit on arrival"). |
| store-consume-cancel | ✅ | Release is identity-matched (`ss:101-120`; test `cancellation_releases_reservation_and_stale_ids_preserve_new_pin`). |

### 05 · Store worker

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| storage-shell-state | ✅ | `sw:28-38`; active and queue counts are always 0 (`sw:75-80`). |
| storage-tick | ⚠️ | The order is cancel, then land, then messages (`sw:81-96`). But the cited "same-tick" test, `lookup_observes_push_that_is_due_on_the_same_tick`, covers only landing-before-lookup. **No test has a cancel and a landing due on the same tick.** The "leak" rationale is also off: cancel is identity-based either way. What the order actually prevents is a cancelled shared write landing as an unreachable resident entry. |
| storage-start-push | ✅ | Reserve, then submit; a failed reserve returns `CapacityExceeded` with nothing transferred (`sw:211-262`). |
| storage-waiting | ✅ | Same-session writes are serialized (`ss:48-50`, `sw:116-124`); other sessions proceed but share the store's receive link. |
| storage-land-ack | ✅ | Land happens before `PushComplete`, and the ack carries the actual token count and `store_send_gid` (`sw:264-289`). |
| storage-receipts | ⚠️ | Retain/evict rows are true before/after receipts (`sw:299-320`). On reserve/pin rows, both `cache_used_*` columns are the same post-call snapshot (`sw:240-241, 276-277`), and the reservation/pin change appears only as post-state `reserved_tokens`/`pinned_tokens`. `push-rejected` rows carry zeros. The validator checks reserve rows as `after + reserved ≤ capacity`, not as a receipt chain. (A reviewer marked this ❌; I downgraded it, because resident occupancy really does not change on these rows.) |
| storage-cancellation | ⚠️ minor | Identity removal everywhere is correct. "Network time already spent is not rolled back" understates it: the **whole** booked transfer window stays reserved on both links (`gpu_cluster.rs:440-443`), including the part not yet used. |
| Prose: storage pool, not compute | ⚠️ minor | No model, admission or decoder KV is correct. It does hold an entry in the shared device table, marked `HostMemory`. "Fault domain" is not modeled anywhere: there is no store-failure path. |

### 06 · Remote admission

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| selected-state | ✅ | Pending and selected are kept separate (`rps:21-34, 211-219`; test `selected_lookups_obey_backpressure_and_do_not_reenter_pending`). Nit: a cancelled in-flight read stays in the shell's `reads` queue and on the link calendar; its landing is ignored. |
| local-first | ❌ | A sufficient local hit does skip the lookup, and the normal gates still apply. **False:** "look up only the shortfall". The whole declared prefix is looked up and the whole hit is read (`rps:220`; `remote_cache_iter_worker.rs:212-217`; `doc/distributed_kv.md:86`). Also, the `max_reads` limit `break`s the loop (`rps:208-209`), so local-only requests queued behind it wait. |
| lookup-to-read | ✅ | 0 → ready, positive → read plan (`rps:85-109`); install happens only after landing. |
| skip-obsolete-read | ✅ | `rps:111-138`; test `local_growth_before_transfer_submission_skips_obsolete_read_and_keeps_affinity`. |
| read-arrival | ✅ | Landing re-checks the local prefix (`fa:71-76`; test `remote_landing_and_push_ack_cannot_shrink_newer_local_prefix`). The landed copy is unpinned until admission; the design doc acknowledges this. |
| resume-normal-gates | ✅ | Admission resumes through preview → budget → fit → reserve (`rps:230-269`). Note: the PD remote gates add a budget check and multi-partition placement that the store-off PD prefill does not have. |

### 07 · Prefill lifecycles

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| unified-admission-messages | ⚠️ | "The lifecycle defines only terminal behavior" overstates it. It also owns message dispatch and a near-copy of the local `complete_iteration` (`remote_cache_prefill_decode_admission.rs:43-115`). |
| unified-terminal-push | ✅ | Hold, offer, then complete (`…decode_admission.rs:116-139`). Nit: the hold is also released on cancel or rejection, not only on ack. |
| pd-admission-messages | ⚠️ minor | The shared `RemotePrefillState` is reused with no mode branch. But "avoids copying" overstates it: the struct, `new`, and the `RemoteKvAdmission` impl are near-identical between the two lifecycles. |
| pd-prefill-terminal | ⚠️ minor | There are four arms, not three (see `pd-prefill-events`). The control group is preserved, as shown by test `standalone_pd_matches_direct_link…`. |
| remote-iteration-shell | ⚠️ minor | It does compose `IterBatchWorker`. But "only takes over messages and landing" is inaccurate: `enqueue` passes messages straight through, and the shell also submits reads, drains lookups and logs reuse (`remote_cache_iter_worker.rs:186-229`). |
| receiver-read | ✅ | The only store-side `submit_transfer` is the push (`sw:248`), and reads are submitted by the GPU receiver. Nit: "no longer sends pull" implies a removal, but no store-side pull existed anywhere in this range. |

### 08 · DecodeCore and PD shells

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| decode-state | ⚠️ minor | The core does own the pull pipeline and the FSM. But it also contains remote-specific logic: it chooses the transfer kind from the session field and has `cancel_pull` (`decode_core.rs:247-271, 433-445`). |
| decode-pull-arrival | ✅ | `decode_core.rs:203-213`. |
| decode-admit | ✅ | Nit: the gate is `fits()` (`decode_core.rs:325`), and `remaining == 0` skips it. That predates this change and cannot happen on the remote path. |
| direct-decode-adapter | ⚠️ | The direct shell's protocol is preserved, and its old unit tests are kept. But the cited "action-equivalence" test compares direct PD with remote-standalone PD, **both on the new core**, and compares only `Complete` actions (`flows:967-980`). It cannot detect a regression in the shared core. The real store-off evidence is the `compare_store_off` artifact run. |
| remote-decode-input | ✅ | Typed `Handoff`/`StandaloneHandoff` messages exist (`types.rs:191-245`). Nit: the core picks the transfer label from the session field. |
| remote-decode-consume | ⚠️ | On the normal path this is correct. On cancel, `KvConsumed` and the unpin are sent **before** landing (`remote_pull_decode_worker.rs:136-144`). `types.rs:223` says "after read arrival or cancellation", and the unit test asserts that behavior. |
| remote-decode-cancel | ⚠️ | Shell, core and pin cleanup are all correct. But "tests cover per-stage cancellation" overstates it: decode backlog, active and held-write are never cancelled in any test. |

### 09 · GPU KV ownership

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| monotonic-landing | ✅ | Install only when the remote copy is longer (`fa:73-76`). "Never regresses" depends on the HBM slack invariant. `insert` removes the old entry before clamping to the physical limit (`pc:345-356`), so the cache can regress if that invariant is broken; see additional finding 1. |
| held-and-retained | ✅ | Held KV is counted in both `fits` and `non_cache_peak` (`fa:507-518, 743-755`). Applies to `FullAttnKv` only; hybrid models are rejected for remote (`deployment/unified.rs:69-72`). |
| request-owned-restore | ✅ | `push_return_owners`, revoked in `retain_prefix` (`fa:775`) and on activation. |
| restore-regression | ✅ | The owner check comes before the length check; test `push_ack_restores_capacity_limited_copy_but_never_a_superseded_version`. |
| remote-hit-attribution | ✅ | `fa:132-139`; test `extending_read_credits_only_the_added_usable_prefix_as_remote`. |
| kv-cancel | ✅ | `fa:177-196`; test `push_copy_obeys_physical_slack_and_cancel_releases_hold`. |

### 10 · Configuration and deployment — all ✅

All ten steps match the code. These relevant behaviors exist but have no tests:

- the two-way rejection between `kv_store` and the remote selectors (`unified.rs:61-65`,
  `pd.rs:62-67`);
- the PD bytes-per-token check (`pd.rs:382-385`);
- `KvStorePoolSpec::validate`;
- the unified-side disabled-cache rejection.

The launcher tests also use a hand-written schema fixture rather than the real `dump.rs`
output.

### 11 · Network calendar and L7

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| host-endpoint | ✅ | `gpu_cluster.rs:291-296, 354-359`. |
| transfer-calendar | ✅ | `gpu_cluster.rs:425-443`. |
| foreground-horizon | ✅ | `run.rs:241-281`; test `background_drain_keeps_foreground_horizon_and_completion_rows`. |
| failure-evidence | ⚠️ | Request logs are finalized and flushed before the error returns (`run.rs:282-290`). **Store and cluster logs are not**: they flush on `Drop` in `main.rs`. Test `protocol_failure_finalizes_partial_requests_and_flushes_before_returning` uses a flow with no store and checks only the SLO and state logs. |

### 12 · Logs and Analyzer

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| store-event-shape | ⚠️ | Only the newer optional columns are nullable. 13 core columns are non-null and filled with `0`, for example `store_worker=0` on `request-complete` and `session_id=0` on `transfer` rows. Store 0 is a real store id, so "not applicable" cannot be told apart from it. |
| event-sequence | ⚠️ | One sequence covers every row in `kv_store_event` (router, store, cluster, flow). "All KV events" overstates it: `prefix_cache_event` has its own per-worker sequence, and `gpu_cluster` has none. |
| transport-events | ✅ | The only `transfer` emitter is `gpu_cluster.rs:453`, inside `submit_transfer`. |
| device-metadata | ✅ | The new `devices` field exists, and `gpus`/`gpu_ids` stay GPU-only. Note: `workers[]` now also lists store workers. |
| independent-peaks | ✅ | `kv_sampler.rs:183-186`. |
| snapshot-wire | ✅ | Old logs show null, not 0, in the Analyzer (`occupancy.rs:86-87, 185-186`). |
| peak-regression | ⚠️ | The test staggers held and non-cache peaks only against the active/retained peak. Held and non-cache always peak on the same submit, so a sampler that read both from one snapshot would still pass. |
| analyzer-raw-peaks | ✅ | `occupancy.rs:265-272`; legacy behavior is covered by a test. |

### 13 · Regressions, audits, oracles

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| always-miss-protocol | ✅ | Every lookup gets 0; the summary (except wall-clock fields) and the request state/SLO logs must match exactly. |
| copy-interleaving | ✅ | Test `concurrent_copies_survive_eviction_and_writeback_during_the_other_read`. |
| protocol-audit | ✅ | Replay in sequence order with read-before-unpin causality (`val:31-73`). Nit: reserve rows are checked one at a time, not chained. |
| audit-attribution | ❌ | The admitted-token and horizon checks exist. **The background-count check does not.** |
| require-evidence | ⚠️ minor | The store stream is explicitly required. A missing `gpu_cluster`/`request_slo` raises a raw `FileNotFoundError` instead of the explicit validator error, and no test covers it. |
| audit-negative-tests | ⚠️ minor | The committed tests corrupt rows (capacity, early pin/unpin, stale unpin, receipt, endpoint, completion) and delete whole streams, but never delete a single event. The concurrent uncommitted edits are adding such cases. |
| store-off-equivalence | ⚠️ minor | Comparison is stricter than "per request ID": every stream is compared exactly in row order, and only `request_slo` is sorted. The exclusions also include the non-additive `wall_s`/`realtime_x` and the `schema_version` value. |
| pair-comparison | ✅ | `val:249-263`. Nit: the profile DB identity is not compared. |
| cpu-gate | ✅ | Re-run here and passed. Caveats: the oracle lines in `justfile` are uncommitted, upstream regeneration is skipped by default, and "audit" here means unit tests on synthetic rows. |
| external-oracle-generation | ⚠️ | Commits and SHA-256 values are pinned and verified, and Mooncake's header really is compiled. But the Mooncake classes are used only by Mooncake's own tests, not its production eviction. kvcached runs real methods on mocks with a bypassed constructor. The project name is **kvcached**. |
| external-oracle-parity | ⚠️ | Rust compares the full normalized JSON, but its traces are hard-coded and the release row is a literal. The Python exact regeneration only runs when opted in. |
| Oracle paragraph | ✅ | Matches the fixture and the parity doc, apart from the name. |

### 14 · Presets and acceptance limits

| Step | Verdict | Evidence / discrepancy |
| --- | --- | --- |
| smoke-matrix | ✅ | `stores [1,2]` × 3 placements, on a chained multi-turn trace. |
| pd-controls | ⚠️ | The remote grid also varies tick, trace (no_reuse, load64) and `max_inflight_remote_reads`, not only the store factors. Only 5 of the 16 remote cases have a direct control. |
| unified-controls | ✅ | The presets differ only in worker type and the `kv_store` block. |
| reuse-fixtures | ✅ | prefix + input is identical on all 28 rows. |
| load-generator | ✅ | 64 × 8 = 512 rows; regeneration produces an identical SHA-256. |
| accepted-design-deltas | ⚠️ minor | Receive-backlog and the two KV copies are listed. Whole-hit read is described as RFC behavior (`doc:85-86`) and is not in the deltas list. |
| acceptance-boundary | ✅ | `doc/distributed_kv.md:336-348`. |

### #16 requirement map

Rows 1–7 and row 9 are ✅. Row 2 does not mention that placement uses the receive
backlog, not the send backlog, but that deviation is documented elsewhere. Row 9 also
leaves out a change: the RFC's expected direction for TTFT and hit rate was replaced by
"measure, don't assert" (`doc:301-304`).

**Row 8, "store-disabled behavior preserved", is ⚠️.** The RFC asks for bit-identical
store-off output for unified, PD, HP and AFD. The evidence covers unified and PD only,
and shows equal modeled values with schema and ordering exceptions, not bit identity.

### Industry comparison

The ServingStudioSim column is ✅, except that the policy list leaves out `LargestFirst`
(`pc:35-44`). The Mooncake column is consistent with the upstream checkout, though
"distributed metadata/master" is loose: Mooncake's master is logically centralized with
HA. The kvcached column matches the parity doc. One ⚠️: the claimed similarity to
kvcached in "atomic allocation" holds only for pinned reservations, because shared
pushes accept partial amounts.

## Additional findings not raised by the audit

These come from reading the code and were not reproduced by any test.

1. **HBM can be over-committed for a moment.** Confirmed by a failing test; see
   the follow-up section.
   - When a prefill is admitted, its promise moves into `prefill_admits` (`fa:545-548`).
     `non_cache_peak` counts only promised, held and decode-resident tokens
     (`fa:743-749`; `resident_partition.rs:298-302`).
   - A remote read that lands while that prefill is computing
     (`remote_cache_iter_worker.rs:193, 224-229`) can therefore fill the slack the
     prefill is about to use.
   - `commit_resident` does not trim the cache (`fa:550-560`), so occupancy can exceed
     capacity until the next `reserve`.
   - Direct PD had a similar window before this change; the remote read adds a new way
     to hit it.
2. **The directory is last-writer-wins.** Reproduced by a test, then classified
   as out of current scope; see the follow-up section. `router:255-256` overwrites
   on every non-zero `PushComplete`. If two writes for the same session complete
   out of order on different stores, the directory can fall back to the older,
   shorter copy.
3. **A cancelled transfer keeps its full link reservation.** This applies to
   cancelled pushes, and also to reads, which additionally stay in the shell's queue
   and keep setting the next wakeup.
4. **A protocol bug in PD ack dispatch panics** (`flows:365-368`) instead of reaching
   the failure-evidence path that chapter 11 describes.

## Conclusion

The audit is reliable on structure, ownership and the main protocol invariants: 64 of
94 steps are fully correct. Three explicit errors should be fixed:

- `local-first`: the whole hit is read, not the shortfall;
- `directory-lookup`: `Evicted` receipts, not lookup replies, clear stale entries;
- `audit-attribution`: there is no background-count check.

The "why correct" wording should also stop presenting pinned-relay behavior as general
store behavior, and stop citing tests for cases they do not exercise (same-tick cancel,
staged decode cancel, staggered held/non-cache peaks, pre-refactor direct
equivalence). The oracle section should say that Mooncake's LRU/FIFO classes are
test-only reference code, that kvcached runs under mocks, and spell the project as
**kvcached**. None of these discrepancies changes the audit's acceptance position:
CPU-level protocol evidence passes, and the CUDA/HP/AFD/profile gates remain open.

## Follow-up: explanations and tests (2026-10-14)

Rule applied: when a wrong claim affects simulator correctness, add a test that
exposes it; otherwise explain it and leave it. Evidence gaps were re-checked only
after the concurrent Codex session finished. That session added 50 behavior tests
and an added-line coverage gate (`doc/distributed_kv_test_coverage.md`).

### Wrong claims and defects

| Item | Why it is wrong | Affects correctness? | Action |
| --- | --- | --- | --- |
| ❌ `local-first` | The audit misread the design. Reading the whole hit is intended (`doc/distributed_kv.md:86`). | No | Explain only |
| ❌ `directory-lookup` | Every store-side removal is a whole-entry removal that emits `Evicted` (`sw:321-326`), and those receipts come before the `PushComplete` of the same landing (`sw:267-268` vs `281`). So a stale entry lasts only until that tick's receipts are processed, and the lookup still returns a correct 0-hit. | No | Explain only |
| ❌ `audit-attribution` | The validator never had a background-count check. The behavior it would check is covered by `background_drain_keeps_foreground_horizon_and_completion_rows` and `next_turn_can_use_local_prefix_while_shared_writes_are_outstanding`. | No (evidence wording) | Explain only |
| ⚠️ `unified-replies` | Shared writes are truncated by design (`doc/distributed_kv.md:106-107`); only the stated error path is wrong for unified. | No | Explain only |
| ⚠️ cancellation claims (`remote-decode-consume`, staged/same-tick/unified cancel) | Every production cancel comes from `PushRejected`, and `check_health()` aborts the run in the same tick (`run.rs:201-203`). | No; no successful run is affected | Explain only |
| PD unknown-ack panic; link time kept after cancel | Reachable only through an internal protocol bug or the failing-run path. | No | Explain only |
| **HBM over-commit on remote landing** | After `drain_ready` the running prefill is in neither the promise ledger nor resident state, so `prefix_cache_physical_limit` ignores it. A read that lands mid-iteration fills that space, and `commit_resident` does not trim. | **Yes**: HBM occupancy exceeds capacity | Failing test `worker::kv::full_attn::tests::remote_landing_during_a_running_prefill_leaves_room_for_that_prefill`: "running prefill peak 74 + remote prefix 50 exceed 100 HBM tokens" |
| **Directory regression on out-of-order writes** | `PushComplete` overwrites `directory[session]` with no ordering across stores. An older turn's write that lands late replaces the newer copy, and the newer copy is left unreachable. | Only with writes to different stores, which the current centralized target does not consider | Reproduced (lookup returned 50 tokens from store 0 instead of 80 from store 1), then removed from the suite as out of scope; see "Scope decision" below |

Both tests were first proven in an isolated worktree at `cf56bfd`, and every
precondition assertion passes before the final one fails. Only test code was
added. The defects themselves are **not fixed**.

### Why the remote landing over-commits HBM

The over-commit needs a remote read to land while another prefill on the same
partition is computing. That window is real in production:

1. `form_batch` reserves the prefill (a promise of post-prefill context plus
   remaining output), then calls `drain_ready` (`rps:280`).
2. `drain_ready` removes the promise and records only the request ID in
   `prefill_admits` (`fa:544-548`; `request_ledger.rs:107-117`). Until
   `commit_resident`, the running prefill's tokens are not in promised, held or
   resident state, so `non_cache_peak` (`fa:743-749`) drops by its footprint.
3. `RemoteCacheIterWorker` wakes at `min(compute_end, read_end)` and runs
   `advance_reads` before the iteration tick (`remote_cache_iter_worker.rs:193-204,
   224-229`), so a read can land mid-iteration. `install_remote_prefix` inserts
   into the cache up to `capacity - non_cache_peak` (`fa:751-755, 776`), which
   is the space the running prefill is about to use.
4. At compute end, `commit_resident` makes the prefill resident without trimming
   the cache (`fa:550-563`). Trimming happens only in `reserve` (`fa:530`).
5. If the reader is at the head of `ready` and does not fit, `form_batch` breaks
   (`rps:248-250`), so no other `reserve` trims the cache either. The over-commit
   then lasts until the running request's decode releases enough KV.

The test drives exactly this order at the KV level: reserve 70+4, `drain_ready`,
land 50, commit. Capacity is 100, so 74 + 50 = 124.

Effects appear only under HBM pressure:

- Sampled occupancy (active plus retained) can exceed HBM capacity.
- The reader can later reuse a remote prefix that a real engine could not have
  stored, because a real engine needs free destination blocks before the read.
  This makes the reported cache hits and TTFT optimistic.

The push-acknowledgement re-retention in `complete_push` (`fa:154-170`) has the
same root cause. Messages are handled on `enqueue`, mid-iteration. It applies
only when the session's entry was evicted while held. This path was found by
reading the code and is not tested.

### Scope decision (2026-10-14)

The user ruled the directory case out of scope: the current target is
centralized, and multi-store write ordering will be fixed later. The test was
removed so that the gate does not fail on a deferred case. It is not marked
`#[ignore]`, because `just bench` runs all ignored tests. Note that `replicas`
remains a sweep leaf and the smoke presets sweep 1 and 2 stores
(`doc/distributed_kv.md:39-41`), so a multi-store configuration can still reach
the case. The reproducer, for the later fix, appends to the
`kv_store_router.rs` test module:

<details><summary>Reproducer: <code>an_older_write_landing_late_cannot_replace_a_newer_directory_copy</code></summary>

```rust
    #[test]
    fn an_older_write_landing_late_cannot_replace_a_newer_directory_copy() {
        // Turn N of session 7 is written first, but its store's receive link is
        // busy. Turn N+1 comes from another producer, is placed on the other
        // store and lands first. The late, shorter turn-N copy must not become
        // the read route for the session's next lookup.
        let cluster = test_cluster();
        let gpu = cluster.borrow_mut().allocate(0, 0, 3, "test", "main");
        let filler = cluster.borrow_mut().register_comm_group(gpu, 1, "main", 0);
        let older = cluster
            .borrow_mut()
            .register_comm_group(gpu + 1, 1, "main", 1);
        let newer = cluster
            .borrow_mut()
            .register_comm_group(gpu + 2, 1, "main", 2);
        let mut router = KvStoreRouter::new(
            &KvStorePoolSpec {
                worker: Default::default(),
                replicas: 2,
                dram_gb: 0.001,
                bandwidth_gbps: 1.0,
                latency_us: 0.0,
                placement: KvStorePlacement::RoundRobin,
                backlog_weight_per_ms: 1.0,
                policy: PrefixCachePolicy::Lru,
            },
            1,
            &cluster,
        );
        // Construction appends one communication group per store.
        let store0 = newer + 1;
        cluster
            .borrow_mut()
            .submit_transfer(Time::ZERO, filler, store0, 1_000_000, "test", "");
        router.push(
            WorkerId(0),
            RequestId(1),
            7,
            50,
            older,
            KvRetention::Shared,
            Time::ZERO,
        );
        router.push(
            WorkerId(1),
            RequestId(2),
            7,
            80,
            newer,
            KvRetention::Shared,
            Time::ZERO,
        );
        let early = router.tick(Time::from_us(1));
        assert!(early.iter().any(|e| matches!(
            e,
            KvStoreEvent::PushComplete {
                req: RequestId(2),
                store: WorkerId(1),
                tokens: 80,
                ..
            }
        )));
        let late = router.tick(Time::from_ms(2.0));
        assert!(late.iter().any(|e| matches!(
            e,
            KvStoreEvent::PushComplete {
                req: RequestId(1),
                store: WorkerId(0),
                tokens: 50,
                ..
            }
        )));
        router.lookup(WorkerId(2), RequestId(3), 7, 80);
        let reply = router.tick(Time::from_ms(2.0));
        assert!(
            reply.iter().any(|e| matches!(
                e,
                KvStoreEvent::LookupResult {
                    req: RequestId(3),
                    resident_tokens: 80,
                    ..
                }
            )),
            "newer 80-token copy on store 1 was replaced as the read route: {reply:?}"
        );
    }
```

</details>

### Evidence gaps after the Codex session

The Codex session closed these gaps: single-event corruption cases (sequence gap,
missing or duplicate completion), resource validation, and the unified/PD
assembly examples. Four gaps were still open, so I added these tests; all pass:

| Test | Gap it closes |
| --- | --- |
| `deployment::unified::tests::kv_store_and_remote_selector_require_each_other_and_an_enabled_cache` | Unified two-way store/selector guard and the disabled-cache guard |
| `deployment::pd::remote_config_tests::kv_store_requires_both_remote_selectors_and_matching_relay_layouts` | All four PD selector/store mismatches, plus the relay bytes/token check (a 1-byte model against a 2-byte model) |
| `log::kv_sampler::tests::held_and_non_cache_window_maxima_come_from_different_submits` | Held, non-cache and active maxima taken from three different submits in one window |
| `orchestrator::impls::kv_store_flows::tests::rejected_relay_receipts_reach_disk_after_the_failed_flow_is_dropped` | After a protocol failure, the exact store receipt sequence is on disk once the flow is dropped |

These gaps get no test:

- **Background-count check:** see the table above.
- **Cancellation paths:** they cannot affect a successful run.
- **Missing comm/request streams:** these already fail the validator, just with a
  raw `FileNotFoundError`.
- **Pre-refactor direct-decode equivalence:** it cannot be tested in-tree, and the
  store-off artifact comparison covers it.
- **The literal kvcached release row:** its `used`/`free` fields are measured.

### Result

Full `cargo test -p simulator --lib`, after the directory test was removed:
**934 passed, 1 failed, 6 ignored**. The one failure is the HBM over-commit test.
Targeted rustfmt and `git diff --check` pass. Until that defect is fixed,
`just test-distributed-kv` and the coverage recipe stop at this failure. Logs:
`tmp/kv-audit-tests.log` (both exposing tests) and `tmp/kv-audit-scope.log`
(after the scope decision).
