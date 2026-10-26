# Distributed KV behavioral tests and added-line coverage

Measured on 2026-10-14 against base
`48864532c291e5cd48d01efb515d63733496e420`. This follow-up adds 50 test cases:
13 Rust tests and 37 Python cases, including parameterized failures. Cases check
concrete state, tokens, victims, timing/order, output fields, or a specific
rejection. No new compile-only test is used to meet the target.

These figures predate the upstream-main merge. The recipe now defaults to
merged upstream base `d2e31e7838fb6a9a2a12e1164480942f88d46bac`; coverage
has not been remeasured for that base.

## Measured coverage

| Language | Covered added executable lines | Total added executable lines | Coverage |
| --- | ---: | ---: | ---: |
| Rust (simulator + Analyzer) | 2584 | 2750 | 93.96% |
| Python (Launcher schema + validation/oracle/coverage tools + trace generator) | 457 | 498 | 91.77% |

Both language totals exceed 80%. Every entirely new executable source file also
individually exceeds 80%; the lowest is `validate_distributed_kv.py` at 83.77%.
Modified pre-existing files are evaluated on their added lines in the language
totals, rather than required to pass individually. For example,
`deployment/unified.rs` has 31.25% added-line coverage because its real
profile-backed model-building entry paths are not exercised by the deterministic
FakeModel assembly examples. This file is included in the denominator, not
omitted. The JSON report lists all file results and every uncovered line.

| Main component | Added-line coverage |
| --- | ---: |
| SessionStore | 100% |
| PrefixCache | 100% |
| KvStoreRouter | 98.20% |
| KvStoreWorker | 94.96% |
| unified/PD storage flows | 94.17% |
| remote admission state | 96.00% |
| remote unified admission | 94.89% |
| remote prefill admission | 97.56% |
| remote iteration shell | 94.55% |
| remote decode shell | 94.78% |
| extracted DecodeCore | 95.21% |
| Analyzer occupancy changes | 100% |
| upstream oracle generator | 97.42% |

Rust data comes from LLVM instrumentation, including both crates in the LCOV
export. Python data comes from coverage.py with child-process instrumentation,
so running the oracle or trace generator as a subprocess counts its execution.
Intersect measured executable lines with the added lines in `git diff BASE`
plus untracked source files. Exclude Rust `#[cfg(test)]` items, the dedicated
`always_miss_tests.rs` module, Python test files, comments, and non-executable
declarations. Test-only hits cannot increase the numerator or denominator.
The target is executable **line** coverage, not branch coverage.

## Concrete new examples

| Example | Input | Required observations |
| --- | --- | --- |
| Out-of-order landing | capacity 100; reserve session 7/60 and 8/40, land 8 first | used stays 100; reserved 100→60→0; cache 0→40→100; no visibility before landing |
| Oversized relay | resident shared 80; pinned request 101 in capacity 100 | rejection keeps resident 80 and reserved 0; shared request 101 accepts 100 with an exact eviction receipt |
| Full relay pin | pinned 100; shared request 50 | accept 0; pinned copy still reads 100; stale consume keeps pin; matching consume allows reclamation |
| LRU/FIFO pressure | store 7/40 and 8/40; copy 7; reserve 9/50 | LRU evicts 8, FIFO evicts 7; resident+reservation is 90 |
| Pin replacement/shrink | pin 7/100; replace with 20; shrink to 0 | pin survives; unpin then shrink emits one exact 100→0 receipt |
| LFU frequency | copy 7 twice; zero-length read of 8; miss on 99; insert 9 | 8 is evicted; zero-hit operations do not increase frequency |
| Empty directory write | directory 8→store1/80; empty landing on store0, then store1 | store0 cannot invalidate store1; store1 clears its own location; later lookup misses |
| Cancel replies | queue directory miss and empty push ack, cancel both | neither reaches compute; late reply filtered; new request still gets its result |
| Parquet flush | 1030 receipts across the 1024-row threshold | all sequences, timestamps, occupancy and reservation values survive flush and Drop |
| Unified assembly | two GPU replicas + two stores; turn 1 has 20 new tokens; turn 2 has prefix 20 + new 10 | second replica hits 20, computes 10, emits 2 outputs, completes once and drains writes; stores do not count as GPUs |
| PD assembly | two prefill replicas + one decode + two stores; same chained context | second prefill hits 20, computes 10, emits 3 outputs; relay/final writes drain |
| Worker config | capacity 1000 bytes, token budget 32, FIFO, remote reads 3, multiplier 1.5 | exact fields reach prefill; 2000-byte decode has disabled prefix cache |
| Resource validation | exact one-token DRAM; zero/negative/NaN/infinite resources | exact capacity passes; invalid inputs report the corresponding field |
| Paired reports | fixed 120-token context, compute 120→40, reuse 0→80, TPS 10→15 | exact metric deltas and independent per-shard peaks; decode peak does not contaminate prefill |
| Unfair pair | change tick, deployment, TP, availability, conservation or peak field | six specific errors reject the comparison |
| Relay corruption | sequence gap, double pin, pinned eviction, negative queue, early transfer end, missing/duplicate completion, wrong bytes/queue/end/store/request/endpoint | fourteen specific corruptions are rejected |
| Reuse causality | local 20 + remote 60; read ends at 3.0 ms | use at 3.0 passes, use at 2.9 fails |
| Chained trace | two sessions × three turns | exact six CSV rows; contexts 32/40/48; invalid dimensions create no output |
| Oracle identity | wrong Git revision or changed Mooncake dependency | refuses unreviewed reference before compilation/execution |
| Coverage accounting | source/diff and LCOV with 1000 test-only hits | test hits excluded; 66.67% fails; missing reports fail; undercovered new file fails even with a >80% language total |

The earlier Mooncake victim-order and kvcached allocation/release parity tests
also run against both pinned checkouts in this measurement. The fixture test now
checks actual eviction sequences and capacity transitions rather than merely
checking nonempty fields. See `kv_storage_reference_parity.md` for exact scope.

## Repeatable command

Install the measurement tools in the prepared environment:

```bash
uv run --no-sync rustup component add llvm-tools-preview
uv run --no-sync cargo install cargo-llvm-cov --locked
uv pip install 'coverage>=7.10'
```

Supply upstream checkouts at the commits listed in the parity document:

```bash
MOONCAKE_CHECKOUT=/path/to/Mooncake \
KVCACHED_CHECKOUT=/path/to/kvcached \
just test-distributed-kv-coverage
```

The recipe clears old coverage data, runs simulator/Analyzer/Python tests,
collects subprocess coverage and rejects results below 80%. It writes:

- `target/kv-coverage-reports/rust.lcov`
- `target/kv-coverage-reports/python.json`
- `target/kv-coverage-reports/added-lines.json` (file totals and missing lines)

CI now installs these tools, checks out pinned references, and invokes the same
recipe. The workflow was updated locally; no hosted CI run is claimed.

Final local result: simulator **930 passed, 6 ignored**, Analyzer **219 passed**,
Python **215 passed** with upstream regeneration enabled and no Python skips.
Ruff, targeted rustfmt and `git diff --check` passed. Session:
`distributed-kv/kv-coverage-final`; log: `../tmp/kv-coverage-final.log`.
