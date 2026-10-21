# KV storage reference parity

This gate runs pinned upstream Mooncake and kvcached code, normalizes the
deterministic observations shared with ServingStudioSim, and requires exact
equality for that common semantic subset.

## Pinned references

| Project | Commit | Upstream source used |
| --- | --- | --- |
| Mooncake | `d236a313f888bd06f62a0d95dd06cb6b19b1b52a` | `mooncake-store/include/eviction_strategy.h` |
| kvcached | `a7dfbfe27b0219db6e9e27f5b1b724fdccf8dc6d` | `kvcached/kv_cache_manager.py` |

`tools/generate_kv_storage_oracles.py` rejects a checkout at any other commit,
records the upstream source SHA-256, executes the real implementation, and
emits normalized JSON. The checked-in result is
`tests/fixtures/kv_storage_external_oracles.json`.

Mooncake's inline LRU/FIFO classes are compiled directly from its pinned header.
The only adapter replaces the store-wide `types.h` dependency with the exact
`ErrorCode` type needed by that header; the policy classes themselves are not
reimplemented. The trace is Mooncake's upstream eviction-test sequence:

- LRU must evict `key1`, then `key4`.
- FIFO must evict `key1`, then `key2`.

The simulator runs the same operations against `PrefixCache`, verifies the
same victims, and additionally verifies that a shared copy is non-destructive.

kvcached's pinned `KVCacheManager` runs with CPU fakes for CUDA mapping and one
already-mapped eight-block page. Its real allocation/free methods produce this
exact sequence:

| Operation | Requested | Accepted | Success | Used | Free |
| --- | ---: | ---: | --- | ---: | ---: |
| allocate | 5 | 5 | true | 5 | 3 |
| allocate | 4 | 0 | false | 5 | 3 |
| release | 5 | 5 | true | 0 | 8 |
| allocate | 8 | 8 | true | 8 | 0 |

The simulator runs the same trace with pinned `SessionStore` reservations and
requires every normalized field to match. This proves all-or-nothing admission,
release accounting, and capacity conservation for the common abstraction.

## Regeneration

With checkouts at the pinned commits:

```bash
uv run --no-sync python tools/generate_kv_storage_oracles.py \
  --mooncake /path/to/Mooncake \
  --kvcached /path/to/kvcached

MOONCAKE_CHECKOUT=/path/to/Mooncake \
KVCACHED_CHECKOUT=/path/to/kvcached \
uv run --no-sync pytest tests/test_kv_storage_external_oracles.py -q
```

The second command regenerates the observations and compares the parsed JSON
exactly with the fixture. Without the two environment variables, provenance and
scope are still checked and regeneration is reported as skipped.

## Exactness boundary

“Exact” deliberately does not mean that the three systems are interchangeable.

The exact shared subset is:

- Mooncake LRU/FIFO victim order for the pinned operation trace;
- kvcached all-or-nothing allocation and release accounting for the pinned
  fixed-capacity trace.

The following are not comparable and are explicitly excluded:

- Mooncake object bytes, sharding, replication, leases, metadata consistency,
  failure recovery, and real network transport;
- kvcached CUDA virtual-address mapping, physical-page geometry, page retirement,
  and inference-engine integration;
- ServingStudioSim timing, placement, session directory, request lifecycle,
  pinned PD relay, and observability events.

Mooncake is a useful reference for a disaggregated store. kvcached is a useful
allocator reference, but it is not a disaggregated DRAM KV store: it manages GPU
KV capacity through CUDA virtual memory. Claiming whole-system equality with
either project would therefore be false; the fixture records both the exact
claims and these exclusions.
