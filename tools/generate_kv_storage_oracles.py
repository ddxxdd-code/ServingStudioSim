"""Run pinned Mooncake/kvcached code and emit normalized storage observations.

The two projects expose different abstractions. Mooncake supplies the eviction
order oracle; kvcached supplies the all-or-nothing allocation/capacity oracle.
The output is intentionally limited to those shared, deterministic semantics.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import subprocess
import sys
import tempfile
import threading
import types
from pathlib import Path
from unittest import mock

MOONCAKE_COMMIT = "d236a313f888bd06f62a0d95dd06cb6b19b1b52a"
KVCACHED_COMMIT = "a7dfbfe27b0219db6e9e27f5b1b724fdccf8dc6d"


def _commit(checkout: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _require_commit(checkout: Path, expected: str, project: str) -> None:
    actual = _commit(checkout)
    if actual != expected:
        raise SystemExit(f"{project} checkout is {actual}; expected pinned {expected}")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _mooncake_observations(checkout: Path) -> dict[str, object]:
    """Compile and execute Mooncake's real inline LRU/FIFO implementations.

    ``eviction_strategy.h`` includes the store-wide ``types.h`` only for the
    ``ErrorCode`` return type. The oracle substitutes that enum while retaining
    the upstream strategy classes byte-for-byte; this avoids building RDMA,
    etcd, and the rest of Mooncake Store for a header-only policy trace.
    """

    _require_commit(checkout, MOONCAKE_COMMIT, "Mooncake")
    header = checkout / "mooncake-store/include/eviction_strategy.h"
    source = header.read_text()
    source = source.removeprefix("#pragma once\n")
    include = '#include "types.h"'
    if source.count(include) != 1:
        raise SystemExit("Mooncake eviction_strategy.h has an unexpected types include")
    source = source.replace(
        include,
        "#include <cstdint>\n"
        "#include <list>\n"
        "namespace mooncake { enum class ErrorCode : std::int32_t { OK = 0 }; }",
    )
    adapter = r'''
#include <iostream>

int main() {
    using namespace mooncake;
    LRUEvictionStrategy lru;
    lru.AddKey("key1");
    lru.AddKey("key2");
    const auto lru_first = lru.EvictKey();
    lru.AddKey("key3");
    lru.AddKey("key4");
    lru.UpdateKey("key2");
    lru.UpdateKey("key3");
    const auto lru_second = lru.EvictKey();

    FIFOEvictionStrategy fifo;
    fifo.AddKey("key1");
    fifo.AddKey("key2");
    const auto fifo_first = fifo.EvictKey();
    fifo.AddKey("key3");
    fifo.AddKey("key4");
    fifo.UpdateKey("key2");
    fifo.UpdateKey("key3");
    const auto fifo_second = fifo.EvictKey();

    std::cout << "lru " << lru_first << ' ' << lru_second << '\n';
    std::cout << "fifo " << fifo_first << ' ' << fifo_second << '\n';
}
'''
    with tempfile.TemporaryDirectory(prefix="mooncake-kv-oracle-") as raw_dir:
        build = Path(raw_dir)
        cpp = build / "oracle.cpp"
        binary = build / "oracle"
        cpp.write_text(source + adapter)
        subprocess.run(
            ["g++", "-std=c++17", "-O0", str(cpp), "-o", str(binary)],
            check=True,
        )
        lines = subprocess.run(
            [str(binary)], check=True, capture_output=True, text=True
        ).stdout.splitlines()
    parsed = {parts[0]: parts[1:] for line in lines if (parts := line.split())}
    if set(parsed) != {"lru", "fifo"}:
        raise SystemExit(f"unexpected Mooncake oracle output: {lines!r}")
    return {
        "repository": "https://github.com/kvcache-ai/Mooncake",
        "commit": MOONCAKE_COMMIT,
        "source": "mooncake-store/include/eviction_strategy.h",
        "source_sha256": _sha256(header),
        "trace": "upstream EvictionStrategyTest.EvictKey/FIFOEvictKey sequence",
        "observations": {
            "lru_evictions": parsed["lru"],
            "fifo_evictions": parsed["fifo"],
        },
    }


class _InternalPageStub:
    @staticmethod
    def get_num_blocks(page_size: int, block_mem_size: int) -> int:
        return page_size // block_mem_size


class _Page:
    def __init__(self, capacity: int) -> None:
        self.page_id = 0
        self.capacity = capacity
        self.free_blocks = list(range(capacity))

    def init(self, _block_mem_size: int) -> None:
        pass

    def num_free_blocks(self) -> int:
        return len(self.free_blocks)

    def alloc(self, count: int) -> list[int]:
        result = self.free_blocks[:count]
        self.free_blocks = self.free_blocks[count:]
        return result

    def free_batch(self, indices: list[int]) -> None:
        self.free_blocks.extend(indices)
        self.free_blocks.sort()

    def full(self) -> bool:
        return not self.free_blocks

    def empty(self) -> bool:
        return len(self.free_blocks) == self.capacity


class _PageAllocator:
    def __init__(self, page: _Page) -> None:
        self.page = page

    def get_resize_target(self) -> int:
        return -1

    def get_num_free_pages(self) -> int:
        return 0

    def get_avail_physical_pages(self) -> int:
        return 0

    def get_num_reserved_pages(self) -> int:
        return 0

    def group_indices_by_page(
        self, indices: list[int], _block_mem_size: int
    ) -> dict[int, list[int]]:
        return {self.page.page_id: list(indices)}

    def free_pages(self, _page_ids: list[int]) -> None:
        raise AssertionError("deferred release must keep the test page mapped")

    def alloc_page(self) -> _Page:
        raise AssertionError("the fixed-capacity trace must reuse its mapped page")


def _install_kvcached_import_stubs() -> None:
    torch = mock.MagicMock()
    torch.__version__ = "2.6.0"
    sys.modules["torch"] = torch
    sys.modules["torch.cuda"] = torch.cuda
    sys.modules["torch.utils"] = torch.utils
    sys.modules["torch.utils.cpp_extension"] = torch.utils.cpp_extension
    sys.modules["posix_ipc"] = mock.MagicMock()
    vmm = types.ModuleType("kvcached.vmm_ops")
    vmm.PageAllocator = _PageAllocator  # type: ignore[attr-defined]
    vmm.InternalPage = _InternalPageStub  # type: ignore[attr-defined]
    vmm.kv_tensors_created = lambda group_id=0: True  # type: ignore[attr-defined]
    vmm.map_to_kv_tensors = lambda *args, **kwargs: None  # type: ignore[attr-defined]
    vmm.unmap_from_kv_tensors = lambda *args, **kwargs: None  # type: ignore[attr-defined]
    sys.modules["kvcached.vmm_ops"] = vmm


def _kvcached_observations(checkout: Path) -> dict[str, object]:
    _require_commit(checkout, KVCACHED_COMMIT, "kvcached")
    manager_source = checkout / "kvcached/kv_cache_manager.py"
    sys.path.insert(0, str(checkout))
    _install_kvcached_import_stubs()
    module = importlib.import_module("kvcached.kv_cache_manager")
    locks = importlib.import_module("kvcached.locks")

    capacity = 8
    page = _Page(capacity)
    allocator = _PageAllocator(page)
    manager = object.__new__(module.KVCacheManager)
    manager._lock = locks.NoOpLock()
    manager.page_size = capacity
    manager.block_mem_size = 1
    manager.page_allocator = allocator
    manager.defer_physical_release = True
    manager._physical_release_epoch = 0
    manager._retired_pages = []
    manager._page_release_callbacks = ()
    manager._shrink_reclaimers = ()
    manager.in_shrink = False
    manager.target_num_blocks = None
    manager._avail_physical_pages_cache = 0
    manager._avail_physical_pages_ts = 0.0
    manager._operation_counters = {}
    manager._operation_error_state = (0, None, None)
    manager.reserved_blocks = []
    manager.avail_pages = {page.page_id: page}
    manager.full_pages = {}
    manager.num_avail_blocks = capacity
    manager._post_init_done = threading.Event()
    manager._post_init_done.set()

    observations: list[dict[str, object]] = []

    def record(operation: str, requested: int, accepted: int, success: bool) -> None:
        free = manager.available_size()
        observations.append(
            {
                "operation": operation,
                "requested": requested,
                "accepted": accepted,
                "success": success,
                "used": capacity - free,
                "free": free,
            }
        )

    first = manager.alloc(5)
    assert first is not None
    record("allocate", 5, len(first), True)
    rejected = manager.alloc(4)
    record("allocate", 4, 0 if rejected is None else len(rejected), rejected is not None)
    manager.free(first)
    record("release", len(first), len(first), True)
    full = manager.alloc(capacity)
    assert full is not None
    record("allocate", capacity, len(full), True)

    return {
        "repository": "https://github.com/ovg-project/kvcached",
        "commit": KVCACHED_COMMIT,
        "source": "kvcached/kv_cache_manager.py",
        "source_sha256": _sha256(manager_source),
        "trace": "fixed mapped page: allocate 5, reject 4, release 5, allocate 8",
        "capacity": capacity,
        "observations": observations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mooncake", type=Path, required=True)
    parser.add_argument("--kvcached", type=Path, required=True)
    args = parser.parse_args()
    result = {
        "schema_version": 1,
        "scope": {
            "exact": [
                "Mooncake LRU/FIFO victim order",
                "kvcached all-or-nothing allocation and release accounting",
            ],
            "excluded": [
                "object bytes, sharding, replication, leases, and distributed consistency",
                "CUDA virtual-address mapping, page geometry, and engine integration",
                "ServingStudioSim timing, routing, session-directory, and relay protocol events",
            ],
        },
        "mooncake": _mooncake_observations(args.mooncake),
        "kvcached": _kvcached_observations(args.kvcached),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
