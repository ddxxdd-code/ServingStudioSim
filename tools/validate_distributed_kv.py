"""Reproducible distributed-KV protocol, store-off equivalence and paired-report checks.

Run through uv. Protocol checks read authoritative raw receipts; performance
comparisons consume Analyzer reports and do not assert universal latency gains.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_json(path: Path):
    return json.loads(path.read_text())


def audit_protocol(
    rows: list[dict], requests: list[dict], network: list[dict], capacities: dict[int, int]
) -> dict:
    """Replay receipts and causal dependencies without a second storage model."""
    require([r["sequence"] for r in rows] == list(range(len(rows))), "event sequence gap")
    by_req = {r["request_id"]: r for r in requests}
    pins, cache, reserves, reads, completed = {}, {}, {}, {}, set()
    pushes = {}
    transfers = []
    for row in rows:
        op, req = row["operation"], row["request_id"]
        store = row["store_worker"]
        key = (store, row["session_id"])
        if op == "pin":
            require(key not in pins, "relay pinned twice")
            push = pushes.get((store, req))
            require(
                push is not None and push["transfer_end_ms"] <= row["time_ms"],
                "relay pinned before push lands",
            )
            pins[key] = req
        elif op == "unpin":
            require(pins.pop(key, None) == req, "stale/unmatched unpin")
            read = reads.get(req)
            require(
                read is not None
                and read["store_worker"] == store
                and read["destination_pool"] == "decode"
                and read["transfer_end_ms"] <= row["time_ms"],
                "relay unpinned before matching read lands",
            )
        elif op in ("retain", "evict"):
            require(op != "evict" or key not in pins, "pinned entry evicted")
            before, after = row["cache_used_before"], row["cache_used_after"]
            require(before == cache.get(store, 0), "receipt chain broken")
            require(
                after == before - row["entry_tokens_before"] + row["entry_tokens_after"],
                "receipt token conservation broken",
            )
            require(0 <= after <= capacities[store], "resident store capacity exceeded")
            cache[store] = after
        elif op == "reserve":
            require(
                row["cache_used_after"] + row["reserved_tokens"] <= capacities[store],
                "store capacity exceeded",
            )
            reserves[(store, req)] = row["sequence"]
        elif op == "transfer":
            require(row["queue_ms"] >= 0, "negative network queue time")
            require(
                row["transfer_end_ms"] >= row["time_ms"] + row["queue_ms"],
                "transfer ends before start",
            )
            if row["reason"] == "kv_store_push":
                require(
                    reserves.get((store, req), row["sequence"]) < row["sequence"],
                    "push submitted before reservation",
                )
                pushes[(store, req)] = row
            else:
                reads[req] = row
                if row["destination_pool"] == "decode":
                    # GpuCluster transfer rows identify request and endpoint;
                    # they do not carry a session key. Match the pin's owner.
                    require(
                        any(k[0] == store and owner == req for k, owner in pins.items()),
                        "relay read before matching pin",
                    )
                    record = by_req[req]
                    for time, stage in zip(record["stage_times_ms"], record["stage_codes"]):
                        if stage in (4, 5):
                            # Stage times are Float32; transfer times are Float64.
                            require(
                                time + max(1e-4, abs(time) * 1e-7) >= row["transfer_end_ms"],
                                "decode admitted before read lands",
                            )
            transfers.append(row)
        elif op == "prefix-reuse":
            local, remote = row["local_hit_tokens"], row["remote_hit_tokens"]
            require(
                local + remote == by_req[req]["prefix_cache_hit_tokens"],
                "prefix attribution mismatch",
            )
            if remote:
                require(
                    req in reads and reads[req]["transfer_end_ms"] <= row["time_ms"],
                    "remote prefix used before landing",
                )
        elif op == "request-complete":
            require(req not in completed, "duplicate completion")
            require(row["time_ms"] == by_req[req]["logging_time"], "foreground completion delayed")
            completed.add(req)
    require(not pins, "relay pins remain after drain")
    require(completed == set(by_req), "missing foreground completions")
    network = [r for r in network if r["kind"] in ("kv_store_push", "kv_store_read")]
    require(len(network) == len(transfers), "transfer log count mismatch")
    for transfer, wire in zip(transfers, network):
        require(transfer["bytes"] == wire["bytes"], "network bytes mismatch")
        require(
            abs(wire["net_start_ms"] - transfer["time_ms"] - transfer["queue_ms"]) < 1e-7,
            "network queue mismatch",
        )
        require(
            wire["net_end_ms"] == transfer["transfer_end_ms"] == wire["send_end_ms"],
            "endpoint timing mismatch",
        )
        require(
            wire["src_pool_tag"] == transfer["source_pool"]
            and wire["dst_pool_tag"] == transfer["destination_pool"],
            "endpoint pool mismatch",
        )
        require(wire["send_count"] > 0 and wire["recv_count"] > 0, "empty network endpoint")
        store_endpoint = "dst_worker_id" if wire["kind"] == "kv_store_push" else "src_worker_id"
        require(wire[store_endpoint] == transfer["store_worker"], "wrong store endpoint")
        require(wire["tag"] == str(transfer["request_id"]), "wrong transfer request")
    return {
        "requests": len(requests),
        "events": dict(Counter(r["operation"] for r in rows)),
        "transfers": len(transfers),
    }


def audit_run(run: Path) -> dict:
    require((run / ".complete").is_file(), f"incomplete run: {run}")
    requests = pq.ParquetFile(run / "raw/request_slo.parquet").read().to_pylist()
    require(all(r["completed"] for r in requests), "unfinished request in completed run")
    summary = read_json(run / "summary.json")
    require(
        summary["sim_ms"] == max(r["logging_time"] for r in requests), "foreground horizon changed"
    )
    meta = read_json(run / "raw/run_meta.json")
    snapshot_paths = list((run / "raw/kv_snapshot").glob("*.parquet"))
    expected_snapshots = {
        f"worker_{worker['pool_tag']}_{worker['worker_id']}.parquet"
        for worker in meta["workers"]
        if worker["pool_tag"] != "kv_store" and worker.get("kv_pools")
    }
    require(bool(expected_snapshots), "no compute KV workers in metadata")
    require(
        expected_snapshots <= {path.name for path in snapshot_paths},
        "missing compute KV snapshot stream",
    )
    for path in snapshot_paths:
        table = pq.ParquetFile(path).read()
        require(
            {"held_kv", "non_cache_kv"} <= set(table.column_names), "missing held/non-cache metrics"
        )
        for row in table.to_pylist():
            require(
                row["held_kv"] <= row["non_cache_kv"] <= row["active_kv"],
                "invalid independent KV maxima",
            )
    event_file = run / "raw/kv_store_event.parquet"
    capacities = {
        w["worker_id"]: sum(p["capacity_units"] if "capacity_units" in p else p["capacity_tokens"]
                            for p in w["kv_pools"])
        for w in meta["workers"]
        if w["pool_tag"] == "kv_store"
    }
    if not capacities:
        require(not event_file.exists(), "store events without store workers in metadata")
        return {"run": str(run), "requests": len(requests), "remote": False}
    require(event_file.is_file(), "missing remote store event stream")
    result = audit_protocol(
        pq.ParquetFile(event_file).read().to_pylist(),
        requests,
        pq.ParquetFile(run / "raw/gpu_cluster.parquet").read().to_pylist(),
        capacities,
    )
    return {"run": str(run), "remote": True, **result}


def canonical_meta(meta: dict) -> dict:
    meta = copy.deepcopy(meta)
    meta.pop("schema_version", None)
    meta.pop("devices", None)
    for gpu in meta.get("gpus", []):
        gpu.pop("device_kind", None)
    for group in meta.get("comm_groups", []):
        group.pop("device_ids", None)
    return meta


def compare_store_off(base: Path, feature: Path) -> dict:
    """Exact pre-existing modeled values; explicit additive schema exceptions."""
    summaries = [read_json(root / "summary.json") for root in (base, feature)]
    for summary in summaries:
        summary.pop("wall_s", None)
        summary.pop("realtime_x", None)
    require(summaries[0] == summaries[1], "modeled summary changed")
    require(
        canonical_meta(read_json(base / "raw/run_meta.json"))
        == canonical_meta(read_json(feature / "raw/run_meta.json")),
        "modeled device metadata changed",
    )
    files = [
        {p.relative_to(root / "raw") for p in (root / "raw").rglob("*.parquet")}
        for root in (base, feature)
    ]
    require(files[0] == files[1], "store-off stream set changed")
    for path in sorted(files[0]):
        old, new = [pq.ParquetFile(root / "raw" / path).read() for root in (base, feature)]
        extra = set(new.column_names) - set(old.column_names)
        allowed = {"held_kv", "non_cache_kv"} if path.parts[0] == "kv_snapshot" else set()
        require(extra <= allowed, f"unexpected added columns: {path}: {extra}")
        require(set(old.column_names) <= set(new.column_names), f"removed columns: {path}")
        new = new.select(old.column_names)
        if path.name == "request_slo.parquet":
            old, new = old.sort_by("request_id"), new.sort_by("request_id")
        require(old.equals(new), f"modeled stream changed: {path}")
    return {
        "base": str(base),
        "feature": str(feature),
        "exact_streams": len(files[0]),
        "exceptions": [
            "wall_s/realtime_x",
            "additive device metadata",
            "held/non_cache columns",
            "simultaneous request_slo row order",
        ],
    }


def compare_pair(direct: Path, remote: Path) -> dict:
    configs = [read_json(path / "raw/params.json") for path in (direct, remote)]
    require(configs[0]["deployment"] == configs[1]["deployment"], "paired deployments differ")
    require(configs[0]["workload"] == configs[1]["workload"], "paired workloads differ")
    pools = [copy.deepcopy(config["pools"]) for config in configs]
    for pool_config in pools:
        for pool in pool_config.values():
            for group in pool["groups"]:
                kind = group["worker"].pop("type")
                # Direct PD's selector omits this knob; deployment/pd.rs uses
                # WorkerConfig's SessionStart default in that case.
                if kind == "pd_prefill":
                    group["worker"].setdefault("pending_order", "session-start")
                group["worker"].pop("max_inflight_remote_reads", None)
    require(pools[0] == pools[1], "paired model/placement/worker settings differ")
    values = []
    for run in (direct, remote):
        slo = read_json(run / "reports/slo_general_report.json")
        throughput = read_json(run / "reports/throughput_report.json")
        kv = read_json(run / "reports/kv_occupancy_report.json")
        conservation = read_json(run / "reports/workload_conservation_report.json")
        require(
            slo["available"] and throughput["available"] and kv["available"],
            "Analyzer report unavailable",
        )
        metrics = {
            f"{name}_mean_ms": slo["metrics"][name]["mean"] for name in ("ttft", "tpot", "e2e")
        }
        metrics.update(
            {name: throughput["totals"][name] for name in ("prefill_tokens", "total_tps")}
        )
        require(
            conservation["available"] and conservation["all_ok"], "workload conservation failed"
        )
        checks = {check["name"]: check for check in conservation["checks"]}
        # Analyzer's actual prefix balance is sum(hit + computed), and the
        # expected prefill count is sum(request_slo.prefill_processed).
        metrics["prefix_hit_tokens"] = (
            checks["prefix_token_balance"]["actual"] - checks["prefill_tokens"]["expected"]
        )
        pool_tag = "prefill" if configs[0]["deployment"] == "pd" else "main"
        series = [s for s in kv["totals"]["per_series"] if s["pool_tag"] == pool_tag]
        for name in ("peak_held_max_tokens", "peak_non_cache_max_tokens"):
            require(
                series and all(s.get(name) is not None for s in series),
                "Analyzer held metrics unavailable",
            )
            metrics[name] = max(s[name] for s in series)
        values.append(metrics)
    return {
        "direct": str(direct),
        "remote": str(remote),
        "analyzer_values": values,
        "delta_remote_minus_direct": {k: values[1][k] - values[0][k] for k in values[0]},
        "note": "Per-shard peaks; no assumed TTFT or total-cache benefit from relay.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("audit", "equivalence", "pair"):
        child = sub.add_parser(command)
        child.add_argument("paths", type=Path, nargs="+" if command == "audit" else 2)
        child.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "audit":
        runs = []
        for path in args.paths:
            runs.extend(
                [path]
                if (path / ".complete").exists()
                else sorted(p.parent for p in path.rglob(".complete"))
            )
        require(bool(runs), "no completed runs found")
        result = []
        for path in runs:
            try:
                result.append(audit_run(path))
            except ValueError as error:
                raise ValueError(f"{path}: {error}") from error
    elif args.command == "equivalence":
        result = compare_store_off(*args.paths)
    else:
        result = compare_pair(*args.paths)
    content = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(content)
    else:
        print(content, end="")


if __name__ == "__main__":
    main()
