"""The offline gate must reject corrupted protocol and equivalence evidence."""

import copy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tools.validate_distributed_kv import audit_protocol, audit_run, compare_pair, compare_store_off


def protocol():
    common = dict(request_id=0, store_worker=2, session_id=7)
    rows = [
        dict(common, operation="reserve", cache_used_after=0, reserved_tokens=80),
        dict(
            common,
            operation="transfer",
            reason="kv_store_push",
            time_ms=1.0,
            queue_ms=0.0,
            transfer_end_ms=2.0,
            bytes=80,
            source_pool="prefill",
            destination_pool="kv_store",
        ),
        dict(
            common,
            operation="retain",
            cache_used_before=0,
            cache_used_after=80,
            entry_tokens_before=0,
            entry_tokens_after=80,
        ),
        dict(common, operation="pin", time_ms=2.0),
        dict(
            common,
            operation="transfer",
            reason="kv_store_read",
            time_ms=2.0,
            queue_ms=0.0,
            transfer_end_ms=3.0,
            bytes=80,
            source_pool="kv_store",
            destination_pool="decode",
        ),
        dict(common, operation="unpin", time_ms=3.0),
        dict(common, operation="request-complete", time_ms=4.0),
    ]
    for i, row in enumerate(rows):
        row["sequence"] = i
    requests = [
        dict(
            request_id=0,
            stage_times_ms=[3.0],
            stage_codes=[4],
            prefix_cache_hit_tokens=0,
            logging_time=4.0,
        )
    ]
    network = [
        dict(
            kind=r["reason"],
            bytes=r["bytes"],
            net_start_ms=r["time_ms"],
            net_end_ms=r["transfer_end_ms"],
            send_end_ms=r["transfer_end_ms"],
            src_pool_tag=r["source_pool"],
            dst_pool_tag=r["destination_pool"],
            src_worker_id=2 if r["reason"] == "kv_store_read" else 0,
            dst_worker_id=2 if r["reason"] == "kv_store_push" else 0,
            tag="0",
            send_count=1,
            recv_count=1,
        )
        for r in rows
        if r["operation"] == "transfer"
    ]
    return rows, requests, network, {2: 100}


def test_protocol_audit_accepts_balanced_relay():
    assert audit_protocol(*protocol())["transfers"] == 2


@pytest.mark.parametrize(
    "fault, message",
    [
        ("capacity", "capacity exceeded"),
        ("reservation", "before reservation"),
        ("early_decode", "before read lands"),
        ("early_pin", "before push lands"),
        ("early_unpin", "before matching read lands"),
        ("stale_unpin", "stale/unmatched"),
        ("receipt", "conservation"),
        ("endpoint", "endpoint pool"),
        ("completion", "completion delayed"),
    ],
)
def test_protocol_audit_rejects_corruption(fault, message):
    rows, requests, network, capacities = protocol()
    if fault == "capacity":
        capacities[2] = 79
    elif fault == "reservation":
        rows[0]["operation"] = "placement"
    elif fault == "early_decode":
        requests[0]["stage_times_ms"] = [2.0]
    elif fault == "early_pin":
        rows[3]["time_ms"] = 1.5
    elif fault == "early_unpin":
        rows[5]["time_ms"] = 2.5
    elif fault == "stale_unpin":
        rows[5]["request_id"] = 9
    elif fault == "receipt":
        rows[2]["entry_tokens_after"] = 79
    elif fault == "endpoint":
        network[1]["dst_pool_tag"] = "prefill"
    elif fault == "completion":
        rows[-1]["time_ms"] = 5.0
    with pytest.raises(ValueError, match=message):
        audit_protocol(rows, requests, network, capacities)


@pytest.mark.parametrize(
    "missing, message",
    [
        ("kv_store_event.parquet", "missing remote store event stream"),
        ("kv_snapshot/worker_decode_0.parquet", "missing compute KV snapshot stream"),
    ],
)
def test_completed_marker_cannot_hide_missing_required_streams(tmp_path, missing, message):
    rows, requests, network, capacities = protocol()
    requests[0]["completed"] = True
    raw = tmp_path / "raw"
    (raw / "kv_snapshot").mkdir(parents=True)
    (tmp_path / ".complete").touch()
    (tmp_path / "summary.json").write_text(json.dumps(dict(sim_ms=4.0)))
    workers = [dict(worker_id=0, pool_tag="decode", kv_pools=[dict(capacity_tokens=100)])]
    workers += [
        dict(worker_id=store, pool_tag="kv_store", kv_pools=[dict(capacity_tokens=capacity)])
        for store, capacity in capacities.items()
    ]
    (raw / "run_meta.json").write_text(json.dumps(dict(workers=workers)))
    for name, records in [
        ("request_slo.parquet", requests),
        ("kv_store_event.parquet", rows),
        ("gpu_cluster.parquet", network),
    ]:
        # Explicit union: protocol rows are sparse and Arrow otherwise infers
        # column names from only the first row.
        keys = sorted(set().union(*(row.keys() for row in records)))
        pq.write_table(pa.table({key: [r.get(key) for r in records] for key in keys}), raw / name)
    pq.write_table(
        pa.table(dict(active_kv=[80], held_kv=[0], non_cache_kv=[80])),
        raw / "kv_snapshot/worker_decode_0.parquet",
    )
    assert audit_run(tmp_path)["remote"]
    (raw / missing).unlink()
    with pytest.raises(ValueError, match=message):
        audit_run(tmp_path)


def test_equivalence_allows_only_documented_additions(tmp_path):
    base, feature = [tmp_path / p for p in ("base", "feature")]
    for root in (base, feature):
        (root / "raw/kv_snapshot").mkdir(parents=True)
        (root / "summary.json").write_text(json.dumps(dict(sim_ms=3.0, wall_s=1.0)))
        (root / "raw/run_meta.json").write_text(json.dumps(dict(schema_version=5, workers=[])))
        pq.write_table(pa.table(dict(active_kv=[3, 5])), root / "raw/kv_snapshot/a.parquet")
    (feature / "raw/run_meta.json").write_text(
        json.dumps(dict(schema_version=6, workers=[], devices=[]))
    )
    new = dict(active_kv=[3, 5], held_kv=[1, 2], non_cache_kv=[2, 3])
    pq.write_table(pa.table(new), feature / "raw/kv_snapshot/a.parquet")
    assert compare_store_off(base, feature)["exact_streams"] == 1
    changed = copy.deepcopy(new)
    changed["active_kv"][1] = 6
    pq.write_table(pa.table(changed), feature / "raw/kv_snapshot/a.parquet")
    with pytest.raises(ValueError, match="modeled stream changed"):
        compare_store_off(base, feature)


def write_pair_example(root, remote):
    """Two workers run identical three-turn inputs with measured report values."""
    (root / "raw").mkdir(parents=True)
    (root / "reports").mkdir()
    params = {
        "deployment": "pd",
        "workload": {"trace": "three-turn.csv", "tick_us": 1},
        "pools": {
            "prefill": {
                "groups": [
                    {
                        "arch": {"model": "llama", "tp_size": 1},
                        "replicas": 2,
                        "worker": {"type": "remote_cache_pd_prefill" if remote else "pd_prefill"},
                    }
                ]
            }
        },
    }
    if remote:
        params["pools"]["prefill"]["groups"][0]["worker"].update(
            pending_order="session-start", max_inflight_remote_reads=16
        )
    (root / "raw/params.json").write_text(json.dumps(params))
    reports = {
        "slo_general": {
            "available": True,
            "metrics": {
                "ttft": {"mean": 2.0 if remote else 3.0},
                "tpot": {"mean": 1.5 if remote else 1.0},
                "e2e": {"mean": 8.0 if remote else 9.0},
            },
        },
        "throughput": {
            "available": True,
            "totals": {
                "prefill_tokens": 40 if remote else 120,
                "total_tps": 15.0 if remote else 10.0,
            },
        },
        "kv_occupancy": {
            "available": True,
            "totals": {
                "per_series": [
                    {
                        "pool_tag": "prefill",
                        "peak_held_max_tokens": 20 if remote else 60,
                        "peak_non_cache_max_tokens": 80 if remote else 120,
                    },
                    {
                        "pool_tag": "prefill",
                        "peak_held_max_tokens": 30 if remote else 40,
                        "peak_non_cache_max_tokens": 70 if remote else 100,
                    },
                    {
                        "pool_tag": "decode",
                        "peak_held_max_tokens": 999,
                        "peak_non_cache_max_tokens": 999,
                    },
                ]
            },
        },
        "workload_conservation": {
            "available": True,
            "all_ok": True,
            "checks": [
                {"name": "prefix_token_balance", "actual": 120},
                {"name": "prefill_tokens", "expected": 40 if remote else 120},
            ],
        },
    }
    for name, report in reports.items():
        (root / f"reports/{name}_report.json").write_text(json.dumps(report))
    return params, reports


def test_pair_reports_exact_deltas_with_same_work_and_independent_shard_peaks(tmp_path):
    direct, remote = tmp_path / "direct", tmp_path / "remote"
    write_pair_example(direct, False)
    write_pair_example(remote, True)
    result = compare_pair(direct, remote)
    assert result["delta_remote_minus_direct"] == {
        "ttft_mean_ms": -1.0,
        "tpot_mean_ms": 0.5,
        "e2e_mean_ms": -1.0,
        "prefill_tokens": -80,
        "total_tps": 5.0,
        "prefix_hit_tokens": 80,
        "peak_held_max_tokens": -30,
        "peak_non_cache_max_tokens": -40,
    }
    assert result["analyzer_values"][1]["peak_held_max_tokens"] == 30
    assert result["analyzer_values"][1]["peak_non_cache_max_tokens"] == 80


@pytest.mark.parametrize(
    "fault, message",
    [
        ("workload", "paired workloads differ"),
        ("deployment", "paired deployments differ"),
        ("model", "paired model/placement/worker settings differ"),
        ("unavailable", "Analyzer report unavailable"),
        ("conservation", "workload conservation failed"),
        ("missing_peak", "Analyzer held metrics unavailable"),
    ],
)
def test_pair_refuses_unfair_inputs_and_invalid_reports(tmp_path, fault, message):
    direct, remote = tmp_path / "direct", tmp_path / "remote"
    write_pair_example(direct, False)
    params, reports = write_pair_example(remote, True)
    if fault == "workload":
        params["workload"]["tick_us"] = 100
    elif fault == "deployment":
        params["deployment"] = "unified"
    elif fault == "model":
        params["pools"]["prefill"]["groups"][0]["arch"]["tp_size"] = 2
    elif fault == "unavailable":
        reports["slo_general"]["available"] = False
    elif fault == "conservation":
        reports["workload_conservation"]["all_ok"] = False
    elif fault == "missing_peak":
        reports["kv_occupancy"]["totals"]["per_series"][0].pop("peak_held_max_tokens")
    (remote / "raw/params.json").write_text(json.dumps(params))
    for name, report in reports.items():
        (remote / f"reports/{name}_report.json").write_text(json.dumps(report))
    with pytest.raises(ValueError, match=message):
        compare_pair(direct, remote)


@pytest.mark.parametrize(
    "fault, message",
    [
        ("sequence", "event sequence gap"),
        ("double_pin", "relay pinned twice"),
        ("pinned_eviction", "pinned entry evicted"),
        ("negative_queue", "negative network queue time"),
        ("ends_before_start", "transfer ends before start"),
        ("missing_completion", "missing foreground completions"),
        ("duplicate_completion", "duplicate completion"),
        ("bytes", "network bytes mismatch"),
        ("queue", "network queue mismatch"),
        ("end", "endpoint timing mismatch"),
        ("empty_endpoint", "empty network endpoint"),
        ("wrong_store", "wrong store endpoint"),
        ("wrong_request", "wrong transfer request"),
        ("missing_wire", "transfer log count mismatch"),
    ],
)
def test_relay_audit_rejects_specific_causal_and_wire_errors(fault, message):
    rows, requests, network, capacities = protocol()
    if fault == "sequence":
        rows[2]["sequence"] = 99
    elif fault == "double_pin":
        rows.insert(4, dict(rows[3]))
    elif fault == "pinned_eviction":
        rows.insert(4, dict(rows[2], operation="evict"))
    elif fault == "negative_queue":
        rows[1]["queue_ms"] = -1.0
    elif fault == "ends_before_start":
        rows[1]["transfer_end_ms"] = 0.5
    elif fault == "missing_completion":
        rows.pop()
    elif fault == "duplicate_completion":
        rows.append(dict(rows[-1]))
    elif fault == "bytes":
        network[0]["bytes"] = 79
    elif fault == "queue":
        network[0]["net_start_ms"] = 1.1
    elif fault == "end":
        network[0]["send_end_ms"] = 2.1
    elif fault == "empty_endpoint":
        network[0]["recv_count"] = 0
    elif fault == "wrong_store":
        network[0]["dst_worker_id"] = 1
    elif fault == "wrong_request":
        network[0]["tag"] = "999"
    elif fault == "missing_wire":
        network.pop()
    if fault != "sequence":
        for i, row in enumerate(rows):
            row["sequence"] = i
    with pytest.raises(ValueError, match=message):
        audit_protocol(rows, requests, network, capacities)


def test_shared_remote_prefix_extension_is_credited_only_after_landing():
    rows, requests, network, capacities = protocol()
    rows.insert(
        6,
        dict(
            request_id=0,
            store_worker=2,
            session_id=7,
            operation="prefix-reuse",
            time_ms=3.0,
            local_hit_tokens=20,
            remote_hit_tokens=60,
        ),
    )
    requests[0]["prefix_cache_hit_tokens"] = 80
    for i, row in enumerate(rows):
        row["sequence"] = i
    assert audit_protocol(rows, requests, network, capacities)["events"]["prefix-reuse"] == 1
    rows[6]["time_ms"] = 2.9
    with pytest.raises(ValueError, match="remote prefix used before landing"):
        audit_protocol(rows, requests, network, capacities)
