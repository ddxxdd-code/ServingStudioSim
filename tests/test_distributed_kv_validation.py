"""The offline gate must reject corrupted protocol and equivalence evidence."""

import copy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tools.validate_distributed_kv import audit_protocol, compare_store_off


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
        dict(common, operation="pin"),
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
        dict(common, operation="unpin"),
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
