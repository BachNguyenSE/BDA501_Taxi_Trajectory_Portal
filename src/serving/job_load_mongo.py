"""Job: NẠP bảng gold vào MongoDB, tạo index, chứng minh IXSCAN và đo độ trễ đọc một điểm.

Mục trong sơ đồ : 9 (MongoDB), mũi tên "gold" từ 5.
Làm gì          : upsert 3 collection batch (zones, zone_hour_speed, zone_graph_edges) với _id cố định theo khóa
                  tự nhiên (chạy lại nhiều lần vẫn cùng kết quả); tạo index theo cách giao diện truy vấn;
                  explain() có index so với buộc quét toàn bộ; đo độ trễ p50/p95.
                  Tạo sẵn index cho zone_speed_live (collection này do job streaming ghi).
Đọc             : data/gold/zones, zone_hour_speed, zone_graph_edges
Ghi             : MongoDB porto_eta.*, evidence/mongo/G_nosql.json
Chạy            : tự gọi trong scripts/run_demo.sh  (bên trong: python src/serving/job_load_mongo.py)
"""
import json
import random
import statistics
import time

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne

from core.common import ROOT, load_config


def docs(df, key):
    out = []
    for r in df.to_dict("records"):
        r = {k: (v.item() if hasattr(v, "item") else v) for k, v in r.items()}
        r["_id"] = key(r)
        out.append(r)
    return out


def upsert_all(coll, rows, batch=5000):
    for i in range(0, len(rows), batch):
        coll.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in rows[i:i + batch]], ordered=False)


def winning_stage(plan):
    """Lấy tên các stage trong winningPlan (IXSCAN, FETCH, COLLSCAN...)."""
    stages, node = [], plan["queryPlanner"]["winningPlan"]
    node = node.get("queryPlan", node)  # Mongo 8 có thể bọc trong queryPlan
    while node:
        stages.append(node.get("stage"))
        node = node.get("inputStage")
    return stages


def main():
    cfg = load_config()
    gold = ROOT / cfg["paths"]["gold"]
    mc = cfg["mongo"]
    db = MongoClient(mc["uri"], serverSelectionTimeoutMS=10000)[mc["database"]]
    t0 = time.time()

    zones = pd.read_parquet(gold / "zones")
    zhs = pd.read_parquet(gold / "zone_hour_speed")
    edges = pd.read_parquet(gold / "zone_graph_edges")
    upsert_all(db.zones, docs(zones, lambda r: int(r["zone_id"])))
    upsert_all(db.zone_hour_speed, docs(zhs, lambda r: f"{r['zone_id']}|{r['hour']}|{int(r['is_weekend'])}"))
    upsert_all(db.zone_graph_edges, docs(edges, lambda r: f"{r['zone_from']}|{r['zone_to']}"))
    load_s = round(time.time() - t0, 2)

    # index theo access pattern của giao diện
    db.zone_hour_speed.create_index([("zone_id", ASCENDING), ("hour", ASCENDING), ("is_weekend", ASCENDING)],
                                    name="zone_hour_weekend", unique=True)
    db.zone_graph_edges.create_index([("zone_from", ASCENDING)], name="zone_from")
    db.zones.create_index([("gx", ASCENDING), ("gy", ASCENDING)], name="gx_gy", unique=True)
    db.zone_speed_live.create_index([("window_end", DESCENDING), ("zone_id", ASCENDING)], name="window_end_zone")

    counts = {c: db[c].count_documents({}) for c in ["zones", "zone_hour_speed", "zone_graph_edges"]}
    expected = {"zones": len(zones), "zone_hour_speed": len(zhs), "zone_graph_edges": len(edges)}

    # explain: truy vấn một điểm (zone, giờ, loại ngày)
    probe = {"zone_id": int(zhs.iloc[0]["zone_id"]), "hour": int(zhs.iloc[0]["hour"]), "is_weekend": bool(zhs.iloc[0]["is_weekend"])}
    ex_idx = db.command("explain", {"find": "zone_hour_speed", "filter": probe}, verbosity="executionStats")
    ex_scan = db.command("explain", {"find": "zone_hour_speed", "filter": probe, "hint": {"$natural": 1}},
                         verbosity="executionStats")

    def lat(n, hint=None):
        keys = zhs[["zone_id", "hour", "is_weekend"]].sample(n=n, random_state=cfg["project"]["seed"]).to_dict("records")
        out = []
        for k in keys:
            q = {"zone_id": int(k["zone_id"]), "hour": int(k["hour"]), "is_weekend": bool(k["is_weekend"])}
            t = time.perf_counter()
            cur = db.zone_hour_speed.find(q)
            if hint:
                cur = cur.hint(hint)
            assert len(list(cur)) == 1
            out.append((time.perf_counter() - t) * 1000)
        out.sort()
        return {"n": n, "p50_ms": round(statistics.median(out), 3), "p95_ms": round(out[int(0.95 * n) - 1], 3),
                "max_ms": round(out[-1], 3)}

    lat(50)  # khởi động kết nối và cache, không tính
    random.seed(cfg["project"]["seed"])
    res = {
        "load_seconds": load_s,
        "counts_in_mongo": counts,
        "counts_in_gold": expected,
        "counts_match": counts == expected,
        "indexes": {c: list(db[c].index_information().keys())
                    for c in ["zones", "zone_hour_speed", "zone_graph_edges", "zone_speed_live"]},
        "explain_point_query": {
            "filter": probe,
            "with_index": {"stages": winning_stage(ex_idx),
                           "totalDocsExamined": ex_idx["executionStats"]["totalDocsExamined"],
                           "totalKeysExamined": ex_idx["executionStats"]["totalKeysExamined"]},
            "forced_collection_scan": {"stages": winning_stage(ex_scan),
                                       "totalDocsExamined": ex_scan["executionStats"]["totalDocsExamined"]},
        },
        "latency_point_read": {"with_index": lat(1000), "forced_collection_scan": lat(200, hint=[("$natural", 1)])},
        "note": "Độ trễ đo từ container spark tới container mongo trên cùng máy, gồm cả vòng mạng Docker.",
    }
    ev = ROOT / "evidence" / "mongo"
    ev.mkdir(parents=True, exist_ok=True)
    (ev / "G_nosql.json").write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
