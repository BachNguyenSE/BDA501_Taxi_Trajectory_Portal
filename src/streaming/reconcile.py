"""Đối soát streaming với manifest của producer (đề mục 14). Ghi evidence/streaming/summary.md và .json.

Nguồn số liệu:
  - manifest      : evidence/streaming/producer_manifest.json (producer ghi số THẬT đã gửi)
  - Spark progress: evidence/streaming/spark_stream_metrics.json (stateOperators của từng micro-batch)
  - MongoDB       : stream_quality, zone_speed_live (kết quả cuối cùng)
  - Kafka         : evidence/streaming/kafka_offsets.txt (offset cuối của từng partition)
"""
import json

from pymongo import MongoClient  # noqa: E402

from core.common import ROOT, load_config  # noqa: E402


def main():
    cfg = load_config()
    ev = ROOT / "evidence" / "streaming"
    man = json.loads((ROOT / cfg["streaming"]["replay"]["manifest"]).read_text(encoding="utf-8"))
    met = json.loads((ev / "spark_stream_metrics.json").read_text(encoding="utf-8"))
    db = MongoClient(cfg["mongo"]["uri"])[cfg["mongo"]["database"]]
    offsets = (ev / "kafka_offsets.txt").read_text().split()
    part = {p.split(":")[1]: int(p.split(":")[2]) for p in offsets if p.count(":") == 2}

    q = list(db.stream_quality.aggregate([{"$match": {"type": "quality"}}, {"$group": {"_id": None, "n": {"$sum": "$n_messages"},
                                                       "bad": {"$sum": "$n_corrupt_json"}, "b": {"$sum": 1}}}]))[0]
    live = list(db.zone_speed_live.aggregate([{"$group": {"_id": None, "docs": {"$sum": 1}, "seg": {"$sum": "$n_segments"},
                                                          "w0": {"$min": "$window_start"}, "w1": {"$max": "$window_end"}}}]))[0]
    n_windows_per_event = 3   # cửa sổ 15 phút trượt 5 phút: mỗi đoạn thuộc đúng 3 cửa sổ
    ops = met["live"]["stateOperators"]
    dedup = next((v for k, v in ops.items() if "dedup" in k.lower()), {})
    dropped_dup = dedup.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
    dropped_late = sum(v["numRowsDroppedByWatermark"] for v in ops.values())
    agg_segments = live["seg"] / n_windows_per_event
    exp = man["expected"]
    late_slipped = man["late_sent"] - dropped_late
    rows = [
        ("Message gửi vào Kafka", man["messages_sent_total"], sum(part.values()), "offset cuối Kafka (3 partition)"),
        ("Message Spark đọc (query quality)", man["messages_sent_total"], q["n"], "stream_quality"),
        ("Message Spark đọc (query live)", man["messages_sent_total"], met["live"]["numInputRows"], "progress"),
        ("JSON hỏng bắt được", man["corrupt_json_sent"], q["bad"], "stream_quality"),
        ("Bản trùng bị bỏ", man["duplicate_copies_sent"], dropped_dup, "numDroppedDuplicateRows"),
        ("Bản trễ bị bỏ (watermark)", man["late_sent"], dropped_late, "numRowsDroppedByWatermark"),
        ("Đoạn đã tổng hợp", exp["segments_aggregated_if_all_late_dropped"], agg_segments,
         "tổng n_segments / 3 trong zone_speed_live"),
    ]
    result = {
        "kafka_partitions_end_offsets": part,
        "table": [{"item": a, "producer_manifest": b, "spark_or_mongo": c, "source": d} for a, b, c, d in rows],
        "late_not_dropped": late_slipped,
        "late_not_dropped_that_pass_filter_upper_bound": min(late_slipped, exp["late_messages_that_would_pass_filter"]),
        "aggregated_minus_expected": agg_segments - exp["segments_aggregated_if_all_late_dropped"],
        "zone_speed_live": {"docs": live["docs"], "window_start_min": str(live["w0"]), "window_end_max": str(live["w1"])},
        "quality_batches": q["b"],
        "spark": {k: met[k] for k in ["ran_seconds", "config"]},
        "live_batches": met["live"]["batches"],
        "manifest_extract": {k: man[k] for k in ["window_local", "speedup", "fate_counts", "input", "replay_wall_seconds", "send_errors"]},
    }
    (ev / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    f = lambda x: f"{x:,.0f}" if isinstance(x, (int, float)) and float(x).is_integer() else f"{x:,.2f}"  # noqa: E731
    shared = man["input"]["trip_ids_shared_by_several_rows"]
    md = f"""# Streaming: đối soát với manifest của producer (số đo thật)

Chạy bằng `scripts/run_streaming.sh`. Khung phát lại: {man['window_local']['start']} – {man['window_local']['end']} (giờ Lisbon), tốc độ ×{man['speedup']:g}, phát lại mất {man['replay_wall_seconds']} giây thật. Đây là **bản phát lại dữ liệu lịch sử, không phải real-time**.

Kafka 3.9 KRaft một node, topic `{man['topic']}` 3 partition, key = TAXI_ID, `acks={man['acks']}`. Message theo partition: {', '.join(f'p{k}: {v:,}' for k, v in sorted(part.items()))}. Lỗi gửi: {man['send_errors']}.

Spark Structured Streaming: trigger {met['config']['trigger']}, watermark {met['config']['watermark']}, `dropDuplicatesWithinWatermark{tuple(met['config']['dedup_keys'])}`, cửa sổ {met['config']['window']} trượt {met['config']['slide']}, `maxOffsetsPerTrigger={met['config']['max_offsets_per_trigger']}`, có checkpoint. Query live ghi ở chế độ append (chỉ cửa sổ đã đóng) bằng `foreachBatch` upsert `_id = zone|window_start`.

## Bảng đối soát

| Hạng mục | Producer (manifest) | Spark / Mongo | Nguồn số phía Spark |
|---|---|---|---|
""" + "\n".join(f"| {a} | {f(b)} | {f(c)} | {d} |" for a, b, c, d in rows) + f"""

Ghi chú:
- Producer gửi {man['original_messages']:,} message gốc. Số phận mỗi message: {', '.join(f'{k} {v:,}' for k, v in man['fate_counts'].items())}. Thêm {man['duplicate_copies_sent']:,} bản sao và 1 message kết thúc (event_time sau khung 1 giờ, tọa độ ngoài bbox, dùng để đẩy watermark cho Spark xuất các cửa sổ cuối).
- "Đoạn đã tổng hợp" phía producer là số đoạn đúng giờ, JSON hợp lệ, qua bộ lọc bbox và tốc độ (bản sao chỉ tính một lần), **giả sử mọi bản trễ đều bị bỏ**. Mỗi đoạn thuộc đúng 3 cửa sổ trượt nên phía Mongo chia tổng cho 3.
- Bản trễ **không** bị bỏ: {late_slipped}. Chênh lệch đoạn đã tổng hợp so với kỳ vọng: {f(result['aggregated_minus_expected'])}. Nếu có bản trễ lọt qua thì đó là hành vi đã biết của Spark: watermark dùng để lọc là watermark của batch **trước**, và Spark chỉ cam kết không bỏ dòng nằm trong watermark, không cam kết bỏ mọi dòng trễ.
- `TRIP_ID` dùng chung giữa nhiều dòng trong khung phát lại: {len(shared)} {'(không ảnh hưởng tới khóa bỏ trùng `(trip_id, seq)`)' if not shared else '— cần xem xét vì khóa bỏ trùng dùng trip_id: ' + ', '.join(shared)}.
- `zone_speed_live`: {live['docs']:,} tài liệu (zone × cửa sổ), cửa sổ từ {live['w0']} đến {live['w1']} (giờ Lisbon). Streaming chỉ ghi collection riêng, không ghi đè bảng batch.
"""
    (ev / "summary.md").write_text(md, encoding="utf-8")
    print(md)


if __name__ == "__main__":
    main()
