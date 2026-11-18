"""Ground truth của producer (đề: "Unit test ... ground truth của producer") và phần parse/lọc của stream."""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
from core.common import get_spark, load_config  # noqa: E402
from streaming.producer import build_plan, passes_filter, replay, window_bounds  # noqa: E402

CFG = load_config()


def fake_segments(n=20000):
    w0, _ = window_bounds(CFG)
    return [{"trip_id": f"t{i // 50}", "taxi_id": 1000 + i % 37, "seq": i % 50 + 1, "event_time": w0 + i,
             "lon": -8.61, "lat": 41.15 + (i % 7) * 1e-4, "prev_lon": -8.61, "prev_lat": 41.15} for i in range(n)]


def test_plan_ground_truth_counts():
    segs = fake_segments()
    plan, man = build_plan(segs, CFG, seed=42)
    kinds = [p[3] for p in plan]
    f = man["fate_counts"]
    assert sum(f.values()) == man["original_messages"] == len(segs)
    assert kinds.count("corrupt") == man["corrupt_json_sent"] == f.get("corrupt", 0)
    assert kinds.count("late") == man["late_sent"]
    assert kinds.count("duplicate_copy") == kinds.count("duplicate_original") == man["duplicate_copies_sent"]
    assert kinds.count("sentinel") == 1 and man["messages_sent_total"] == len(plan) == len(segs) + man["duplicate_copies_sent"] + 1
    r = CFG["streaming"]["replay"]["bad_data"]   # tỉ lệ thực tế gần tỉ lệ cấu hình
    assert abs(f["duplicate"] / len(segs) - r["duplicate_rate"]) < 0.003
    assert abs(f["late"] / len(segs) - r["late_rate"]) < 0.002
    assert abs(f["corrupt"] / len(segs) - r["corrupt_json_rate"]) < 0.0015


def test_plan_message_properties():
    plan, man = build_plan(fake_segments(5000), CFG, seed=7)
    late_s = CFG["streaming"]["replay"]["bad_data"]["late_delay_min"] * 60
    clocks = [p[0] for p in plan]
    assert clocks == sorted(clocks)                                      # gửi theo thứ tự đồng hồ sự kiện
    for i, (clock, key, val, kind) in enumerate(plan):
        if kind == "corrupt":
            with pytest.raises(ValueError):
                json.loads(val)
        elif kind == "late":
            assert clock - json.loads(val)["event_time"] == late_s       # trễ đúng 30 phút sự kiện
        elif kind == "duplicate_copy":
            assert plan[i - 1][2] == val and plan[i - 1][3] == "duplicate_original"
        elif kind == "sentinel":
            m = json.loads(val)
            assert not passes_filter(m, CFG) and m["event_time"] > max(c for c, *_ in plan[:-1])
        if kind not in ("sentinel",):
            assert key == str(json.loads(val)["taxi_id"]).encode() if kind != "corrupt" else True


def test_replay_speedup_and_order():
    plan = [(100, b"1", b"a", "normal"), (160, b"1", b"b", "normal"), (220, b"2", b"c", "normal")]
    t = {"now": 0.0}
    sent, slept = [], []
    replay(plan, lambda k, v: sent.append(v), speedup=60,
           sleep=lambda s: (slept.append(s), t.__setitem__("now", t["now"] + s)), now=lambda: t["now"])
    assert sent == [b"a", b"b", b"c"] and [round(s, 6) for s in slept] == [1.0, 1.0]   # 60 s dữ liệu = 1 s thật


@pytest.fixture(scope="module")
def spark():
    s = get_spark(CFG, "test-stream")
    yield s
    s.stop()


def test_parse_and_filter(spark):
    from streaming.stream_job import clean_segments, parse
    good = {"trip_id": "t", "taxi_id": 1, "seq": 1, "event_time": 1400000000,
            "lon": -8.61, "lat": 41.151, "prev_lon": -8.61, "prev_lat": 41.15}
    fast = {**good, "seq": 2, "lat": 41.17}                       # ~2,2 km / 15 s: quá tốc độ
    outside = {**good, "seq": 3, "prev_lon": -9.0}                # đầu trước ngoài bbox
    rows = [(json.dumps(good),), (json.dumps(good)[:30],), ("not json",), (json.dumps({"trip_id": "x"}),),
            (json.dumps(fast),), (json.dumps(outside),)]
    p = parse(spark.createDataFrame(rows, ["json"]))
    assert [r["is_valid"] for r in p.collect()] == [True, False, False, False, True, True]
    out = clean_segments(p.filter("is_valid").select("m.*"), CFG).collect()
    assert [r["seq"] for r in out] == [1]
