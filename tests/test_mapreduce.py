"""Hợp đồng của mapper / combiner / reducer (đề mục 5, "Unit test: hợp đồng của mapper/combiner/reducer").

1. Mapper + reducer cho cùng kết quả với Spark (bronze -> silver -> group by zone, giờ) trên cùng dữ liệu.
2. Combiner chạy trên các phần tùy ý rồi reduce vẫn ra đúng kết quả (chỉ cộng dồn, có tính kết hợp).
3. Counter reject_reason khớp với bronze.
"""
import csv
import io
import json
import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MR = ROOT / "src" / "mapreduce"

from batch.bronze import build_bronze  # noqa: E402
from core.common import get_spark, load_config  # noqa: E402
from mapreduce.export_mr_conf import mr_conf  # noqa: E402
from batch.silver import build_segments, build_trips  # noqa: E402

CFG = load_config()
HEADER = ["TRIP_ID", "CALL_TYPE", "ORIGIN_CALL", "ORIGIN_STAND", "TAXI_ID", "TIMESTAMP", "DAY_TYPE", "MISSING_DATA", "POLYLINE"]


def P(n, lon=-8.61, lat=41.15, dlat=0.001):
    return json.dumps([[lon, round(lat + i * dlat, 6)] for i in range(n)])


# (trip_id, epoch, missing, polyline)
ROWS = [
    ("v1", 1372636858, "False", P(30)),                                   # hợp lệ, ~26.7 km/h
    ("v2", 1372636858 + 3540, "False", P(12, lon=-8.6014, dlat=0.0)),     # đứng yên, cắt qua mốc giờ 1->2 giờ Lisbon
    ("v3", 1398898790, "False", json.dumps([[-8.6014, 41.1525], [-8.5966, 41.1525], [-8.5960, 41.1530]])),  # qua zone kề
    ("v4", 1372636858, "False", json.dumps([[-8.61, 41.15], [-9.0, 41.15], [-8.611, 41.15], [-8.611, 41.151]])),  # điểm ngoài bbox
    ("v5", 1372636858, "False", json.dumps([[-8.61, 41.15], [-8.61, 41.16], [-8.61, 41.161]])),  # một bước quá tốc độ
    ("m1", 1372636858, "True", P(10)),                                    # missing_data
    ("b1", 1372636858, "False", "[[1,"),                                  # bad_json
    ("f1", 1372636858, "False", "[]"),                                    # too_few_points
    ("f2", 1372636858, "False", P(1)),                                    # too_few_points
    ("l1", 1372636858, "False", P(722, dlat=0.00001)),                    # too_long
]


def csv_text(rows):
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    w.writerow(HEADER)
    for t, e, m, p in rows:
        w.writerow([t, "A", "", "", "1", str(e), "A", m, p])
    return buf.getvalue()


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    d = tmp_path_factory.mktemp("mr")
    conf = d / "mr_conf.json"
    conf.write_text(json.dumps(mr_conf(CFG)))
    return {**os.environ, "MR_CONF": str(conf)}


def run(script, text, env):
    r = subprocess.run([sys.executable, str(MR / script)], input=text, capture_output=True, text=True, env=env, check=True)
    return r.stdout, r.stderr


def map_reduce(text, env, combine_chunks=None):
    mapped, err = run("mapper.py", text, env)
    lines = sorted(mapped.splitlines(keepends=True), key=lambda x: x.split("\t")[0])  # Hadoop sort theo khóa
    if combine_chunks:
        k = max(1, len(lines) // combine_chunks)
        parts = [lines[i:i + k] for i in range(0, len(lines), k)]
        combined = "".join(run("reducer.py", "".join(p), env)[0] for p in parts)
        lines = sorted(combined.splitlines(keepends=True), key=lambda x: x.split("\t")[0])
    reduced, _ = run("reducer.py", "".join(lines), env)
    out = {}
    for line in reduced.splitlines():
        key, km, s, n = line.split("\t")
        z, h = key.split(",")
        out[(int(z), int(h))] = (float(km), int(s), int(n))
    counters = {}
    for line in err.splitlines():
        if line.startswith("reporter:counter:Porto,"):
            name, v = line.split(",")[1:3]
            counters[name] = int(v)
    return out, counters


@pytest.fixture(scope="module")
def spark():
    s = get_spark(CFG, "test-mapreduce")
    yield s
    s.stop()


def spark_result(spark):
    raw = spark.createDataFrame([[t, "A", "", "", "1", str(e), "A", m, p] for t, e, m, p in ROWS], HEADER)
    bronze = build_bronze(raw, CFG)
    seg = build_segments(build_trips(bronze, CFG), CFG)
    seg.createOrReplaceTempView("seg_test")
    q = spark.sql("SELECT zone_id, hour, SUM(km) km, COUNT(*) * 15 s, COUNT(*) n FROM seg_test GROUP BY zone_id, hour")
    reasons = {r["reject_reason"]: r["count"] for r in bronze.groupBy("reject_reason").count().collect()}
    return {(r["zone_id"], r["hour"]): (r["km"], r["s"], r["n"]) for r in q.collect()}, reasons


def test_mapreduce_equals_spark(spark, env):
    mr, counters = map_reduce(csv_text(ROWS), env)
    sp, reasons = spark_result(spark)
    assert set(mr) == set(sp)
    for k in sp:
        assert mr[k][1:] == sp[k][1:], k
        assert abs(mr[k][0] - sp[k][0]) <= sp[k][2] * 1e-6, k
    # đoạn cắt qua mốc giờ (v2) phải rơi vào hai giờ khác nhau
    assert {h for (_, h) in mr} >= {1, 2}
    # counter khớp bronze
    for r in ["missing_data", "bad_json", "too_few_points", "too_long"]:
        assert counters[r] == reasons.get(r, 0), r
    assert counters["valid_trips"] == reasons[None]
    assert counters["header"] == 1 and counters["rows"] == len(ROWS)
    assert counters["pairs_out_bbox"] == 2 and counters["pairs_fast_in_bbox"] == 1


@pytest.mark.parametrize("chunks", [2, 3, 7])
def test_combiner_is_associative(env, chunks):
    direct, _ = map_reduce(csv_text(ROWS), env)
    combined, _ = map_reduce(csv_text(ROWS), env, combine_chunks=chunks)
    assert set(direct) == set(combined)
    for k in direct:
        assert direct[k][1:] == combined[k][1:]
        assert abs(direct[k][0] - combined[k][0]) < 1e-9


def test_reducer_sums_not_averages(env):
    # hai bản ghi cùng khóa: 1 km/15 s và 0 km/15 s -> tổng 1 km, 30 s, 2 đoạn
    out, _ = run("reducer.py", "5,8\t1.0\t15\t1\n5,8\t0.0\t15\t1\n", env)
    assert out == "5,8\t1.0\t30\t2\n"
