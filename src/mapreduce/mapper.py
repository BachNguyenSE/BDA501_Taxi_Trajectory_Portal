#!/usr/bin/env python3
"""Mapper: đọc dòng CSV raw, phát mỗi đoạn hợp lệ thành  "zone_id,hour<TAB>km<TAB>giây<TAB>1".

Khóa gồm một trường "zone_id,hour" để Hadoop sort và chia reducer theo đúng cặp (zone, giờ).
Counter (nhóm Porto): số dòng theo reject_reason, số đoạn giữ lại và bị bỏ.
"""
import os
import sys

sys.path.insert(0, os.getcwd())  # -files tạo symlink ở thư mục làm việc của task, không nằm cạnh mapper.py

import csv  # noqa: E402

from mr_common import classify, get_tz, load_conf, segments  # noqa: E402


def main():
    conf = load_conf()
    tz = get_tz(conf)
    dt = conf["gps_interval_s"]
    c = {k: 0 for k in ["rows", "header", "missing_data", "bad_json", "too_few_points", "too_long",
                        "valid_trips", "pairs_out_bbox", "pairs_fast_in_bbox", "segments_emitted", "bad_csv"]}
    out = sys.stdout
    for row in csv.reader(sys.stdin):
        if row and row[0] == "TRIP_ID":
            c["header"] += 1
            continue
        c["rows"] += 1
        if len(row) != 9:
            c["bad_csv"] += 1
            continue
        reason, pts = classify(row, conf)
        if reason:
            c[reason] += 1
            continue
        c["valid_trips"] += 1
        segs, n_out, n_fast = segments(int(row[5]), pts, conf, tz)
        c["pairs_out_bbox"] += n_out
        c["pairs_fast_in_bbox"] += n_fast
        for z, h, km in segs:
            out.write(f"{z},{h}\t{km!r}\t{dt}\t1\n")
        c["segments_emitted"] += len(segs)
    # counter gửi một lần ở cuối, tránh ghi stderr cho từng dòng
    for k, v in c.items():
        sys.stderr.write(f"reporter:counter:Porto,{k},{v}\n")


if __name__ == "__main__":
    main()
