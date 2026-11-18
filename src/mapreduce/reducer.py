#!/usr/bin/env python3
"""Reducer và combiner (cùng một script): CỘNG DỒN total_km, total_s, n_segments theo khóa.

Chỉ cộng, không chia: phép cộng có tính kết hợp và giao hoán nên chạy combiner bao nhiêu lần cũng
cho cùng kết quả. Tốc độ = total_km / total_s chỉ tính ở bước đối chiếu, không lấy trung bình của
trung bình. Đầu vào và đầu ra cùng định dạng:  khóa<TAB>km<TAB>giây<TAB>số_đoạn
"""
import sys


def main():
    cur, km, s, n = None, 0.0, 0, 0
    w = sys.stdout.write
    for line in sys.stdin:
        k, v_km, v_s, v_n = line.rstrip("\n").split("\t")
        if k != cur:
            if cur is not None:
                w(f"{cur}\t{km!r}\t{s}\t{n}\n")
            cur, km, s, n = k, 0.0, 0, 0
        km += float(v_km)
        s += int(v_s)
        n += int(v_n)
    if cur is not None:
        w(f"{cur}\t{km!r}\t{s}\t{n}\n")


if __name__ == "__main__":
    main()
