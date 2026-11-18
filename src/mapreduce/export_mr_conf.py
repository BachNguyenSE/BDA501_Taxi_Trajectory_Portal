"""Sinh mr_conf.json từ conf/pipeline.yaml để mapper dùng chung tham số (không hard-code)."""
import json
import pathlib
import sys

from core.common import load_config  # noqa: E402


def mr_conf(cfg):
    c, g = cfg["cleaning"], cfg["grid"]
    return {
        "bbox": g["bbox"], "cell_deg": g["cell_deg"], "n_cols": g["n_cols"], "n_rows": g["n_rows"],
        "gps_interval_s": c["gps_interval_s"], "min_points": c["min_points"],
        "max_trip_min": c["max_trip_min"], "max_speed_kmh": c["max_speed_kmh"],
        "timezone_local": cfg["project"]["timezone_local"],
        # cho scripts/run_mapreduce.sh (không dùng trong mapper)
        "hdfs_input": cfg["mapreduce"]["hdfs_input"], "hdfs_output": cfg["mapreduce"]["hdfs_output"],
        "reducers": cfg["mapreduce"]["reducers"],
    }


if __name__ == "__main__":
    out = pathlib.Path(sys.argv[1])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(mr_conf(load_config()), indent=2), encoding="utf-8")
    print(f"wrote {out}")
