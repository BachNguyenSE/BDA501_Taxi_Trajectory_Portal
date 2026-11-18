"""Tiện ích dùng chung: đọc conf/pipeline.yaml và tạo SparkSession theo cấu hình."""
import pathlib

import yaml

def _find_root():
    """Thư mục gốc dự án = thư mục cha gần nhất có conf/pipeline.yaml."""
    for parent in pathlib.Path(__file__).resolve().parents:
        if (parent / "conf" / "pipeline.yaml").exists():
            return parent
    raise RuntimeError("không tìm thấy conf/pipeline.yaml")


ROOT = _find_root()


def load_config(path=None):
    path = pathlib.Path(path) if path else ROOT / "conf" / "pipeline.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_spark(cfg, app_name, driver_memory=None):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", driver_memory or cfg["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(cfg["spark"]["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", cfg["project"]["spark_session_timezone"])
        .config("spark.local.dir", "/tmp/spark")
        .getOrCreate()
    )


def first_row(df):
    """df.first() nhưng báo lỗi rõ ràng nếu rỗng (first() có kiểu Row | None)."""
    row = df.first()
    if row is None:
        raise ValueError("DataFrame rỗng, không có dòng nào")
    return row
