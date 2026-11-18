"""Hàm hình học dùng chung (biểu thức Spark, không UDF Python)."""
from pyspark.sql import functions as F

EARTH_R_KM = 6371.0


def haversine_km(lon1, lat1, lon2, lat2):
    """Khoảng cách km giữa hai điểm (các tham số là Column)."""
    a = (
        F.pow(F.sin((F.radians(lat2) - F.radians(lat1)) / 2), 2)
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
        * F.pow(F.sin((F.radians(lon2) - F.radians(lon1)) / 2), 2)
    )
    return 2 * EARTH_R_KM * F.asin(F.sqrt(a))


def haversine_sql(a, b):
    """Cùng công thức, dạng chuỗi SQL cho hai điểm [lon, lat] (dùng trong transform)."""
    return (
        f"2*{EARTH_R_KM}*asin(sqrt(pow(sin(radians({b}[1]-{a}[1])/2),2)"
        f"+cos(radians({a}[1]))*cos(radians({b}[1]))*pow(sin(radians({b}[0]-{a}[0])/2),2)))"
    )


def bearing_deg(lon1, lat1, lon2, lat2):
    """Hướng từ điểm 1 đến điểm 2, độ, 0 = Bắc, theo chiều kim đồng hồ."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dl = F.radians(lon2 - lon1)
    y = F.sin(dl) * F.cos(p2)
    x = F.cos(p1) * F.sin(p2) - F.sin(p1) * F.cos(p2) * F.cos(dl)
    return F.pmod(F.degrees(F.atan2(y, x)) + 360, F.lit(360.0))


def in_bbox(lon, lat, b):
    return (lon >= b["lon_min"]) & (lon <= b["lon_max"]) & (lat >= b["lat_min"]) & (lat <= b["lat_max"])


def in_bbox_sql(p, b):
    return (f"({p}[0] >= {b['lon_min']} and {p}[0] <= {b['lon_max']} "
            f"and {p}[1] >= {b['lat_min']} and {p}[1] <= {b['lat_max']})")


def zone_id(lon, lat, grid):
    """zone_id = gy * n_cols + gx trên lưới cell_deg; điểm phải nằm trong bbox."""
    b, c = grid["bbox"], grid["cell_deg"]
    gx = F.least(F.floor((lon - b["lon_min"]) / c), F.lit(grid["n_cols"] - 1)).cast("int")
    gy = F.least(F.floor((lat - b["lat_min"]) / c), F.lit(grid["n_rows"] - 1)).cast("int")
    return gy * grid["n_cols"] + gx


def zone_gx(zid, grid):
    return (zid % grid["n_cols"]).cast("int")


def zone_gy(zid, grid):
    return F.floor(zid / grid["n_cols"]).cast("int")


def zone_center(zid, grid):
    b, c = grid["bbox"], grid["cell_deg"]
    lon = F.lit(b["lon_min"]) + (zone_gx(zid, grid) + 0.5) * c
    lat = F.lit(b["lat_min"]) + (zone_gy(zid, grid) + 0.5) * c
    return lon, lat
