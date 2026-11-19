"""Feature thời gian theo đồ thị tuyến (Session 9 đã hứa; thiết kế ở docs/DATA_LEAKAGE.md).

Cho mỗi chuyến (origin_zone, dest_zone, hour, is_weekend), dùng đồ thị zone để tính:
  graph_time_min : thời gian tuyến ngắn nhất theo thời gian, cạnh tính theo tốc độ zone × giờ × loại ngày
  graph_km       : quãng đường ngắn nhất trên đồ thị (cạnh = khoảng cách tâm-tâm), cho biết phải đi vòng bao nhiêu
  graph_found    : 1 nếu tìm được tuyến (điểm đầu và cuối đều thuộc đồ thị), 0 nếu không (khi đó hai cột trên = -1)
Đều chỉ cần A, B và thời điểm xuất phát nên có trước khi xe chạy.

CHỐNG LEAKAGE (bảng tổng hợp truyền vào phải đúng giai đoạn, xem ml/job_graph_features.py):
  - đồ thị và tốc độ zone × giờ chỉ xây từ đoạn GPS TRƯỚC mốc chia;
  - với chuyến dùng để HUẤN LUYỆN: đồ thị xây từ các tuần KHÁC (out-of-fold theo tuần), để chuyến không
    "nhìn thấy" chính dữ liệu của nó qua đồ thị.
"""
import numpy as np
import pandas as pd
from scipy.sparse.csgraph import dijkstra

from serving.routing import ZoneGraph, hourly_edge_times

GRAPH_FEATURES = ["graph_time_min", "graph_km", "graph_found"]


def _lookup(graph: ZoneGraph, origins: np.ndarray, dests: np.ndarray) -> np.ndarray:
    """Khoảng cách ngắn nhất từ mỗi origin tới dest tương ứng (NaN nếu không có tuyến hoặc zone lạ)."""
    out = np.full(len(origins), np.nan)
    oi = np.array([graph.index.get(int(z), -1) for z in origins])
    di = np.array([graph.index.get(int(z), -1) for z in dests])
    ok = (oi >= 0) & (di >= 0)
    if not ok.any():
        return out
    src = np.unique(oi[ok])
    dist = dijkstra(graph.matrix, directed=True, indices=src)   # một lần cho mọi origin của nhóm
    row = {s: k for k, s in enumerate(src)}
    vals = dist[[row[s] for s in oi[ok]], di[ok]]
    out[ok] = np.where(np.isfinite(vals), vals, np.nan)
    return out


def route_features(trips: pd.DataFrame, zhs: pd.DataFrame, edges: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """trips: cột origin_zone, dest_zone, hour, is_weekend (giữ index). zhs, edges: bảng tổng hợp của MỘT đồ thị."""
    r = cfg["routing"]
    out = pd.DataFrame(np.nan, index=trips.index, columns=["graph_time_min", "graph_km"])
    ok = trips["origin_zone"].notna() & trips["dest_zone"].notna()
    t = trips[ok]
    if len(t):
        km_graph = ZoneGraph(edges, min_transitions=r["min_edge_transitions"], times=edges["center_km"])
        out.loc[t.index, "graph_km"] = _lookup(km_graph, t["origin_zone"].to_numpy(), t["dest_zone"].to_numpy())
        for (h, w), g in t.groupby(["hour", "is_weekend"]):
            times, _ = hourly_edge_times(edges, zhs, int(h), bool(w), r["min_zone_hour_segments"])
            tg = ZoneGraph(edges, min_transitions=r["min_edge_transitions"], times=times)
            out.loc[g.index, "graph_time_min"] = _lookup(tg, g["origin_zone"].to_numpy(), g["dest_zone"].to_numpy()) / 60.0
    found = out["graph_time_min"].notna() & out["graph_km"].notna()
    out["graph_found"] = found.astype(float)
    out.loc[~found, ["graph_time_min", "graph_km"]] = -1.0   # GBT tách được nhờ graph_found
    return out


def leave_fold_out(fold_agg: pd.DataFrame, keys: list, sums: list, fold: int) -> pd.DataFrame:
    """Tổng của mọi fold TRỪ fold đang xét: bảng tổng hợp cho chuyến thuộc fold đó (out-of-fold)."""
    tot = fold_agg.groupby(keys, as_index=False)[sums].sum()
    mine = fold_agg[fold_agg["fold"] == fold].drop(columns="fold")
    m = tot.merge(mine, on=keys, how="left", suffixes=("", "_f")).fillna({f"{c}_f": 0 for c in sums})
    for c in sums:
        m[c] = m[c] - m[f"{c}_f"]
    m = m.drop(columns=[f"{c}_f" for c in sums])
    return m[m[sums[-1]] > 0].reset_index(drop=True)


def all_folds(fold_agg: pd.DataFrame, keys: list, sums: list) -> pd.DataFrame:
    return fold_agg.groupby(keys, as_index=False)[sums].sum()
