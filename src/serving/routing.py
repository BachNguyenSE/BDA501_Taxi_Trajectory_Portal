"""Tìm tuyến trên đồ thị zone (đề mục 10): Dijkstra, trọng số = thời gian đi qua cạnh.

Đầu vào là bảng gold `zone_graph_edges` (pandas). Không cần Spark, để giao diện gọi nhanh.
Bộ lọc `min_edge_transitions` (conf routing) bỏ cạnh có quá ít lượt xe khi tìm đường.
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components, dijkstra


def hourly_edge_times(edges: pd.DataFrame, zhs: pd.DataFrame, hour: int, is_weekend: bool, min_segments: int):
    """Thời gian đi qua mỗi cạnh (giây) tại một giờ, từ tốc độ lịch sử zone × giờ × cuối tuần.

    Nửa quãng tâm-tâm đi với tốc độ zone đầu, nửa còn lại với tốc độ zone cuối. Tốc độ là
    tổng km / tổng giờ (gồm cả đoạn dừng). Ô ít dữ liệu lùi dần:
      1. zone × giờ × cuối tuần (n_segments >= min_segments)
      2. zone, gộp mọi giờ (n_segments >= min_segments)
      3. toàn thành phố tại giờ × cuối tuần
    Trả về (Series thời gian theo index của edges, dict đếm mức lùi đã dùng).
    """
    def speed(df):
        return df["total_km"] / (df["total_s"] / 3600.0)

    cell = zhs[(zhs["hour"] == hour) & (zhs["is_weekend"] == is_weekend)]
    lvl1 = speed(cell[cell["n_segments"] >= min_segments].set_index("zone_id"))
    allday = zhs.groupby("zone_id")[["total_km", "total_s", "n_segments"]].sum()
    lvl2 = speed(allday[allday["n_segments"] >= min_segments])
    lvl3 = float(cell["total_km"].sum() / (cell["total_s"].sum() / 3600.0))

    used = {"zone_hour": 0, "zone_all_day": 0, "city_hour": 0}

    def zone_speed(z):
        v = lvl1.reindex(z)
        used["zone_hour"] += int(v.notna().sum())
        v2 = lvl2.reindex(z).to_numpy()
        m = v.isna().to_numpy()
        used["zone_all_day"] += int((m & ~np.isnan(v2)).sum())
        out = np.where(m, v2, v.to_numpy())
        m3 = np.isnan(out)
        used["city_hour"] += int(m3.sum())
        return np.where(m3, lvl3, out)

    va = zone_speed(edges["zone_from"])
    vb = zone_speed(edges["zone_to"])
    half = edges["center_km"].to_numpy() / 2.0
    with np.errstate(divide="ignore"):
        t = half / va * 3600.0 + half / vb * 3600.0
    return pd.Series(t, index=edges.index), used


@dataclass
class Route:
    zones: list          # chuỗi zone_id từ A đến B
    time_s: float        # tổng travel_time_s theo đồ thị
    n_edges: int


class ZoneGraph:
    def __init__(self, edges: pd.DataFrame, min_transitions: int = 1, times: pd.Series = None):
        """times: thời gian cạnh (giây) theo index của edges; mặc định dùng cột travel_time_s."""
        e = edges.assign(travel_time_s=times) if times is not None else edges
        e = e[(e["n_transitions"] >= min_transitions) & np.isfinite(e["travel_time_s"])]
        nodes = np.unique(np.concatenate([e["zone_from"].to_numpy(), e["zone_to"].to_numpy()]))
        self.min_transitions = min_transitions
        self.zone_ids = nodes
        self.index = {int(z): i for i, z in enumerate(nodes)}
        n = len(nodes)
        rows = e["zone_from"].map(self.index).to_numpy()
        cols = e["zone_to"].map(self.index).to_numpy()
        self.matrix = csr_matrix((e["travel_time_s"].to_numpy(dtype=float), (rows, cols)), shape=(n, n))
        self.n_edges = len(e)
        # thành phần liên thông mạnh: trong cùng thành phần thì đi được hai chiều
        self.n_components, self.component = connected_components(self.matrix, directed=True, connection="strong")
        sizes = np.bincount(self.component)
        self.main_component = int(np.argmax(sizes))
        self.main_size = int(sizes.max())

    def in_main(self, zone_id) -> bool:
        i = self.index.get(int(zone_id))
        return i is not None and self.component[i] == self.main_component

    def routes_from(self, origin, destinations):
        """Dijkstra một lần từ origin tới nhiều đích. Trả về {dest: Route hoặc None}."""
        i = self.index.get(int(origin))
        out = {int(d): None for d in destinations}
        if i is None:
            return out
        dist, pred = dijkstra(self.matrix, directed=True, indices=i, return_predecessors=True)
        for d in out:
            j = self.index.get(d)
            if j is None or not np.isfinite(dist[j]):
                continue
            path = [j]
            while path[-1] != i:
                path.append(pred[path[-1]])
            path.reverse()
            out[d] = Route([int(self.zone_ids[k]) for k in path], float(dist[j]), len(path) - 1)
        return out

    def route(self, origin, destination):
        return self.routes_from(origin, [destination])[int(destination)]
