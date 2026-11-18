"""Tìm tuyến trên đồ thị zone và bộ lọc cạnh ít lượt."""

import pandas as pd

from serving.routing import ZoneGraph  # noqa: E402


def edges(rows):
    return pd.DataFrame(rows, columns=["zone_from", "zone_to", "travel_time_s", "n_transitions"])


# 1 -> 2 -> 3 mất 60 + 60 = 120 s (nhiều lượt). 1 -> 3 trực tiếp 10 s nhưng chỉ 1 lượt (số đo không tin được).
E = edges([
    (1, 2, 60, 50), (2, 3, 60, 50), (3, 2, 60, 50), (2, 1, 60, 50),
    (1, 3, 10, 1), (3, 1, 10, 1),
    (4, 3, 30, 2), (3, 4, 30, 2),     # zone 4 chỉ nối bằng cạnh ít lượt
])


def test_shortest_path_uses_all_edges_when_filter_off():
    r = ZoneGraph(E, min_transitions=1).route(1, 3)
    assert r.zones == [1, 3] and r.time_s == 10


def test_filter_drops_sparse_edges():
    r = ZoneGraph(E, min_transitions=5).route(1, 3)
    assert r.zones == [1, 2, 3] and r.time_s == 120 and r.n_edges == 2


def test_connectivity_reports_cut_off_zones():
    g_all, g_f = ZoneGraph(E, 1), ZoneGraph(E, 5)
    assert g_all.in_main(4) and g_all.main_size == 4
    assert not g_f.in_main(4) and g_f.main_size == 3
    assert g_f.route(1, 4) is None


def test_same_origin_destination():
    r = ZoneGraph(E, 5).route(2, 2)
    assert r.zones == [2] and r.time_s == 0


from serving.routing import hourly_edge_times  # noqa: E402


def zhs(rows):
    return pd.DataFrame(rows, columns=["zone_id", "hour", "is_weekend", "total_km", "total_s", "n_segments"])


def test_hourly_edge_time_half_distance_each_zone():
    e = pd.DataFrame({"zone_from": [1], "zone_to": [2], "center_km": [1.0], "n_transitions": [50]})
    z = zhs([(1, 8, False, 10.0, 3600, 100),    # 10 km/h
             (2, 8, False, 30.0, 3600, 100)])   # 30 km/h
    t, used = hourly_edge_times(e, z, 8, False, min_segments=20)
    # 0,5 km ở 10 km/h = 180 s; 0,5 km ở 30 km/h = 60 s
    assert abs(t.iloc[0] - 240) < 1e-9 and used["zone_hour"] == 2


def test_hourly_edge_time_fallbacks():
    e = pd.DataFrame({"zone_from": [1, 3], "zone_to": [2, 2], "center_km": [1.0, 1.0], "n_transitions": [50, 50]})
    z = zhs([(1, 8, False, 1.0, 360, 5),          # ô 8h quá ít đoạn -> lùi về cả ngày của zone 1
             (1, 9, False, 20.0, 3600, 100),      # zone 1 cả ngày: 21 km / 3960 s
             (2, 8, False, 30.0, 3600, 100),
             (3, 9, False, 1.0, 360, 5)])         # zone 3 ít dữ liệu mọi giờ -> lùi về tốc độ thành phố lúc 8h
    t, used = hourly_edge_times(e, z, 8, False, min_segments=20)
    v1 = 21.0 / (3960 / 3600)
    v_city = 31.0 / (3960 / 3600)
    assert abs(t.iloc[0] - (0.5 / v1 * 3600 + 0.5 / 30 * 3600)) < 1e-9
    assert abs(t.iloc[1] - (0.5 / v_city * 3600 + 0.5 / 30 * 3600)) < 1e-9
    assert used == {"zone_hour": 2, "zone_all_day": 1, "city_hour": 1}


def test_graph_accepts_custom_times():
    t = pd.Series([1.0, 1.0, 1.0, 1.0, 999.0, 999.0, 1.0, 1.0], index=E.index)
    r = ZoneGraph(E, min_transitions=1, times=t).route(1, 3)
    assert r.zones == [1, 2, 3] and r.time_s == 2
