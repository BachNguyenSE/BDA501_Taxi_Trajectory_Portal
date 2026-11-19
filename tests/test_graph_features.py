"""Feature đồ thị: tính đúng và KHÔNG để chuyến huấn luyện thấy dữ liệu của chính fold tuần của nó."""
import numpy as np
import pandas as pd

from core.common import load_config
from ml.graph_features import all_folds, leave_fold_out, route_features

CFG = {**load_config()}
CFG["routing"] = {**CFG["routing"], "min_edge_transitions": 1, "min_zone_hour_segments": 1}


def test_leave_fold_out_excludes_own_fold():
    agg = pd.DataFrame({"fold": [0, 1, 2, 0], "zone_id": [7, 7, 7, 8], "n": [10, 20, 30, 5]})
    oof0 = leave_fold_out(agg, ["zone_id"], ["n"], 0)
    assert oof0.set_index("zone_id")["n"].to_dict() == {7: 50}        # zone 8 chỉ có ở fold 0 -> biến mất
    assert all_folds(agg, ["zone_id"], ["n"]).set_index("zone_id")["n"].to_dict() == {7: 60, 8: 5}


def _graph():
    # 1 -> 2 -> 3 trên một hàng zone, cạnh 0,5 km mỗi bước; zone tốc độ 30 km/h
    edges = pd.DataFrame({"zone_from": [1, 2], "zone_to": [2, 3], "n_transitions": [10, 10], "center_km": [0.5, 0.5]})
    zhs = pd.DataFrame({"zone_id": [1, 2, 3], "hour": 8, "is_weekend": False,
                        "total_km": [30.0] * 3, "total_s": [3600] * 3, "n_segments": [100] * 3})
    return zhs, edges


def test_route_features_values_and_missing():
    zhs, edges = _graph()
    trips = pd.DataFrame({"origin_zone": [1.0, 1.0, np.nan, 3.0], "dest_zone": [3.0, 1.0, 3.0, 1.0],
                          "hour": 8, "is_weekend": False})
    f = route_features(trips, zhs, edges, CFG)
    assert abs(f.loc[0, "graph_km"] - 1.0) < 1e-9
    assert abs(f.loc[0, "graph_time_min"] - 2.0) < 1e-9       # 1 km ở 30 km/h = 2 phút
    assert f.loc[1, "graph_time_min"] == 0 and f.loc[1, "graph_found"] == 1
    assert f.loc[2, "graph_found"] == 0 and f.loc[2, "graph_time_min"] == -1    # zone xuất phát ngoài bbox
    assert f.loc[3, "graph_found"] == 0                         # cạnh một chiều: không có đường 3 -> 1
