"""Khoảng ước lượng thời gian (phần mở rộng ngoài đề, đã duyệt).

Khoảng 80% = dự đoán + [phân vị 10%, phân vị 90%] của sai số (thực tế − dự đoán), tính RIÊNG cho từng nhóm
độ dài dự đoán, vì chuyến dài sai nhiều hơn và sai số lệch về phía dài.
Sai số lấy trên tháng hiệu chỉnh (04/2014, nằm trong giai đoạn train) từ một model chỉ học dữ liệu trước đó;
tập test chỉ dùng MỘT lần để đo độ bao phủ thật.
"""
import bisect


def lookup(table: dict, pred_min: float):
    """table: {"bin_edges": [...], "q_low": [...], "q_high": [...], "min_duration_min": x}."""
    i = bisect.bisect_right(table["bin_edges"], pred_min) - 1
    i = min(max(i, 0), len(table["q_low"]) - 1)
    lo = max(pred_min + table["q_low"][i], table["min_duration_min"])
    hi = max(pred_min + table["q_high"][i], lo)
    return round(lo, 2), round(hi, 2)
