"""serving: mục 9, 10 của sơ đồ. MongoDB, tìm tuyến, hàm estimate() và giao diện Streamlit.

job_load_mongo.py   bước 5: nạp gold vào MongoDB, index, explain, đo độ trễ   (scripts/run_demo.sh)
make_ui_data.py     địa điểm có sẵn (đã kiểm bằng dữ liệu) và chuyến mẫu từ tập test
routing.py          đồ thị zone + Dijkstra; thời gian cạnh theo tốc độ zone × giờ; bỏ cạnh ít lượt
estimate.py         estimate(A, B, giờ) -> ETA, khoảng 80%, tuyến, zone tắc, lớp live
app.py              giao diện Streamlit (http://localhost:8501)
"""
