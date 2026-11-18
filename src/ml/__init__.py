"""ml: mục 7, 8 của sơ đồ. Dự đoán thời gian chuyến (known-destination ETA).

evaluation.py        danh sách feature được phép, metric (MAE, RMSE, R², signed error), baseline tốc độ lịch sử chỉ từ train
interval.py          tra khoảng ước lượng 80% từ models/interval.json
job_train_eta.py     bước 4: 2 baseline + Linear + GBT, chia theo thời gian, kiểm tra leakage, lưu model   (scripts/run_ml.sh)
job_eta_interval.py  bước 4b: hiệu chỉnh khoảng 80% trên 04/2014, đo bao phủ trên test   (scripts/run_ml.sh --full)
"""
