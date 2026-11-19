"""Giao diện Streamlit (mục 10): điểm vào, chỉ điều hướng giữa hai trang.

Chạy: scripts/run_demo.sh  ->  http://localhost:8501
  page_estimate.py   Ước lượng ETA: chọn A, B, giờ -> ETA, khoảng 80%, tuyến trên lưới zone, zone tắc, lớp live
  page_streaming.py  Giám sát streaming: đồng hồ dữ liệu, hành trình tin nhắn, bản đồ cửa sổ live, nhật ký, xem lại
"""
import streamlit as st

st.set_page_config(page_title="Porto taxi ETA", layout="wide")
nav = st.navigation([
    st.Page("page_estimate.py", title="Ước lượng ETA", icon=":material/route:", default=True),
    st.Page("page_streaming.py", title="Giám sát streaming", icon=":material/monitoring:"),
])
nav.run()
