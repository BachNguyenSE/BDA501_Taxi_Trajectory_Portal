"""Trang "Ước lượng ETA" của giao diện Streamlit (mục 10). Mở qua app.py: scripts/run_demo.sh -> http://localhost:8501

Ba cách nhập A, B: địa điểm có sẵn, chuyến mẫu từ tập test, chọn trên bản đồ thật.
Tuyến vẽ trên LƯỚI ZONE dạng sơ đồ (không vẽ đường chim bay trên bản đồ thật).
Lớp live chỉ đánh dấu, không đổi ETA; dữ liệu live là bản phát lại, không phải real-time.
"""
import datetime as dt
import json


import altair as alt  # noqa: E402
import folium  # noqa: E402
import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402
from streamlit_folium import st_folium  # noqa: E402

from core.common import ROOT, load_config  # noqa: E402
from serving.estimate import Estimator  # noqa: E402

cfg = load_config()
LEVEL_COLORS = {"thông thoáng": "#97C459", "đông": "#EF9F27", "tắc": "#E24B4A", "không đủ dữ liệu": "#888780"}
D_MIN, D_MAX = dt.date(2013, 7, 1), dt.date(2014, 6, 30)
DEFAULT_DEP = dt.datetime.strptime(cfg["ui"]["default_departure"], "%Y-%m-%d %H:%M")


@st.cache_resource(show_spinner="Đang nạp model và dữ liệu từ MongoDB...")
def get_estimator():
    e = Estimator(cfg)
    e.model()  # khởi động Spark một lần
    return e


est = get_estimator()
landmarks = [x for x in json.loads((ROOT / cfg["ui"]["landmarks"]).read_text(encoding="utf-8")) if x["ok"]]
samples = json.loads((ROOT / cfg["ui"]["sample_trips"]).read_text(encoding="utf-8"))

st.title("Porto taxi ETA")
st.caption("Ước lượng thời gian, vùng có khả năng tắc và tuyến gợi ý từ quỹ đạo GPS taxi Porto (07/2013–06/2014)")

left, right = st.columns([1, 1.6], gap="large")
actual = None
with left:
    mode = st.radio("Cách nhập", ["Địa điểm có sẵn", "Chuyến mẫu từ tập test", "Chọn trên bản đồ"], horizontal=True)

    if mode == "Địa điểm có sẵn":
        names = [x["name"] for x in landmarks]
        a = st.selectbox("Điểm đi", names, index=0)
        b = st.selectbox("Điểm đến", names, index=1)
        la, lb = landmarks[names.index(a)], landmarks[names.index(b)]
        origin, dest = (la["lat"], la["lon"]), (lb["lat"], lb["lon"])
        st.caption("Tọa độ địa điểm là xấp xỉ, đã kiểm tra nằm trong vùng có nhiều dữ liệu taxi.")
    elif mode == "Chuyến mẫu từ tập test":
        labels = [f"{s['start_local'][:16]} · {s['straight_km']:.1f} km · thực tế {s['actual_duration_min']:.1f} phút"
                  for s in samples]
        pick = samples[labels.index(st.selectbox("Chuyến (05–06/2014, model chưa từng thấy)", labels))]
        origin, dest = tuple(pick["origin"]), tuple(pick["destination"])
        actual = pick["actual_duration_min"]
        st.caption("20 chuyến chọn ngẫu nhiên (seed 42) trong tập test, bỏ chuyến có bước nhảy GPS và chuyến dưới 1 km.")
    else:
        st.caption("Bấm lần 1 chọn điểm đi, lần 2 chọn điểm đến.")
        pts = st.session_state.setdefault("map_points", [])
        m = folium.Map(location=[41.16, -8.62], zoom_start=12, tiles="OpenStreetMap")
        for i, p in enumerate(pts):
            folium.Marker(p, tooltip="A" if i == 0 else "B",
                          icon=folium.Icon(color="green" if i == 0 else "red")).add_to(m)
        click = st_folium(m, height=320, width=None, key="pick_map")
        if click and click.get("last_clicked"):
            p = (round(click["last_clicked"]["lat"], 6), round(click["last_clicked"]["lng"], 6))
            if (not pts or p != pts[-1]) and len(pts) < 2:
                pts.append(p)
                st.rerun()
        if st.button("Chọn lại"):
            st.session_state["map_points"] = []
            st.rerun()
        origin = pts[0] if len(pts) > 0 else None
        dest = pts[1] if len(pts) > 1 else None

    if mode == "Chuyến mẫu từ tập test":
        dep = dt.datetime.strptime(pick["start_local"], "%Y-%m-%d %H:%M:%S")
        st.write(f"Thời điểm xuất phát (giờ Porto): **{dep:%d/%m/%Y %H:%M}**")
    else:
        c1, c2 = st.columns(2)
        d = c1.date_input("Ngày", value=DEFAULT_DEP.date(), min_value=D_MIN, max_value=D_MAX, format="DD/MM/YYYY")
        quick = st.radio("Giờ nhanh", ["Tự chọn", "Sáng 8h", "Chiều 18h", "Đêm 2h"], horizontal=True)
        t = {"Sáng 8h": dt.time(8), "Chiều 18h": dt.time(18), "Đêm 2h": dt.time(2)}.get(quick) or \
            c2.time_input("Giờ (Porto)", value=DEFAULT_DEP.time(), step=dt.timedelta(minutes=15))
        dep = dt.datetime.combine(d, t)

    go = st.button("Ước lượng", type="primary", width="stretch")
    if go:
        if origin is None or dest is None:
            st.error("Chọn đủ điểm đi và điểm đến trên bản đồ trước.")
        else:
            with st.spinner("Đang tính..."):
                st.session_state["result"] = (est.estimate(origin, dest, dep), origin, dest, dep, actual)

with right:
    if "result" not in st.session_state:
        st.info("Chọn điểm đi, điểm đến, thời điểm rồi bấm Ước lượng.")
    else:
        res, origin, dest, dep, actual = st.session_state["result"]
        route, live = res["route"], res["live"]
        m1, m2 = st.columns(2)
        iv = res.get("predicted_interval_min")
        m1.metric("Thời gian dự kiến", f"{res['predicted_duration_min']:.0f} phút",
                  delta=None if actual is None else f"{res['predicted_duration_min'] - actual:+.1f} so với thực tế",
                  delta_color="off")
        if iv:
            m1.caption(f"Thường từ **{iv[0]:.0f} đến {iv[1]:.0f} phút** (khoảng 80% chuyến tương tự)")
        m2.metric("Tuyến gợi ý", f"{len(route)} zone")
        m3, m4 = st.columns(2)
        n_busy = sum(z["level"] == "đông" for z in route)
        m3.metric("Theo lịch sử giờ này: tắc / đông", f"{len(res['congested_zones'])} / {n_busy} zone")
        m4.metric("Lúc này chậm bất thường (live)",
                  f"{len(live['slow_zones'])} zone" if live["available"] else "chưa có dữ liệu")
        if actual is not None:
            inside = iv and iv[0] <= actual <= iv[1]
            st.caption(f"Chuyến thật: {actual:.1f} phút. Model dự đoán {res['predicted_duration_min']:.1f} phút"
                       + (f", khoảng {iv[0]:.1f}–{iv[1]:.1f}: thực tế {'nằm trong' if inside else 'nằm ngoài'} khoảng." if iv else "."))
        f = res["debug"]["features"]
        weekday = ["Thứ hai", "Thứ ba", "Thứ tư", "Thứ năm", "Thứ sáu", "Thứ bảy", "Chủ nhật"][dep.weekday()]
        st.caption(f"{weekday} {dep:%d/%m/%Y %H:%M} · {'cuối tuần' if f['is_weekend'] else 'ngày thường'}"
                   f"{' · ngày lễ' if f['is_holiday'] else ''} · đường thẳng {f['straight_km']:.2f} km")

        if route:
            rz = pd.DataFrame(route)
            pad = 3
            x0, x1 = rz["gx"].min() - pad, rz["gx"].max() + pad
            y0, y1 = rz["gy"].min() - pad, rz["gy"].max() + pad
            bg = est.zones.reset_index()
            bg = bg[bg["gx"].between(x0, x1) & bg["gy"].between(y0, y1)]
            slow = {z["zone_id"] for z in live.get("slow_zones", [])}
            rz["live_slow"] = rz["zone_id"].isin(slow)
            rz["order"] = range(len(rz))
            xs, ys = alt.Scale(domain=[x0, x1 + 1]), alt.Scale(domain=[y0, y1 + 1])
            cxy = dict(x=alt.X("cx:Q", scale=xs, axis=None), y=alt.Y("cy:Q", scale=ys, axis=None))
            enc = dict(x=alt.X("gx:Q", scale=alt.Scale(domain=[x0, x1 + 1]), axis=None),
                       x2="gx2:Q",
                       y=alt.Y("gy:Q", scale=alt.Scale(domain=[y0, y1 + 1]), axis=None),
                       y2="gy2:Q")
            base = alt.Chart(bg.assign(gx2=bg["gx"] + 0.92, gy2=bg["gy"] + 0.92)).mark_rect(color="#E8E6DF").encode(**enc)
            cells = alt.Chart(rz.assign(gx2=rz["gx"] + 0.92, gy2=rz["gy"] + 0.92)).mark_rect().encode(
                **enc,
                color=alt.Color("level:N", title="Theo lịch sử giờ này",
                                scale=alt.Scale(domain=list(LEVEL_COLORS), range=list(LEVEL_COLORS.values()))),
                tooltip=["zone_id", "level", "speed_kmh", "ratio_vs_all_day"])
            outline = alt.Chart(rz[rz["live_slow"]].assign(gx2=lambda d: d["gx"] + 0.92, gy2=lambda d: d["gy"] + 0.92)) \
                .mark_rect(fill=None, stroke="#A32D2D", strokeWidth=3).encode(**enc)
            path = alt.Chart(rz.assign(cx=rz["gx"] + 0.46, cy=rz["gy"] + 0.46)).mark_line(
                color="#2C2C2A", strokeDash=[3, 3]).encode(**cxy, order="order:Q")
            # nhãn A, B nằm ngay trên ô đầu và ô cuối, viền đen đậm cho hai ô này
            ends = pd.DataFrame([{"gx": route[0]["gx"], "gy": route[0]["gy"], "t": "A"},
                                 {"gx": route[-1]["gx"], "gy": route[-1]["gy"], "t": "B"}])
            ends = ends.assign(gx2=ends["gx"] + 0.92, gy2=ends["gy"] + 0.92, cx=ends["gx"] + 0.46, cy=ends["gy"] + 0.46)
            end_box = alt.Chart(ends).mark_rect(fill=None, stroke="#2C2C2A", strokeWidth=2.5).encode(**enc)
            labels = alt.Chart(ends).mark_text(fontSize=13, fontWeight="bold", color="#2C2C2A").encode(
                **cxy, text="t:N")
            st.markdown("**Tuyến trên lưới zone** (sơ đồ, mỗi ô ≈ 420 × 555 m, không phải đường thật)")
            st.altair_chart((base + cells + outline + path + end_box + labels).properties(height=380), width="stretch")
            st.caption("Màu ô: mức đông theo LỊCH SỬ của zone tại giờ này (so với cả ngày của chính zone). "
                       "Viền đỏ đậm: zone LÚC NÀY chậm bất thường theo dữ liệu live (không đổi ETA). Ô xám nhạt: zone có xe đi qua.")
        if live["available"]:
            st.caption(f"Live: cửa sổ đã đóng gần nhất {live['window_start']} – {live['window_end']}, "
                       f"{live['zones_with_live_data']} zone trên tuyến có dữ liệu.")
        else:
            st.caption(f"Live: {live['reason']}.")

        mp = folium.Map(location=[(origin[0] + dest[0]) / 2, (origin[1] + dest[1]) / 2], zoom_start=12)
        folium.Marker(origin, tooltip="A: điểm đi", icon=folium.Icon(color="green")).add_to(mp)
        folium.Marker(dest, tooltip="B: điểm đến", icon=folium.Icon(color="red")).add_to(mp)
        st.markdown("**Vị trí A, B trên bản đồ thật** (chỉ để định vị, tuyến xem ở lưới zone phía trên)")
        st_folium(mp, height=260, width=None, key="result_map", returned_objects=[])

        with st.expander("Chi tiết: zone trên tuyến, feature, model"):
            st.dataframe(pd.DataFrame(route)[["zone_id", "level", "speed_kmh", "ratio_vs_all_day"]]
                         .rename(columns={"level": "mức (lịch sử)", "speed_kmh": "km/h giờ này", "ratio_vs_all_day": "so với cả ngày"}), hide_index=True)
            st.json(res["debug"])

st.divider()
st.caption("ETA từ model GBT (Spark ML) huấn luyện trên chuyến trước 05/2014, chỉ dùng thông tin có trước khi xuất phát. "
           "Khoảng ước lượng hiệu chỉnh trên sai số tháng 04/2014, đo trên tập test chứa 80,4% chuyến thật. "
           "Lớp live chỉ đánh dấu, không thay đổi ETA. Dữ liệu live là bản phát lại ×60, không phải real-time.")
