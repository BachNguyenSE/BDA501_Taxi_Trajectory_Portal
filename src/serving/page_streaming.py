"""Trang "Giám sát streaming" (mục 12-14 nhìn từ giao diện).

Cho người xem thấy luồng streaming đang làm gì, đọc thẳng từ MongoDB:
  ① đồng hồ dữ liệu  : giờ sự kiện mới nhất Spark đã nhận, watermark, tiến độ trong khung phát lại
  ② hành trình tin    : số tin nhận, JSON hỏng, bản trùng bị bỏ, bản trễ bị bỏ, đoạn đã chốt trong cửa sổ
  ③ bản đồ cửa sổ     : mọi zone có dữ liệu trong một cửa sổ, tốc độ lúc đó so với lịch sử cùng zone, cùng giờ
  ④ nhật ký           : các cửa sổ vừa đóng, số zone chậm bất thường
  ⑤ xem lại           : chọn bất kỳ cửa sổ đã lưu (kể cả khi streaming đã dừng)
Nguồn: stream_quality (type run / progress / quality do src/streaming/stream_job.py ghi), zone_speed_live,
zone_hour_speed, zones. Trang chỉ đọc, không ảnh hưởng ETA. Dữ liệu là bản PHÁT LẠI, không phải real-time.
"""
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import streamlit as st
from pymongo import MongoClient

from core.common import ROOT, load_config

cfg = load_config()
TZ = ZoneInfo(cfg["project"]["timezone_local"])
SLOW = cfg["mongo"]["live_slow_ratio"]
N_WINDOWS_PER_EVENT = 3          # cửa sổ 15 phút trượt 5 phút: mỗi đoạn thuộc đúng 3 cửa sổ
COLORS = {"bình thường": "#97C459", "chậm bất thường": "#E24B4A"}


@st.cache_resource
def get_db():
    mc = cfg["mongo"]
    return MongoClient(mc["uri"], serverSelectionTimeoutMS=5000)[mc["database"]]


@st.cache_data(ttl=3600)
def static_tables():
    db = get_db()
    zones = pd.DataFrame(list(db.zones.find({}, {"_id": 0, "zone_id": 1, "gx": 1, "gy": 1})))
    zhs = pd.DataFrame(list(db.zone_hour_speed.find({}, {"_id": 0, "zone_id": 1, "hour": 1, "is_weekend": 1,
                                                         "total_km": 1, "total_s": 1})))
    zhs["hist_kmh"] = zhs["total_km"] / (zhs["total_s"] / 3600.0)
    return zones, zhs[["zone_id", "hour", "is_weekend", "hist_kmh"]]


def local(iso_utc):
    """Chuỗi thời gian UTC Spark báo (ISO) -> giờ Lisbon, không kèm múi giờ. None nếu chưa có."""
    if not isinstance(iso_utc, str) or iso_utc.startswith("1970"):   # batch chưa có dữ liệu: NaN hoặc mốc 0
        return None
    return pd.Timestamp(iso_utc).tz_convert(TZ).tz_localize(None)


def compare_with_history(live, zhs):
    """Mỗi dòng (zone, cửa sổ): tốc độ live / tốc độ lịch sử cùng zone, cùng giờ (giờ đầu cửa sổ), cùng loại ngày."""
    if live.empty:
        return live
    d = live.copy()
    d["hour"] = d["window_start"].dt.hour
    d["is_weekend"] = d["window_start"].dt.dayofweek >= 5
    d["live_kmh"] = d["total_km"] / (d["total_s"] / 3600.0)
    d = d.merge(zhs, on=["zone_id", "hour", "is_weekend"], how="left")
    d["ratio"] = d["live_kmh"] / d["hist_kmh"]
    d["trạng thái"] = d["ratio"].lt(SLOW).map({True: "chậm bất thường", False: "bình thường"})
    return d


st.title("Giám sát streaming")
st.caption("Phát lại ×60 dữ liệu GPS có thật ngày 14/05/2014 qua Kafka → Spark Structured Streaming → MongoDB. "
           "Đây là **bản phát lại**, không phải real-time. Lớp live chỉ đánh dấu zone chậm, không đổi ETA.")

with st.expander("Đọc trang này thế nào"):
    st.markdown(f"""
- **Giờ dữ liệu** là giờ ghi trong GPS (14/05/2014), không phải giờ máy tính. Phát ×60 nên mỗi giây ngoài đời đồng hồ này nhảy 1 phút.
- **Watermark** = giờ dữ liệu mới nhất trừ {cfg['streaming']['spark']['watermark']}. Tin nào cũ hơn watermark thì Spark coi là quá trễ và bỏ.
- **Cửa sổ** dài {cfg['streaming']['spark']['window']}, cứ {cfg['streaming']['spark']['slide']} tạo một cửa sổ mới. Một cửa sổ chỉ được **chốt và ghi xuống MongoDB** khi watermark đã vượt qua giờ kết thúc của nó, nên kết quả live luôn chậm hơn đồng hồ dữ liệu một chút.
- **Chậm bất thường**: tốc độ của zone trong cửa sổ thấp hơn {SLOW:.0%} tốc độ lịch sử của chính zone đó vào cùng giờ, cùng loại ngày.
""")

auto = st.toggle("Tự cập nhật mỗi 3 giây", value=True)


@st.fragment(run_every="3s" if auto else None)
def body():
    db = get_db()
    run = db.stream_quality.find_one({"_id": "run|current"})
    if not run:
        st.info("Chưa có lần chạy streaming nào. Chạy `./scripts/run_streaming.sh` ở terminal.")
        return
    zones, zhs = static_tables()
    prog = pd.DataFrame(list(db.stream_quality.find({"type": "progress", "run_id": run["run_id"]}, {"_id": 0})))
    qual = pd.DataFrame(list(db.stream_quality.find({"type": "quality", "run_id": run["run_id"]}, {"_id": 0})))
    live = pd.DataFrame(list(db.zone_speed_live.find({}, {"_id": 0, "zone_id": 1, "window_start": 1, "window_end": 1,
                                                          "total_km": 1, "total_s": 1, "n_segments": 1})))
    lp = prog[prog["query"] == "live"] if not prog.empty else prog

    # ---------------- ① đồng hồ dữ liệu
    w = run["window_local"]
    w0, w1 = pd.Timestamp(w["start"]), pd.Timestamp(w["end"])
    last_seen = prog["processed_at"].max() if not prog.empty else None
    if run["status"] == "stopped":
        status = "⏹ Đã dừng"
    elif last_seen is None:
        status = "⏳ Đang khởi động, chờ tin đầu tiên"
    elif time.time() - last_seen < 20:
        status = "🟢 Đang chạy"
    else:
        status = "⚠️ Không thấy batch mới trong 20 giây"
    emax = max((t for t in map(local, lp["event_time_max_utc"]) if t is not None), default=None) if not lp.empty else None
    wm = local(lp.sort_values("batch_id")["watermark_utc"].iloc[-1]) if not lp.empty else None

    st.subheader("① Đồng hồ dữ liệu")
    st.markdown(f"**Trạng thái:** {status}")
    c2, c3 = st.columns(2)
    if emax is None:
        c2.metric("Dữ liệu đã nhận tới", "—")
        frac = 0.0
    elif emax > w1:
        c2.metric("Dữ liệu đã nhận tới", f"{w1:%H:%M}", help="Đã phát hết khung; tin cuối là message kết thúc để đẩy watermark")
        frac = 1.0
    else:
        c2.metric("Dữ liệu đã nhận tới", f"{emax:%H:%M:%S}")
        frac = max(0.0, min(1.0, (emax - w0) / (w1 - w0)))
    c3.metric("Watermark", f"{wm:%H:%M:%S}" if wm is not None else "chưa có")
    if wm is not None and wm > w1:
        st.caption(f"Watermark vượt giờ kết thúc khung ({w1:%H:%M}) vì producer gửi một **message kết thúc** có giờ "
                   "sự kiện sau khung 1 giờ. Message này đẩy watermark lên để Spark chốt nốt các cửa sổ cuối; "
                   "nó nằm ngoài bbox nên không được tính vào tốc độ zone.")
    st.progress(frac, text=f"Khung phát lại {w0:%d/%m/%Y %H:%M} → {w1:%H:%M} (giờ Porto) · {frac:.0%} · "
                           f"tốc độ ×{run['speedup']} · bắt đầu chạy lúc "
                           f"{datetime.fromtimestamp(run['started_at'], TZ):%H:%M:%S} (giờ máy)")

    # ---------------- ② hành trình tin nhắn
    st.subheader("② Hành trình tin nhắn (cộng dồn từ lúc bắt đầu)")
    received = int(qual["n_messages"].sum()) if not qual.empty else 0
    corrupt = int(qual["n_corrupt_json"].sum()) if not qual.empty else 0
    dups = int(lp["dropped_duplicates"].sum()) if not lp.empty else 0
    late = int(lp["dropped_by_watermark"].sum()) if not lp.empty else 0
    agg = live["n_segments"].sum() / N_WINDOWS_PER_EVENT if not live.empty else 0
    m = st.columns(3) + st.columns(2)
    m[0].metric("Tin Spark đã nhận", f"{received:,}")
    m[1].metric("JSON hỏng bị loại", f"{corrupt:,}")
    m[2].metric("Bản trùng bị bỏ", f"{dups:,}")
    m[3].metric("Bản trễ bị bỏ", f"{late:,}")
    m[4].metric("Đoạn đã chốt", f"{agg:,.0f}", help="Đoạn hợp lệ đã nằm trong cửa sổ ĐÃ ĐÓNG; tổng n_segments chia 3 vì mỗi đoạn thuộc 3 cửa sổ trượt")
    man_p = ROOT / cfg["streaming"]["replay"]["manifest"]
    if run["status"] == "stopped" and man_p.exists() and man_p.stat().st_mtime >= run["started_at"]:
        man = json.loads(man_p.read_text(encoding="utf-8"))
        st.caption(f"Sổ của producer (manifest) cho lần chạy này: gửi {man['messages_sent_total']:,} tin, "
                   f"cố tình làm hỏng {man['corrupt_json_sent']:,}, gửi trùng {man['duplicate_copies_sent']:,}, "
                   f"gửi trễ {man['late_sent']:,}. Bảng đối soát đầy đủ: evidence/streaming/summary.md")

    # ---------------- ③ ④ ⑤ cửa sổ
    if live.empty:
        st.info("Chưa có cửa sổ nào được chốt. Cửa sổ đầu tiên đóng khi watermark vượt qua giờ kết thúc của nó.")
        return
    live["window_start"] = pd.to_datetime(live["window_start"])
    live["window_end"] = pd.to_datetime(live["window_end"])
    cmp = compare_with_history(live, zhs)
    per_w = (cmp.groupby(["window_start", "window_end"])
             .agg(zones=("zone_id", "count"), segments=("n_segments", "sum"),
                  slow=("trạng thái", lambda s: int((s == "chậm bất thường").sum())))
             .reset_index().sort_values("window_end"))
    labels = [f"{a:%H:%M}–{b:%H:%M}" for a, b in zip(per_w["window_start"], per_w["window_end"])]

    st.subheader("③ Bản đồ cửa sổ live")
    follow = st.checkbox("Luôn hiện cửa sổ mới nhất", value=True, key="follow_latest")
    if follow:
        pick = labels[-1]
    else:
        pick = st.select_slider("⑤ Xem lại cửa sổ", options=labels, value=labels[-1], key="replay_window")
    sel = per_w.iloc[labels.index(pick)]
    win = cmp[cmp["window_start"] == sel["window_start"]]
    st.markdown(f"**Cửa sổ đang xem:** {pick} (giờ Porto, ngày {sel['window_start']:%d/%m/%Y})")
    k2, k3 = st.columns(2)
    k2.metric("Zone có dữ liệu", int(sel["zones"]))
    k3.metric("Zone chậm bất thường", int(sel["slow"]))

    xs = alt.Scale(domain=[int(zones["gx"].min()), int(zones["gx"].max()) + 1])
    ys = alt.Scale(domain=[int(zones["gy"].min()), int(zones["gy"].max()) + 1])
    enc = dict(x=alt.X("gx:Q", scale=xs, axis=None), x2="gx2:Q", y=alt.Y("gy:Q", scale=ys, axis=None), y2="gy2:Q")
    bg = zones.assign(gx2=zones["gx"] + 0.9, gy2=zones["gy"] + 0.9)
    wz = win.merge(zones, on="zone_id").assign(gx2=lambda d: d["gx"] + 0.9, gy2=lambda d: d["gy"] + 0.9)
    wz["tỉ lệ so với lịch sử"] = wz["ratio"].round(2)
    base = alt.Chart(bg).mark_rect(color="#E8E6DF").encode(**enc)
    cells = alt.Chart(wz).mark_rect().encode(
        **enc, color=alt.Color("trạng thái:N", title="Trong cửa sổ này",
                               scale=alt.Scale(domain=list(COLORS), range=list(COLORS.values()))),
        tooltip=["zone_id", "trạng thái", "tỉ lệ so với lịch sử", "n_segments"])
    st.altair_chart((base + cells).properties(height=430), width="stretch")
    st.caption("Ô xám: zone có xe đi qua trong lịch sử nhưng không có dữ liệu trong cửa sổ này. "
               "Rê chuột lên ô để xem tỉ lệ tốc độ live / lịch sử.")

    st.subheader("Số zone chậm bất thường theo từng cửa sổ")
    line = alt.Chart(per_w.assign(t=per_w["window_end"])).mark_line(point=True).encode(
        x=alt.X("t:T", title="Giờ kết thúc cửa sổ (giờ Porto)", axis=alt.Axis(format="%H:%M")),
        y=alt.Y("slow:Q", title="Zone chậm bất thường"),
        tooltip=[alt.Tooltip("t:T", format="%H:%M", title="kết thúc"), "zones", "slow", "segments"])
    st.altair_chart(line.properties(height=220), width="stretch")

    st.subheader("④ Nhật ký: cửa sổ vừa đóng")
    log = per_w.tail(8).iloc[::-1]
    st.dataframe(pd.DataFrame({
        "cửa sổ (giờ Porto)": [f"{a:%H:%M}–{b:%H:%M}" for a, b in zip(log["window_start"], log["window_end"])],
        "zone có dữ liệu": log["zones"], "đoạn": log["segments"], "zone chậm bất thường": log["slow"],
    }), hide_index=True, width="stretch")


body()
