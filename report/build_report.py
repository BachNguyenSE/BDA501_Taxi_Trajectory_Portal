"""Tạo báo cáo final (Word) từ các file bằng chứng trong evidence/. Chạy: .venv/bin/python report/build_report.py (cần python-docx)."""
import json
import pathlib

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

R = pathlib.Path("/Users/nguyenbach/Documents/BigData")
E = R / "evidence"
J = lambda p: json.loads((E / p).read_text(encoding="utf-8"))  # noqa: E731
f = lambda x, d=0: f"{x:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")  # số kiểu Việt Nam  # noqa: E731

rc = J("bronze/reject_counts_full.json")["by_reason"]
eda = J("eda/eda_full.json")
D = J("spark/D_counts_full.json")
noise = J("spark/noise_profile_full.json")
B = J("spark/B_benchmark_full.json")["summary"]
q2 = J("spark/Q2_speed_by_hour_full.json")["rows"]
q1 = J("spark/Q1_congested_zones_peak_full.json")["rows"]
mr = J("mapreduce/reconcile_full.json")
ML = J("ml/model_results_full.json")
LK = J("ml/leakage_checks_full.json")
IV = J("ml/interval_validation.json")
GF = J("ml/graph_features.json")
G = J("mongo/G_nosql.json")
API = J("serving/api_latency.json")
F3 = J("serving/F3_end_to_end.json")
ST = J("streaming/summary.json")
RT = J("spark/routing_check.json")
LATE = json.loads((E / "streaming/late_explained.json").read_text())

doc = Document()
sec = doc.sections[0]
sec.page_height, sec.page_width = Cm(29.7), Cm(21.0)
sec.left_margin = sec.right_margin = Cm(2.0)
sec.top_margin = sec.bottom_margin = Cm(2.0)

st = doc.styles["Normal"]
st.font.name = "Arial"
st.font.size = Pt(10.5)
st.element.rPr.rFonts.set(qn("w:eastAsia"), "Arial")
st.paragraph_format.space_after = Pt(4)
for lvl, size in [(1, 15), (2, 12.5), (3, 11)]:
    hs = doc.styles[f"Heading {lvl}"]
    hs.font.name, hs.font.size, hs.font.bold = "Arial", Pt(size), True
    hs.font.color.rgb = RGBColor(0x1F, 0x3A, 0x4D)
    hs.element.rPr.rFonts.set(qn("w:eastAsia"), "Arial")
    hs.paragraph_format.space_before = Pt(12 if lvl == 1 else 8)
    hs.paragraph_format.space_after = Pt(4)
    hs.paragraph_format.keep_with_next = True

ACCENT = "1F3A4D"


def shade(cell, color):
    tcPr = cell._element.get_or_add_tcPr()
    sh = OxmlElement("w:shd")
    sh.set(qn("w:val"), "clear")
    sh.set(qn("w:color"), "auto")
    sh.set(qn("w:fill"), color)
    tcPr.append(sh)


def runs(par, text):
    """**đậm** trong chuỗi."""
    parts = text.split("**")
    for i, t in enumerate(parts):
        if t:
            r = par.add_run(t)
            r.bold = i % 2 == 1


def H(text, lvl=1):
    doc.add_heading(text, level=lvl)


def P(text, italic=False, size=None, align=None):
    p = doc.add_paragraph()
    runs(p, text)
    if italic or size:
        for r in p.runs:
            r.italic = italic or r.italic
            if size:
                r.font.size = Pt(size)
    if align:
        p.alignment = align
    return p


def BL(items):
    for t in items:
        p = doc.add_paragraph(style="List Bullet")
        runs(p, t)


def T(headers, rows, widths=None, size=9):
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, h in enumerate(headers):
        c = t.rows[0].cells[i]
        c.text = ""
        r = c.paragraphs[0].add_run(h)
        r.bold, r.font.size = True, Pt(size)
        r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        shade(c, ACCENT)
    for k, row in enumerate(rows):
        cells = t.add_row().cells
        for i, v in enumerate(row):
            cells[i].text = ""
            runs(cells[i].paragraphs[0], str(v))
            for r in cells[i].paragraphs[0].runs:
                r.font.size = Pt(size)
            if k % 2 == 1:
                shade(cells[i], "EEF2F5")
    if widths:
        t.autofit = False
        scale = 17.0 / sum(widths)                     # trải hết bề ngang 17 cm của trang A4
        for i, w in enumerate(widths):
            t.columns[i].width = Cm(w * scale)
        for row in t.rows:
            for i, w in enumerate(widths):
                row.cells[i].width = Cm(w * scale)
    doc.add_paragraph()
    return t


def IMG(path, width_cm, caption):
    doc.add_picture(str(path), width=Cm(width_cm))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    P(caption, italic=True, size=9, align=WD_ALIGN_PARAGRAPH.CENTER)


def PB():
    doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)


# =========================================================== BÌA
P("")
t = doc.add_paragraph()
t.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = t.add_run("BDA501 · BIG DATA · FINAL PROJECT REPORT")
r.bold, r.font.size = True, Pt(12)
t = doc.add_paragraph()
t.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = t.add_run("Ước lượng thời gian di chuyển, vùng tắc nghẽn và tuyến gợi ý\ntừ quỹ đạo GPS taxi Porto")
r.bold, r.font.size = True, Pt(20)
r.font.color.rgb = RGBColor(0x1F, 0x3A, 0x4D)
P("Challenge Card F · Porto Taxi GPS Trip Duration & ETA", align=WD_ALIGN_PARAGRAPH.CENTER)
P("")
T(["Mục", "Nội dung"], [
    ["Nhóm / Lớp", "Nhóm 4 · BDA501"],
    ["Thành viên", "Nguyễn Thanh Hoàng · Nguyễn Xuân Bách"],
    ["Dữ liệu chính", "Porto taxi trajectories, ECML/PKDD 2015 (UCI #339 / Kaggle, CC BY 4.0): train.csv 1,94 GB, 1.710.670 chuyến, 07/2013–06/2014, 15 giây một điểm GPS"],
    ["Bài toán", "Known-destination ETA: người dùng nhập A, B, giờ xuất phát → thời gian dự kiến, vùng có khả năng tắc, tuyến gợi ý (chuỗi ô lưới 0,005°), và lớp live đánh dấu zone đang chậm bất thường"],
    ["Mã nguồn", "https://github.com/BachNguyenSE/BDA501_Taxi_Trajectory_Portal"],
], widths=[4, 13])
P("**Quy ước:** [I] = đã implement và đã chạy, có bằng chứng trong thư mục evidence/; [P] = chỉ thiết kế. Mọi con số trong báo cáo là **số đo thật trên toàn bộ dữ liệu** của bản final, trừ khi ghi khác. Streaming là **PHÁT LẠI (replay ×60) dữ liệu lịch sử, không phải real-time**.", size=9.5)
PB()

# =========================================================== 1. PHÂN CÔNG
H("1. Thành viên và phân công")
T(["Thành viên", "Phụ trách chính", "Thành phần trong repo"], [
    ["Nguyễn Thanh Hoàng", "ETL / làm sạch dữ liệu, MapReduce, Streaming", "src/batch (bronze, silver, gold), src/mapreduce, src/streaming, Kafka, đối soát MapReduce ↔ Spark và streaming ↔ manifest"],
    ["Nguyễn Xuân Bách", "Machine learning, serving, thí nghiệm hiệu năng", "src/ml, src/serving (MongoDB, tìm tuyến, REST API, giao diện Streamlit), thí nghiệm broadcast vs sort-merge, báo cáo"],
], widths=[3.5, 5, 8.5])
P("Cả hai thành viên cùng rà soát kiến trúc, bằng chứng và danh sách red flag.", size=9.5)

# =========================================================== 2. BÀI TOÁN
H("2. Bài toán và lý do cần Big Data")
BL([
    "**Đầu vào:** điểm A (lat, lon), điểm B (lat, lon), thời điểm xuất phát (giờ Porto). **Đầu ra:** thời gian dự kiến (phút) kèm khoảng 80%, tuyến gợi ý là chuỗi zone, các zone có khả năng tắc theo lịch sử, và các zone **lúc này** chậm bất thường từ luồng streaming.",
    "**Phạm vi đúng:** known-destination ETA. B do người dùng nhập lúc đặt xe nên được dùng; model chỉ dùng thông tin có **trước khi xuất phát**.",
    f"**Quy mô:** 1.710.670 chuyến, {f(eda['gps_quality_valid']['points_total'])} điểm GPS của chuyến hợp lệ, tách thành {f(D['silver_segments'])} đoạn 15 giây. Với khoảng 8 GB RAM, pandas một máy không xử lý nổi; cần Spark (xử lý phân tán) và MapReduce (đối chiếu độc lập).",
    "**Dữ liệu bị loại:** NYC TLC Yellow Taxi chỉ có mã zone, không có quỹ đạo GPS nên không tính được tốc độ theo vùng.",
    "**Bảng tham chiếu:** holidays.csv gồm 10 ngày (9 lễ quốc gia Bồ Đào Nha + São João của Porto) do nhóm tự tổng hợp, vì cột DAY_TYPE của dataset toàn là A. Bốn ngày lễ bị Bồ Đào Nha bỏ trong 2013–2015 đã được loại.",
])

# =========================================================== 3. KIẾN TRÚC
H("3. Kiến trúc hệ thống")
IMG(R / "docs/architecture.png", 17, "Hình 1. Kiến trúc 5 cụm: Nguồn → Thu nhận → Lưu trữ → Xử lý → Phục vụ. Cam = batch, xanh = streaming, nét đứt = đối chiếu / thay thế / chỉ thiết kế.")
T(["#", "Thành phần", "Nhãn", "Vai trò"], [
    ["1", "Kaggle · Porto train.csv", "[I]", "Nguồn dữ liệu duy nhất (file đặt thủ công vào data/raw)"],
    ["2", "Tải + bronze", "[I]", "Parse POLYLINE, giữ mọi dòng kèm reject_reason"],
    ["3", "Data lake Parquet", "[I]", "raw → bronze → silver → gold trên Docker volume"],
    ["4", "holidays.csv", "[I]", "Feature is_holiday"],
    ["5", "Spark batch + Spark SQL", "[I]", "Làm sạch, tách đoạn, bảng gold, Q1–Q3, EXPLAIN, thí nghiệm join"],
    ["6", "MapReduce (YARN + HDFS)", "[I]", "Đối chiếu độc lập với Spark theo từng key; không phục vụ"],
    ["7", "Spark ML", "[I]", "2 baseline, Linear, GBT, temporal hold-out, kiểm tra leakage"],
    ["8", "models/", "[I]", "Model + features.json + bảng khoảng 80%"],
    ["9", "MongoDB", "[I]", "Collection batch và streaming riêng, index theo truy vấn"],
    ["10", "Streamlit + REST API", "[I]", "Trang ước lượng, trang giám sát streaming, POST /estimate"],
    ["11", "Taxi GPS live thật", "[P]", "Chỉ thiết kế; producer giữ cùng schema để thay thế"],
    ["12", "Replay producer", "[I]", "Phát lại ×60, chèn dữ liệu xấu có chủ đích, ghi manifest"],
    ["13", "Kafka (KRaft)", "[I]", "Topic gps-segments, 3 partition, key TAXI_ID"],
    ["14", "Structured Streaming", "[I]", "Watermark, bỏ trùng, cửa sổ 15/5 phút theo zone → MongoDB"],
], widths=[0.8, 4.5, 1.2, 10.5])
P("**Ranh giới batch / streaming / serving:** batch xây prior lịch sử và huấn luyện model; streaming chỉ ghi quan sát mới vào collection riêng (zone_speed_live, stream_quality), không huấn luyện model (không online learning); lớp live **chỉ đánh dấu** zone chậm bất thường, **không bao giờ đổi ETA**. MapReduce không nằm trên đường phục vụ.")

# =========================================================== 4. DỮ LIỆU
H("4. Thu nhận, làm sạch và data lake")
tot = sum(rc.values())
T(["reject_reason (bronze)", "Số dòng", "Tỉ lệ"], [
    ["(hợp lệ)", f(rc["valid"]), f"{rc['valid']/tot:.2%}"],
    ["too_few_points (< 2 điểm)", f(rc["too_few_points"]), f"{rc['too_few_points']/tot:.2%}"],
    ["too_long (> 180 phút)", f(rc["too_long"]), f"{rc['too_long']/tot:.2%}"],
    ["missing_data", f(rc["missing_data"]), f"{rc['missing_data']/tot:.4%}"],
    ["bad_json", "0", "0%"],
    ["**Tổng**", f"**{f(tot)}**", "100%"],
], widths=[7, 4, 3])
BL([
    "**Bronze giữ mọi dòng** kèm lý do loại (thứ tự ưu tiên missing_data > bad_json > too_few_points > too_long) để đối soát được với MapReduce.",
    f"**Silver:** {f(D['silver_trips'])} chuyến và {f(D['silver_segments'])} đoạn 15 giây. Đoạn tạo bằng explode + window lag theo chuyến; **không lọc bbox trước lag**; giữ đoạn khi hai đầu trong bbox và tốc độ ≤ 150 km/h; zone = zone của trung điểm; giờ = giờ Lisbon lúc bắt đầu đoạn.",
    f"**Đối soát số đoạn:** đếm độc lập bằng mảng ra {f(D['segments_expected_from_arrays'])}, bảng silver có {f(D['silver_segments'])}: **khớp tuyệt đối**. Bỏ {f(D['pairs_dropped_outside_bbox'])} cặp ngoài bbox và {f(D['pairs_dropped_speed_over_limit_in_bbox'])} cặp quá 150 km/h.",
    f"**Lỗi dữ liệu phát hiện nhờ đối soát:** TRIP_ID không duy nhất ({D['trip_id_shared_by_several_trips']} chuyến dùng chung ID, cùng xe cùng giây) làm window lag ghép điểm của hai chuyến. Thêm khóa trip_uid = TRIP_ID + md5(polyline). Một chuyến bị ghi 3 lần: silver giữ 1 bản.",
    f"**Gold:** zones ({f(D['gold_rows']['zones'])}), zone_hour_speed ({f(D['gold_rows']['zone_hour_speed'])}, lưu total_km và total_s để cộng dồn), zone_graph_edges ({f(D['gold_rows']['zone_graph_edges'])} cạnh giữa zone liền kề), trip_features ({f(D['gold_rows']['trip_features'])}).",
    f"**Chất lượng GPS:** {eda['gps_quality_valid']['share_trips_with_jump']:.2%} chuyến có ít nhất một bước nhảy > 150 km/h (nhãn có thể bị đếm thiếu); {eda['gps_quality_valid']['share_points_outside_bbox']:.2%} điểm ngoài bbox. Gắn cờ has_gps_jump, không xóa.",
    "**Bố trí file Parquet:** các bảng gold nhỏ (dưới 100 MB) nên ghi không chia partition; silver/segments 2,5 GB được đọc toàn bộ ở mọi bước gold nên chia theo thời gian không giúp bỏ bớt dữ liệu đọc. Định dạng cột giúp chỉ đọc đúng cột cần (thấy rõ ở ReadSchema trong EXPLAIN).",
])

# =========================================================== 5. SPARK
H("5. Spark batch, Spark SQL và thí nghiệm hiệu năng")
wk = [r for r in q2 if not r["is_weekend"]]
slow = sorted(wk, key=lambda r: r["speed_kmh"])[:3]
BL([
    f"**Q1 – zone chậm nhất giờ cao điểm ngày thường:** zone chậm nhất {q1[0]['zone_id']} (tâm {q1[0]['center_lat']}, {q1[0]['center_lon']}, gần sân bay) chỉ {q1[0]['speed_kmh']:.2f} km/h với 91,9% đoạn đứng yên: đây là **khu xe chờ khách, không phải tắc đường**.",
    f"**Q2 – tốc độ theo giờ:** ngày thường chậm nhất lúc " + ", ".join(f"{r['hour']}h ({r['speed_kmh']:.1f} km/h)" for r in slow) + "; nhanh nhất lúc 5h. Giờ cao điểm rõ nhất là 17–19h.",
    "**Q3 – join chuyến với zone × giờ tại điểm xuất phát (tập train):** zone xuất phát càng chậm thì số phút trên mỗi km đường thẳng càng lớn.",
    "**EXPLAIN FORMATTED:** Q1 và Q3 dùng BroadcastHashJoin (bảng nhỏ được broadcast, bảng lớn không shuffle); Exchange hashpartitioning xuất hiện cho GROUP BY và rangepartitioning cho ORDER BY; điều kiện WHERE được đẩy xuống lúc đọc Parquet (PushedFilters).",
])
T(["Thí nghiệm có kiểm soát (cùng phép join, 7 lần đo + 1 lần khởi động)", "Broadcast hash join", "Sort-merge join"], [
    ["Join trong plan cuối (kiểm tra sau khi chạy)", ", ".join(B["broadcast"]["join_types_seen"]), ", ".join(B["sort_merge"]["join_types_seen"])],
    ["Shuffle đọc (MB, từ Spark REST)", f(B["broadcast"]["median_shuffle_read_mb"], 2), f(B["sort_merge"]["median_shuffle_read_mb"], 2)],
    ["Thời gian trung vị (giây)", f(B["broadcast"]["median_seconds"], 3), f(B["sort_merge"]["median_seconds"], 3)],
    ["Khoảng min–max (giây)", f"{f(B['broadcast']['min_seconds'],3)}–{f(B['broadcast']['max_seconds'],3)}", f"{f(B['sort_merge']['min_seconds'],3)}–{f(B['sort_merge']['max_seconds'],3)}"],
], widths=[8, 4.5, 4.5])
P("**Kết luận:** broadcast không shuffle, sort-merge phải phân vùng lại cả hai bảng (khác biệt lặp lại y hệt ở 7 lần). Thời gian chỉ chênh vừa phải vì chạy một máy (shuffle chỉ là đĩa cục bộ); trên cụm, shuffle đi qua mạng nên chênh lệch có thể lớn hơn (suy luận, chưa đo). Broadcast chỉ dùng được khi bảng nhỏ (72.142 dòng) vừa bộ nhớ executor.")

# =========================================================== 6. MAPREDUCE
H("6. MapReduce trên YARN + HDFS")
a, jc = mr["vs_spark_same_logic_as_mr"], mr["job_counters"]
T(["Chỉ số (job full)", "Giá trị"], [
    ["Trạng thái YARN · thời gian", f"{mr['yarn_app']['state']} / {mr['yarn_app']['finalStatus']} · {f(mr['yarn_app']['elapsedTime']/1000,1)} s (image amd64 chạy qua giả lập trên Mac ARM)"],
    ["Map / reduce task", f"{jc['Launched map tasks']} / {jc['Launched reduce tasks']}"],
    ["Map output (đoạn)", f(jc["Map output records"])],
    ["Combiner: input → output", f"{f(jc['Combine input records'])} → {f(jc['Combine output records'])}"],
    ["Reduce shuffle bytes", f(jc["Reduce shuffle bytes"])],
    ["Số key (zone, giờ)", f(jc["Reduce output records"])],
    ["Khớp với Spark SQL cùng logic", f"**{f(a['keys_mr'])} / {f(a['keys_spark'])} key, 0 lệch**; sai số km lớn nhất {a['max_abs_km_diff']:.1e}"],
    ["Counter reject_reason so với bronze", "khớp ở mọi lý do (too_long = 847 ở cả hai bên)"],
], widths=[6, 11])
BL([
    "Mapper, combiner, reducer viết bằng Python, chạy bằng Hadoop Streaming, đọc **raw CSV từ HDFS**. Reducer **chỉ cộng dồn** total_km, total_s, n_segments (phép cộng có tính kết hợp) nên dùng lại được làm combiner; tốc độ chỉ tính ở cuối, **không lấy trung bình của trung bình**.",
    "Lệch duy nhất so với bảng silver: đúng 10 đoạn ở 1 key, do silver bỏ 2 bản ghi lặp còn MapReduce đọc từng dòng.",
    "Lỗi đã xử lý: -files tạo symlink nên mapper thêm sys.path.insert(0, os.getcwd()); chờ YARN bằng REST cổng 8088 thay vì yarn node -list (treo).",
])

# =========================================================== 7. ML
H("7. Machine learning: dự đoán thời gian chuyến")
r0 = ML["results"]["train_all -> test_all"]
BL([
    "**Feature (đúng 6 feature, đều có trước khi xuất phát):** straight_km, bearing_deg, hour, day_of_week, is_weekend, is_holiday. **Nhãn:** duration_min = (số điểm − 1) × 15 giây.",
    f"**Chia theo thời gian (giờ Lisbon):** train trước 01/05/2014 ({f(ML['rows']['train_all'])} chuyến), test 05–06/2014 ({f(ML['rows']['test_all'])} chuyến). Không dùng random split.",
    "**Thuật toán:** Spark ML LinearRegression và GBTRegressor (maxIter 50, maxDepth 6, seed 42), tham số **cố định trước khi chạy**, tập test chỉ dùng một lần để báo cáo.",
])
T(["Model (train đủ → test đủ)", "MAE (phút)", "RMSE", "R²", "Signed error"], [
    [n, f(r0[k]["mae"], 3), f(r0[k]["rmse"], 3), f(r0[k]["r2"], 3), f"{r0[k]['signed_error']:+.3f}".replace(".", ",")]
    for n, k in [("Naive (trung bình train)", "naive_mean"), ("Tốc độ lịch sử (zone × giờ, chỉ từ train)", "historical_speed"),
                 ("Linear Regression", "linear_regression"), ("**GBT Regressor (phục vụ)**", "gbt")]
], widths=[7, 2.5, 2.5, 2, 3])
BL([
    f"GBT giảm MAE {(1-r0['gbt']['mae']/r0['naive_mean']['mae']):.0%} so với naive; signed error gần 0. R² khoảng 0,22 phản ánh giới hạn khi chỉ biết A, B và thời điểm.",
    "Baseline tốc độ lịch sử tệ hơn naive; nhóm giữ nguyên, không chỉnh cho đẹp (giả thuyết chưa đo: đường thẳng ngắn hơn đường thật, tốc độ zone gồm cả lúc xe đứng chờ).",
    f"**Nhãn nhiễu GPS:** bỏ chuyến có bước nhảy khỏi tập test làm MAE GBT giảm {f(r0['gbt']['mae'],2)} → {f(ML['results']['train_all -> test_no_gps_jump']['gbt']['mae'],2)}; số báo cáo chính là bản không lọc.",
    f"**Kiểm tra leakage tự động (script dừng nếu vi phạm):** train muộn nhất {LK['max_train_start_local']}, test sớm nhất {LK['min_test_start_local']}, {LK['trip_uid_in_both_train_and_test']} chuyến ở cả hai tập, tốc độ lịch sử chỉ từ đoạn GPS trước {LK['train_cutoff_local']}, feature thuộc danh sách cho phép.",
    f"**Khoảng ước lượng 80% (mở rộng):** hiệu chỉnh trên sai số tháng 04/2014 của model chỉ học dữ liệu trước đó; đo trên test chứa **{IV['test_coverage']:.2%}** chuyến thật, độ rộng trung vị {f(IV['median_width_min'],1)} phút.",
])
gv, gt = GF["validation"]["results"], GF["test"]["results"]
P(f"**Feature thời gian theo đồ thị (đã hứa ở Session 9):** thêm graph_time_min, graph_km, graph_found; đồ thị **chỉ xây từ train**, chuyến huấn luyện dùng đồ thị của các tuần khác (5 nhóm tuần, out-of-fold). Quyết định trên validation 04/2014: MAE GBT {f(gv['base_6_features']['gbt']['mae'],3)} → {f(gv['with_graph_features']['gbt']['mae'],3)} nên giữ; trên test {f(gt['base_6_features']['gbt']['mae'],3)} → {f(gt['with_graph_features']['gbt']['mae'],3)}, R² {f(gt['base_6_features']['gbt']['r2'],3)} → {f(gt['with_graph_features']['gbt']['r2'],3)}. Vì chỉ cải thiện khoảng 2%, nhóm **giữ model 6 feature để phục vụ**; model có feature đồ thị đã lưu, đưa vào phục vụ là hướng phát triển [P].")

# =========================================================== 8. SERVING
H("8. NoSQL serving, tìm tuyến, giao diện và REST API")
L = G["latency_point_read"]
BL([
    "**Vì sao MongoDB (không phải vì \"nhanh hơn\"):** giao diện cần tra cứu theo khóa (zone, giờ, loại ngày), cạnh đi ra từ một zone, cửa sổ live mới nhất; Parquet không có index tra một dòng. Mô hình **document** hợp với bản ghi tổng hợp có khóa tự nhiên; batch và streaming cùng ghi vào **collection riêng**; upsert theo _id cố định nên ghi lại không sinh bản trùng.",
    f"**Index và bằng chứng:** (zone_id, hour, is_weekend) unique, (zone_from), (gx, gy), (window_end, zone_id). explain(): **IXSCAN**, 1 tài liệu đọc; buộc quét toàn bộ: COLLSCAN, {f(G['explain_point_query']['forced_collection_scan']['totalDocsExamined'])} tài liệu. Độ trễ đọc một điểm p50 **{f(L['with_index']['p50_ms'],3)} ms** (quét toàn bộ {f(L['forced_collection_scan']['p50_ms'],1)} ms).",
    f"**Tìm tuyến:** Dijkstra trên đồ thị zone; bỏ cạnh dưới 5 lượt xe (không lọc thì {RT['thresholds']['1']['routes_using_edge_below_5_transitions']/RT['sample_trips']:.0%} tuyến mẫu đi qua cạnh có số đo vô lý); thời gian cạnh theo tốc độ zone × giờ gồm cả lúc dừng (tỉ lệ thời gian tuyến / thực tế tăng từ 0,34 lên 0,71).",
    "**Mức tắc trên tuyến:** tốc độ zone giờ đó so với tốc độ cả ngày của chính zone (< 0,7 tắc, < 0,9 đông). **Lớp live:** cửa sổ live đã đóng gần nhất so với lịch sử cùng zone, cùng giờ; tỉ lệ < 0,7 thì viền đỏ.",
    "**Giao diện Streamlit:** 3 cách nhập (15 địa điểm có sẵn đã kiểm bằng dữ liệu, 20 chuyến mẫu tập test để so dự đoán với thực tế, chọn trên bản đồ thật); tuyến vẽ trên lưới zone dạng sơ đồ.",
    f"**REST POST /estimate (FastAPI):** dùng chung hàm estimate() với giao diện, kiểm tra thứ tự feature khi khởi động; độ trễ p50 **{f(API['latency_ms']['p50'],2)} ms**, p95 {f(API['latency_ms']['p95'],2)} ms (model GBT dự đoán một vector trực tiếp trên driver, không chạy job Spark).",
    f"**F3 (chạy thật từ CSV đến estimate):** 20 chuyến tập test đọc lại từ dòng gốc trong train.csv; ví dụ chuyến {F3['trips'][0]['trip_id']}: thực tế {f(F3['trips'][0]['actual_duration_min'],2)} phút, dự đoán {f(F3['trips'][0]['predicted_duration_min'],2)} phút. Qua API: MAE {f(API['f3_via_api']['mae_min'],2)} phút, {API['f3_via_api']['actual_in_interval']}/20 chuyến nằm trong khoảng 80% (minh họa; số chính thức là MAE trên toàn tập test).",
])
IMG(E / "screenshots/ui_sample_trip_interval.jpg", 13, "Hình 2. Trang ước lượng: chuyến mẫu tập test, dự đoán 8 phút (khoảng 4–12), thực tế 7,8 phút, tuyến trên lưới zone.")

# =========================================================== 9. STREAMING
H("9. Streaming: Kafka và Spark Structured Streaming")
BL([
    "**Replay producer:** đọc raw CSV, phát các điểm GPS của 14/05/2014 16:00–19:00 (giờ Porto) theo thứ tự thời gian sự kiện, tốc độ ×60 (1 giây thật = 1 phút dữ liệu); mỗi message là một điểm kèm điểm trước đó; acks=all, key = TAXI_ID. Cố ý chèn 1% bản trùng, 0,5% bản trễ 30 phút, 0,2% JSON hỏng; ghi số thật vào manifest.",
    "**Kafka 3.9 KRaft một node**, topic gps-segments **3 partition**, tạo trước producer và consumer. Key TAXI_ID giữ thứ tự theo từng xe (Kafka chỉ đảm bảo thứ tự trong một partition) và chia tải đều. Lý do dùng Kafka **không phải throughput** (khoảng 4 tin/giây ngoài đời): tách producer khỏi consumer, đọc lại theo offset và checkpoint, nhiều consumer độc lập (2 query cùng đọc một topic).",
    "**Structured Streaming:** query quality đếm JSON hỏng → stream_quality; query live: parse → withWatermark(10 phút) → dropDuplicatesWithinWatermark(trip_id, seq) → lọc bbox và tốc độ → window(15 phút, trượt 5 phút) × zone → foreachBatch upsert MongoDB (_id = zone|window_start), chế độ append (chỉ cửa sổ đã đóng). Trigger 5 giây, có checkpoint, maxOffsetsPerTrigger 5.000.",
])
T(["Đối soát", "Producer (manifest)", "Spark / Mongo"], [[r["item"], f(r["producer_manifest"]), f(r["spark_or_mongo"])] for r in ST["table"]], widths=[8, 4.5, 4.5])
P(f"**Bản trễ lọt qua ({ST['late_not_dropped']}) đã được giải thích bằng số đo:** dữ liệu đúng giờ dừng ở {LATE['max_on_time_event_local']} nên watermark dừng ở {LATE['max_watermark_before_sentinel_local']}; cả {LATE['late_with_event_after_that_watermark']} bản trễ lọt qua đều có giờ sự kiện sau mốc này, tức vẫn nằm trong watermark, đúng cam kết của Spark. Cả {LATE['of_which_pass_filter']} bản qua bộ lọc nên số đoạn tổng hợp nhiều hơn kỳ vọng đúng {ST['late_not_dropped']}.")
P("**Trang \"Giám sát streaming\"** cho thấy trực tiếp lúc demo: đồng hồ dữ liệu và watermark, số tin nhận / hỏng / trùng / trễ tăng dần, bản đồ cửa sổ live toàn thành phố, nhật ký cửa sổ vừa đóng, và thanh xem lại mọi cửa sổ đã lưu.")
IMG(E / "screenshots/ui_streaming_monitor_map.jpg", 12, "Hình 3. Trang giám sát streaming trong lúc chạy: cửa sổ 16:35–16:50 có 231 zone có dữ liệu, 38 zone chậm bất thường (đỏ).")

# =========================================================== 10. TRIỂN KHAI [P]
H("10. Thiết kế triển khai cluster / cloud [P]")
P("Bản final chạy toàn bộ bằng Docker Compose trên **một máy** (12 vCPU, 8 GB RAM cho Docker, profile mặc định / hadoop / streaming). Phần dưới là **thiết kế** khi lên cụm hoặc cloud, chưa triển khai.")
T(["Thành phần", "Thiết kế khi lên cụm / cloud", "Lý do"], [
    ["Lưu trữ", "Data lake Parquet trên object storage (S3 hoặc tương đương), tách khỏi máy tính toán; HDFS chỉ khi chạy Hadoop on-premise", "Tách compute và storage: tăng giảm số máy xử lý mà không di chuyển dữ liệu; bền vững, rẻ"],
    ["Spark batch", "Spark on Kubernetes hoặc YARN, driver ở cluster mode, dynamic allocation; executor tăng theo số partition (silver ~80 triệu đoạn)", "Mở rộng ngang; tài nguyên chỉ cấp khi có job ETL / huấn luyện"],
    ["Lịch chạy", "Airflow: ETL bronze → gold hằng ngày, huấn luyện lại model hằng tháng, tính lại khoảng 80% và kiểm tra leakage trước khi đổi model phục vụ", "Đúng chu kỳ batch trên sơ đồ; tự động hóa thay chạy tay"],
    ["Kafka", "3 broker, replication factor 3, min.insync.replicas 2; số partition tăng theo số xe và số consumer song song", "Chịu được mất 1 broker không mất tin; key TAXI_ID giữ thứ tự từng xe"],
    ["Structured Streaming", "Chạy dài hạn trên cluster, checkpoint trên object storage; cảnh báo khi consumer lag tăng", "Khôi phục từ checkpoint sau sự cố; sink upsert idempotent"],
    ["MongoDB", "Replica set 3 node, write concern majority (đứng về phía CP trong CAP); shard zone_speed_live theo window_end nếu dữ liệu live lớn", "Đọc phục vụ nhẹ, cần nhất quán khi có node chết"],
    ["Serving", "REST API stateless chạy nhiều bản sao sau load balancer; model và features.json tải từ object storage lúc khởi động", "Mở rộng theo số người dùng; API tự kiểm tra thứ tự feature"],
    ["Giám sát", "Spark History Server, Prometheus + Grafana (độ trễ API, consumer lag, số tin hỏng), log tập trung", "Phát hiện sớm sai lệch dữ liệu và model"],
    ["Bảo mật / chi phí", "TLS, IAM theo vai trò, secret không nhúng trong code; batch chạy máy spot, tắt khi không dùng", "Giảm rủi ro và chi phí"],
], widths=[3, 7.5, 6.5], size=8.5)

# =========================================================== 11. LÝ THUYẾT
H("11. Liên hệ với lý thuyết môn học")
T(["Lý thuyết (buổi học)", "Áp dụng trong dự án"], [
    ["HDFS: block, replication, data locality (buổi 1)", f"File 1.942.911.619 byte chia block 128 MB → 15 block → **15 map task** (đo được trên YARN): mỗi map task xử lý một block, chạy gần dữ liệu. Replication = 1 vì một node; trên cụm đặt 3 để chịu lỗi."],
    ["MapReduce, shuffle/sort, combiner (buổi 1–2)", "Key \"zone_id,hour\"; combiner hợp lệ vì reducer chỉ cộng (kết hợp, giao hoán); combiner giảm khoảng 80 triệu → 1,35 triệu bản ghi trước shuffle."],
    ["Kiến trúc Spark: driver, executor, job, stage, task, lazy evaluation (buổi 3)", "local[*] 12 lõi: driver và executor cùng JVM. Mỗi action tạo job; Exchange (shuffle) chia job thành stage; số task bằng số partition (48 shuffle partition). Transformation chỉ chạy khi có action (count, write)."],
    ["DataFrame, Spark SQL, temp view, query plan (buổi 3–4)", "Logic tầng viết bằng DataFrame API; Q1–Q3 bằng Spark SQL trên temp view; EXPLAIN FORMATTED chỉ ra Exchange, loại join, PushedFilters."],
    ["Parquet, partitioning, bố trí file (buổi 3–4, 9)", "Data lake 4 tầng Parquet; chọn không chia partition cho bảng gold nhỏ; persist DISK_ONLY cho silver/segments thay vì cache toàn bộ (8 GB RAM)."],
    ["Caching và broadcast (buổi 9)", "Thí nghiệm broadcast vs sort-merge: 0 MB so với 51,3 MB shuffle."],
    ["NoSQL: key-value, document, wide-column, graph; CAP; access pattern (buổi 5)", "Chọn document (MongoDB) cho bản ghi tổng hợp có khóa tự nhiên và index phụ; key-value thiếu index phụ; wide-column (Cassandra, thiên về AP) hợp khi ghi rất lớn; graph DB không cần vì Dijkstra chạy trong bộ nhớ."],
    ["Spark ML: transformer, estimator, evaluation (buổi 5–6)", "VectorAssembler (transformer) → GBTRegressor / LinearRegression (estimator) → đánh giá MAE, RMSE, R²; temporal hold-out."],
    ["Kafka, Structured Streaming: source/sink, checkpoint, event time, state (buổi 7)", "Kafka source, foreachBatch sink idempotent, checkpoint, watermark trên event time, state của bỏ trùng và cửa sổ trượt."],
    ["Triển khai cluster/cloud, compute–storage separation (buổi 7, 9)", "Mục 10 (thiết kế [P])."],
], widths=[5.5, 11.5], size=8.5)

# =========================================================== 12. KIỂM THỬ
H("12. Kiểm thử và tái lập")
BL([
    "**37 unit test** chạy trong Docker: hợp đồng bronze (thứ tự lý do, ranh giới 180 phút, lon trước lat, giờ Lisbon), silver/gold (không lọc bbox trước lag, TRIP_ID trùng, cạnh liền kề, cộng dồn không lấy trung bình của trung bình), mapper/combiner/reducer khớp Spark, tìm tuyến, đánh giá và leakage, khớp feature giữa phục vụ và huấn luyện (500 chuyến), ground truth của producer, REST API, feature đồ thị out-of-fold.",
    "**Phiên bản cố định:** Python 3.12, JDK 17, PySpark 4.0.1, Hadoop 3.5.0, MongoDB 8.0, Kafka 3.9.1; seed 42, PYTHONHASHSEED 42, Spark session UTC; cấu hình tập trung conf/pipeline.yaml.",
    "**Mỗi bước một script:** check_env, run_eda, run_pipeline, run_mapreduce, run_ml, run_demo, run_streaming (sample trước, full sau). README hướng dẫn chạy lại từ dữ liệu thô; dữ liệu, model, checkpoint không commit (repo khoảng 3 MB).",
    "**Bằng chứng:** thư mục evidence/ (A–H, streaming, red flag, ảnh chụp giao diện, YARN UI, HDFS UI, Spark UI).",
])

# =========================================================== 13. GIỚI HẠN
H("13. Giới hạn và hướng phát triển")
T(["Giới hạn", "Tác động", "Hướng xử lý"], [
    ["R² khoảng 0,22–0,24", "ETA chỉ chính xác ở mức khiêm tốn", "Thêm thông tin trước khi xuất phát (thời tiết, sự kiện), map-matching với mạng đường thật"],
    ["Khoảng 10% nhãn bị đếm thiếu do GPS hụt điểm", "Nhãn nhiễu", "Đã gắn cờ và báo cáo cả có lọc / không lọc"],
    ["Tuyến ở mức zone 420 × 555 m", "Không phải tuyến đường phố", "Map-matching OpenStreetMap"],
    ["Một máy, Hadoop chạy giả lập amd64", "Không chứng minh khả năng mở rộng trên cụm", "Thiết kế ở mục 10 [P]"],
    ["Dữ liệu 2013–2014, streaming là replay", "Không phản ánh giao thông hiện tại; không phải real-time", "Nguồn GPS live thật [P]"],
    ["Zone \"chậm\" có thể là xe chờ khách", "Diễn giải sai vùng tắc", "Mức tắc so với chính zone; ghi chú zone sân bay"],
], widths=[5, 5, 7], size=9)

# =========================================================== 14. SO VỚI SESSION 9
H("14. Thay đổi so với báo cáo Session 9")
T(["Hạng mục", "Session 9", "Final"], [
    ["Model phục vụ", "sklearn GBT học trên split ngẫu nhiên (F3 có thể đã thấy chuyến test)", "Spark GBT chỉ học giai đoạn train, F3 ngoài mẫu"],
    ["Đánh giá", "Bảng chính dùng split ngẫu nhiên; mốc chia UTC; 5 feature", "Chỉ temporal hold-out; mốc giờ Lisbon; 6 feature (thêm is_holiday)"],
    ["MapReduce", "Lệch Spark 847 chuyến (mapper không lọc > 180 phút)", "Lọc giống hệt, khớp 40.548 / 40.548 key"],
    ["TRIP_ID trùng", "Ghi nhận là gap", "Sửa bằng trip_uid, đối soát số đoạn khớp tuyệt đối"],
    ["Streaming", "Tumbling 15 phút, update mode, consumer lag", "Trượt 15/5 phút, append (chỉ cửa sổ đã đóng), không lag, có trang giám sát"],
    ["Giao diện", "FastAPI + Leaflet", "Streamlit (theo sơ đồ đã duyệt) + FastAPI POST /estimate"],
    ["Top 3 việc đã hứa", "(1) xử lý nhãn nhiễu, (2) feature đồ thị chỉ từ train, (3) phân công + ảnh chụp", "Cả ba đã làm"],
], widths=[3.5, 6.5, 7], size=8.5)
P("Chi tiết từng con số và lý do thay đổi: docs/CHANGES_SINCE_SESSION9.md. Số MAE giữa hai bản không so trực tiếp được vì khác thư viện, feature và mốc chia; cả hai đều tốt hơn naive khoảng một phần tư.", size=9.5)

# =========================================================== 15. RED FLAG
H("15. Red flag (tất cả phải trả lời KHÔNG)")
T(["Câu hỏi", "Trả lời", "Bằng chứng"], [
    ["Có công nghệ không giải thích được vai trò?", "Không", "Mục 3 và 9; MapReduce chỉ đối chiếu, Kafka không vì throughput"],
    ["Có gọi bản replay là real-time?", "Không", "Ghi rõ \"phát lại ×60\" ở báo cáo, giao diện, evidence"],
    ["Có nhãn tính từ chính feature?", "Không", "Feature chỉ dùng A, B, thời điểm; kiểm tra tự động danh sách feature"],
    ["Chia dữ liệu hoặc feature lộ tương lai?", "Không", "Temporal hold-out; baseline và đồ thị chỉ từ train; tham số cố định trước"],
    ["Metric sai loại bài toán?", "Không", "Hồi quy: MAE, RMSE, R², signed error"],
    ["Chọn NoSQL chỉ vì nhanh?", "Không", "Theo access pattern, hai luồng ghi riêng, upsert idempotent"],
    ["Không có thí nghiệm hiệu năng?", "Không (có)", "Broadcast vs sort-merge, 7 lần đo"],
    ["Không tái lập được?", "Không (tái lập được)", "Script mỗi bước, phiên bản cố định, 37 test"],
    ["Lẫn phần đã làm và phần đề xuất?", "Không", "Nhãn [I]/[P] trên sơ đồ và mọi bảng"],
], widths=[6.5, 2.5, 8], size=9)

# =========================================================== 16. TỰ CHẤM
H("16. Tự đánh giá (6 tiêu chí của Final Assessment)")
T(["Tiêu chí", "Trọng số", "Tự chấm", "Bằng chứng", "Khoảng trống"], [
    ["Accuracy & Correctness", "25", "23", "MapReduce ↔ Spark khớp 40.548 key; số đoạn khớp tuyệt đối; đối soát streaming khớp (bản trễ lọt qua đã giải thích bằng số đo); 37 test", "ETA khiêm tốn (MAE 4,21 phút, R² 0,22)"],
    ["Creativity & Problem-Solving", "15", "13", "Known-destination framing; phát hiện và sửa TRIP_ID trùng; mức tắc so với chính zone; khoảng ước lượng 80%; trang giám sát streaming", "Tuyến ở mức zone, chưa theo mạng đường"],
    ["Completeness of Product", "25", "24", "Đủ mọi thành phần bắt buộc chạy thật: MapReduce/YARN, Spark SQL, ML, MongoDB, Kafka streaming, giao diện, REST API, evidence A–H", "Cluster/cloud chỉ thiết kế"],
    ["Presentation & Expression", "10", "9", "Sơ đồ [I]/[P], báo cáo, demo 2 trang giao diện, ảnh chụp", "Báo cáo hoàn thiện sát hạn"],
    ["Application of Theory", "20", "18", "Combiner, HDFS block ↔ map task, Exchange/broadcast, event time/watermark, CAP, temporal hold-out (mục 11)", "CAP và replication mới ở mức thiết kế"],
    ["Timeliness & Requirements", "5", "5", "Nộp đúng hạn; bám tài liệu yêu cầu và checklist", ""],
    ["**Tổng**", "**100**", "**92**", "", ""],
], widths=[3.3, 1.5, 1.5, 6.7, 4], size=8.5)
P("**Ba việc nên làm tiếp:** (1) đưa model có feature đồ thị vào phục vụ (cần đồ thị chỉ từ train lúc dự đoán và hiệu chỉnh lại khoảng 80%); (2) chạy thử trên cụm nhiều node để đo khả năng mở rộng thật; (3) map-matching với mạng đường OpenStreetMap để có tuyến và feature quãng đường chính xác hơn.")

# =========================================================== 17. Q&A
H("17. Câu hỏi dự kiến")
T(["Câu hỏi", "Trả lời dựa trên bằng chứng"], [
    ["Điểm đến B lấy từ đâu lúc dự đoán?", "Người dùng nhập lúc đặt xe; lúc huấn luyện B là điểm GPS cuối. Vì vậy bài toán là known-destination ETA."],
    ["Hệ thống có real-time không?", "Không. Kafka chở bản phát lại ×60 ngày 14/05/2014; streaming chỉ đánh dấu zone, ETA do model batch. Nguồn live thật là [P]."],
    ["Sao cần MapReduce khi Spark làm được?", "Để đối chiếu độc lập: khớp 40.548 / 40.548 key; combiner giảm khoảng 98% bản ghi trước shuffle. Không nằm trên đường phục vụ."],
    ["R² 0,22 thì model có ích không?", "Có so với baseline: MAE 4,21 so với 5,55 (naive) trên 2 tháng model chưa thấy; giới hạn do chỉ biết A, B, thời điểm."],
    ["Làm sao chắc không leakage?", "Chia theo thời gian; script tự kiểm tra và dừng nếu vi phạm; baseline, đồ thị, khoảng 80% chỉ từ dữ liệu trước mốc chia; tham số cố định, test chỉ dùng một lần."],
    ["Sao 3 partition Kafka?", "Theo thiết kế nhóm, để minh họa đọc song song và giữ thứ tự từng xe (key TAXI_ID); lưu lượng thật không đòi hỏi nhiều partition."],
    ["Sao có bản trễ lọt qua watermark?", "15 bản có giờ sự kiện sau 18:49:59; dữ liệu đúng giờ dừng ở 18:59:59 nên chúng vẫn trong watermark, Spark nhận đúng cam kết."],
], widths=[5, 12], size=9)

out = R / "report" / "BDA501_Final_Report_PortoTaxi_G04.docx"
out.parent.mkdir(exist_ok=True)
doc.save(out)
print(out)
