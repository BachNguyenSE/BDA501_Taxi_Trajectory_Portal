# Red flag: tất cả phải trả lời KHÔNG

| Câu hỏi | Trả lời | Bằng chứng |
|---|---|---|
| Có công nghệ nào không giải thích được vai trò? | **Không** | Bảng "Công nghệ và vai trò" trong `H_project_card.md`; Kafka nêu rõ lý do không phải throughput |
| Có gọi bản replay là real-time? | **Không** | Giao diện, `streaming/summary.md`, `H_project_card.md` đều ghi "phát lại ×60, không phải real-time"; nguồn live thật là mục 11 [P] |
| Có nhãn nào được tính từ chính feature? | **Không** | Nhãn = (số điểm − 1) × 15 giây; 6 feature chỉ dùng A, B và thời điểm; feature tính từ polyline bị cấm và kiểm tra tự động (`F_card.md` mục F1) |
| Cách chia dữ liệu hoặc feature có làm lộ thông tin tương lai? | **Không** | Temporal hold-out theo giờ Lisbon; train muộn nhất 30/04 23:59:47, test sớm nhất 01/05 00:00:52; baseline tốc độ lịch sử chỉ từ đoạn trước mốc chia; tham số cố định trước (`ml/leakage_checks_full.json`) |
| Metric có sai loại bài toán? | **Không** | Hồi quy thời gian (phút): MAE, RMSE, R², signed error |
| Chọn NoSQL chỉ vì "nhanh hơn"? | **Không** | Lý do là mẫu truy vấn theo khóa, hai luồng cùng ghi collection riêng, upsert idempotent; tốc độ chỉ là số đo kèm theo (`mongo/G_nosql.md`) |
| Không có kết quả thí nghiệm hiệu năng? | **Không** (có) | `spark/B_benchmark.md`: 7 lần đo mỗi cách, shuffle lấy từ Spark REST, kiểm tra loại join trên plan cuối |
| Không tái lập được kết quả? | **Không** (tái lập được) | Mỗi bước có script (`scripts/run_*.sh`), seed và phiên bản cố định, cấu hình tập trung, 30 unit test |
| Có lẫn phần đã làm với phần chỉ đề xuất? | **Không** | Nhãn [I]/[P] trên sơ đồ và trong `A_architecture.md`; mọi số liệu có file nguồn; số tham chiếu lần trước ghi rõ là không dùng làm kết quả |

Điểm cần lưu ý khi trình bày: ảnh YARN UI là của job sample (đã ghi chú); `holidays.csv` do nhóm tự tổng hợp, chưa đối chiếu nguồn chính thức; model phục vụ chọn trước khi chạy (GBT trên toàn bộ train).
