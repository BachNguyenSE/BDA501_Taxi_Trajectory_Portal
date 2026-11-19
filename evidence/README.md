# Mục lục bằng chứng (checklist Session 9)

Mọi số trong các file dưới đây là số đo thật từ lần chạy, kèm script tạo ra nó. Số tham chiếu của lần làm trước không được dùng làm kết quả.

| Hạng mục | File | Trạng thái |
|---|---|---|
| Thay đổi so với báo cáo Session 9 | `../docs/CHANGES_SINCE_SESSION9.md` | có |
| A. Kiến trúc | `A_architecture.md`, `../docs/architecture.png` | có |
| B. Thí nghiệm hiệu năng | `spark/B_benchmark.md`, `spark/B_benchmark_full.json`, ảnh `screenshots/spark_ui_sql_*` | có |
| C. Query plan | `spark/C_query_plan_full.txt`, `spark/C_query_plan_explained.md` | có |
| D. Kiểm tra dữ liệu | `D_data_validation.md` | có |
| E. Kiểm tra model | `ml/E_model_validation.md`, `ml/model_results_full.json` | có |
| E+. Feature thời gian theo đồ thị (đã hứa Session 9) | `ml/graph_features.md` | có |
| E+. Khoảng ước lượng 80% (mở rộng ngoài đề) | `ml/interval_validation.md` | có |
| F. Card F (F1, F2, F3) | `F_card.md`, `serving/F3_end_to_end.md` | có |
| REST API `/estimate` | `serving/api.md` | có |
| G. NoSQL | `mongo/G_nosql.md`, `mongo/G_nosql.json` | có |
| H. Project card | `H_project_card.md` | có |
| Streaming | `streaming/summary.md` | có |
| Red flag | `red_flags.md` | có |
| Tự chấm (mục 9) | `self_assessment.md` | **chờ 6 tiêu chí từ nhóm** |
| Phân công thành viên | `team.md` | **chờ thông tin từ nhóm** |
| Ảnh chụp | `screenshots/`: giao diện (5), giám sát streaming (4: đang chạy, bản đồ, đã dừng, xem lại), YARN UI (3, lần chạy sample), HDFS UI (1), Spark UI (5) | có |
| Unit test | `validation/phase*_tests.txt` (37 test ở lần chạy cuối) | có |

Ghi chú về ảnh: ảnh YARN UI là của job **sample**, chụp lại ở Phase 7 vì ResourceManager không giữ lịch sử job full sau khi khởi động lại. Bằng chứng job full là `mapreduce/job_full.log`, `mapreduce/yarn_apps_full.json`, `mapreduce/reconcile_full.json`. Thời gian trên ảnh Spark UI gồm cả lần chạy đầu nên không dùng làm số đo; số đo chính thức nằm ở `spark/B_benchmark.md`.
