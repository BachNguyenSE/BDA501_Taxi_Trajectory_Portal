# Phase 2: silver, gold, Spark SQL (số đo thật)

Chạy bằng `scripts/run_pipeline.sh --sample` rồi `--full`. Unit test: 10 test qua (`tests/`).

## Silver và gold

| Bảng | Số dòng |
|---|---|
| bronze, dòng hợp lệ | 1,673,305 |
| silver/trips | 1,673,303 |
| silver/segments | 79,502,899 |
| gold/zones | 2,142 |
| gold/zone_hour_speed | 72,142 |
| gold/zone_graph_edges | 9,444 |
| gold/trip_features | 1,673,303 |

**Đối soát số đoạn** (đếm độc lập bằng mảng, không qua window):
tổng cặp điểm 80,775,358, bỏ vì ngoài bbox 1,073,615, bỏ vì quá 150 km/h (trong bbox) 198,844, còn lại kỳ vọng 79,502,899. Bảng silver có 79,502,899 đoạn: **khớp**.

**Hai lỗi dữ liệu đã phát hiện và xử lý trong phase này:**
1. `TRIP_ID` **không duy nhất**: 9 chuyến dùng chung ID với chuyến khác (cùng xe, cùng giây xuất phát). Window `lag` theo `trip_id` đã ghép điểm của hai chuyến khác nhau. Cách sửa: thêm khóa `trip_uid` = `TRIP_ID` + md5 của polyline. Không xóa chuyến nào.
2. **Chuyến bị ghi lặp:** `TRIP_ID 1397172149620000454` có 3 dòng cùng quỹ đạo (một dòng `CALL_TYPE=A`, hai dòng `C` giống hệt nhau). Silver giữ một bản (chọn cố định theo `call_type`), bỏ 2 dòng. Bronze vẫn giữ đủ cả 3. Đây là **quyết định làm sạch của nhóm, cần nêu trong báo cáo**.

Null ở cột feature của `trip_features`: 0. `origin_zone` null 1,230 dòng và `dest_zone` null 9,481 dòng, do điểm đầu hoặc cuối nằm ngoài bbox. Chuyến rơi vào ngày lễ: 46,002.

Chia theo thời gian (giờ Lisbon): train 1,364,636, test 308,543, ngoài khoảng (01/07/2014) 124.

Thời gian chạy full: silver trips 25.4 s, silver segments 22.7 s, gold 19.7 s.

## Spark SQL

**Q1, 10 zone chậm nhất giờ cao điểm ngày thường** (chỉ zone có ít nhất 1.000 đoạn; 8-9h và 17-19h là giả định):

| zone_id | tâm (lat, lon) | km/h | số đoạn |
|---|---|---|---|
| 2685 | 41.2425, -8.6725 | 1.65 | 4,822 |
| 2501 | 41.2275, -8.5425 | 3.80 | 4,277 |
| 1230 | 41.1375, -8.5975 | 5.54 | 11,674 |
| 2686 | 41.2425, -8.6675 | 5.94 | 3,506 |
| 741 | 41.1025, -8.5925 | 6.65 | 1,486 |
| 2557 | 41.2325, -8.6125 | 7.51 | 2,401 |
| 1950 | 41.1875, -8.4975 | 7.53 | 1,936 |
| 2502 | 41.2275, -8.5375 | 7.94 | 1,352 |
| 2335 | 41.2175, -8.6725 | 8.13 | 1,105 |
| 2219 | 41.2075, -8.5525 | 8.46 | 2,173 |

Cần đọc kỹ: zone chậm nhất 2685 có tâm gần khu sân bay Porto theo tọa độ, với tốc độ 1.65 km/h. Đây có thể là **xe xếp hàng chờ khách** chứ không phải tắc đường. Đây là giả thuyết, chưa kiểm chứng. Nếu đúng, các zone có điểm đỗ taxi sẽ bị coi là "tắc", và việc này ảnh hưởng tới phần "vùng tắc" trên giao diện (xem đề xuất bên dưới).

**Q2, tốc độ theo giờ:** ngày thường chậm nhất lúc 18h (19.9 km/h), 17h (21.5 km/h), 19h (22.3 km/h), nhanh nhất lúc 5h (40.9 km/h). Buổi sáng 8-9h (khoảng 24,2 km/h) chỉ chậm hơn buổi trưa một chút, nên giả định giờ cao điểm sáng trong Q1 **không được Q2 ủng hộ mạnh**. Giờ cao điểm rõ nhất là 17-19h.

**Q3, join chuyến với zone × giờ tại điểm xuất phát (chỉ tập train):** zone xuất phát càng chậm thì thời gian trên mỗi km đường thẳng càng lớn. Ở khoảng 5 km/h là 4.61 phút/km, ở 30 km/h là 3.21 phút/km. Các khoảng trên 60 km/h có ít chuyến nên nhiễu.

Plan và giải thích: `C_query_plan_explained.md`. Thí nghiệm hiệu năng: `B_benchmark.md`.

## Hồ sơ nhiễu (chỉ đo, chưa đặt ngưỡng)

Các mốc dưới đây chỉ là mốc để đo, **chưa phải ngưỡng lọc**. Ngưỡng sẽ đề xuất ở Phase 4, chọn trên tập train.

| Nhóm | Số chuyến | Tỉ lệ / 1,673,303 |
|---|---|---|
| Có ít nhất một bước nhảy > 150 km/h | 174,431 | 10.42% |
|   trong đó đúng 1 bước nhảy | 159,492 | 9.53% |
|   2-5 bước nhảy | 13,729 | 0.82% |
|   trên 5 bước nhảy | 1,210 | 0.07% |
| Đường thẳng A-B < 0,1 km | 32,649 | 1.95% |
| Tốc độ theo đường thẳng < 1 km/h | 38,802 | 2.32% |
| Tốc độ theo đường thẳng > 100 km/h | 2,121 | 0.13% |
| Thời lượng < 1 phút | 15,593 | 0.93% |
| Điểm đầu hoặc cuối ngoài bbox | 10,095 | 0.60% |

**Câu hỏi để lại từ Phase 1:** bước nhảy có làm hỏng nhãn không? 91% chuyến có nhảy chỉ nhảy đúng 1 lần. Chuyến có nhảy có thời lượng trung vị 10.75 phút, gần bằng chuyến không nhảy (10.25 phút). Nhưng số phút trên mỗi km đường thẳng thấp hơn (3.04 so với 3.78). Điều này **phù hợp** với giả thuyết mất tín hiệu GPS làm thiếu điểm, nên nhãn bị đếm thiếu thời gian. Số liệu chưa đủ để kết luận chắc chắn.

## Đề xuất ngoài đề, chờ duyệt (chưa làm)

1. **Phân biệt "chờ khách" với "tắc đường"** khi tô vùng tắc: ví dụ loại các đoạn đứng yên tại điểm đầu chuyến khỏi `zone_hour_speed`. Hiện em làm đúng đề (mọi đoạn trong bbox và ≤ 150 km/h).
2. **Đặt mức tối thiểu số lần đi qua cho cạnh đồ thị** khi chạy Dijkstra. Hiện cạnh chậm nhất mất 6656 giây cho một lần đi qua zone kề, nhiều khả năng do quá ít dữ liệu trên cạnh đó.

## Cập nhật sau khi đo thêm (đã duyệt)

- Đề xuất 1 (tách chờ khách khỏi tắc đường): **rút lại**. Zone 2685 có 91,9% đoạn đứng yên nhưng chỉ 1,1% nằm trong 2 phút đầu chuyến, nên bỏ đoạn đứng yên đầu chuyến chỉ đổi tốc độ từ 1,65 lên 1,66 km/h. Giữ đúng đề và ghi chú trong báo cáo.
- Đề xuất 2 (cạnh ít lượt): **áp dụng** ngưỡng 5 lượt khi tìm đường. Xem `routing_check.md`.
