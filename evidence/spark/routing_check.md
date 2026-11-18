# Bộ lọc cạnh ít lượt khi tìm đường (số đo thật)

Quyết định đã duyệt: khi chạy Dijkstra, bỏ cạnh có dưới `routing.min_edge_transitions` lượt xe (mặc định 5, đặt 1 = tắt). Bảng gold `zone_graph_edges` vẫn giữ đủ mọi cạnh.

Nguồn: `routing_check.json` (`src/checks/check_routing.py`), mẫu 3,000 chuyến tập test (seed 42) có điểm đầu và cuối trong bbox. Đồ thị là bảng gold trên toàn bộ dữ liệu, dùng cho tìm đường trên giao diện, nên **đây không phải đánh giá model**.

| Ngưỡng lượt tối thiểu | 1 (tắt lọc) | 5 (đã chọn) | 20 |
|---|---|---|---|
| Cạnh dùng | 9,444 | 7,376 | 5,604 |
| Thành phần liên thông mạnh lớn nhất (zone) | 2,085 | 1,761 | 1,378 |
| Zone rơi khỏi thành phần chính | 0 | 324 | 707 |
| Chuyến mẫu tìm được tuyến | 3000 / 3000 | 3000 / 3000 | 2996 / 3000 |
| Tuyến đi qua ít nhất một cạnh < 5 lượt | **1,583** (53%) | 0 | 0 |

**Kết luận:**
- Không lọc thì **53% tuyến** đi qua ít nhất một cạnh có dưới 5 lượt xe. Dijkstra "thích" các cạnh này vì số đo hiếm hoi của chúng thường nhanh bất thường. Đây đúng là rủi ro đã nêu, và nó xảy ra ở hơn một nửa số tuyến.
- Ngưỡng 5: mọi chuyến mẫu vẫn tìm được tuyến. 324 zone rơi khỏi thành phần liên thông chính, nhưng các zone này chỉ dính tới 695 / 1.673.303 chuyến (0,042%), đều là vùng rìa.
- Ngưỡng 20: mất thêm zone và có 4 chuyến mẫu không còn tuyến, nên không chọn.

## Thời gian cạnh: đổi sang tốc độ zone × giờ (đã duyệt)

**Vấn đề ban đầu:** thời gian cạnh tính từ những đoạn cắt qua ranh giới zone, mà đoạn cắt ranh giới thì luôn là xe đang chạy (trung bình 49,8 km/h, so với 26,6 km/h của mọi đoạn kể cả lúc dừng). Vì vậy thời gian theo đồ thị ngắn hơn thực tế khoảng 3 lần.

**Cách mới** (`routing.edge_time: zone_hour_speed`): nửa quãng tâm-tâm đi với tốc độ lịch sử của zone đầu, nửa còn lại với tốc độ của zone cuối, tại đúng giờ và loại ngày (thường hoặc cuối tuần) của chuyến. Tốc độ là tổng km / tổng giờ, gồm cả đoạn dừng. Ô zone × giờ có dưới 20 đoạn thì lùi về tốc độ cả ngày của zone, rồi về tốc độ toàn thành phố theo giờ.

So sánh trên cùng 3,000 chuyến tập test, ngưỡng lọc 5 lượt:

| | Tốc độ trên cạnh (cũ) | Tốc độ zone × giờ (mới) |
|---|---|---|
| Chuyến tìm được tuyến | 3,000 | 3,000 |
| Thời gian tuyến, trung vị (phút) | 3.25 | 6.99 |
| Thời lượng thực tế, trung vị (phút) | 10.25 | 10.25 |
| Tỉ lệ tuyến / thực tế, trung vị | **0.34** | **0.71** |
| Như trên, bỏ chuyến có bước nhảy GPS | 0.33 | 0.69 |
| Tương quan Pearson tuyến với thực tế | 0.371 | 0.398 |

Mức lùi đã dùng khi tra tốc độ cho hai đầu cạnh (cộng dồn qua mọi giờ trong mẫu): zone × giờ 59.0%, zone cả ngày 37.5%, toàn thành phố 3.5%.

**Kết luận:**
- Tỉ lệ so với thực tế tăng từ 0.34 lên 0.71: đã sửa được phần lớn sai lệch do chỉ lấy xe đang chạy.
- Phần còn thiếu khoảng 29%: khoảng cách tâm-tâm ngắn hơn đường thật (đường vòng, một chiều), và chuyến thật có thể không đi tuyến ngắn nhất. Đây là giải thích hợp lý nhưng **chưa đo**.
- Tương quan chỉ tăng nhẹ (0.37 lên 0.40). Vì vậy **ETA vẫn lấy từ model (mục 7)**, không lấy thẳng tổng thời gian theo đồ thị. Thời gian theo đồ thị là ứng viên feature cho Phase 4, với đồ thị và tốc độ chỉ xây từ dữ liệu train.

## Phát hiện ban đầu (trước khi đổi): thời gian theo đồ thị ngắn hơn thực tế khoảng 3 lần

Trung vị của (thời gian tuyến theo đồ thị / thời lượng thực tế của chuyến) là **0.34** ở ngưỡng 5.

Nguyên nhân đo được: thời gian mỗi cạnh tính từ những đoạn **cắt qua ranh giới zone**, mà đoạn cắt qua ranh giới thì luôn là xe đang chạy. Tốc độ trung bình của các đoạn này là **49,8 km/h**, trong khi tốc độ trung bình của mọi đoạn (kể cả lúc dừng đèn, kẹt xe) là **26,6 km/h**. Các đoạn cắt ranh giới chỉ chiếm 26,1% tổng số đoạn. Ngoài ra, khoảng cách giữa hai tâm zone ngắn hơn đường đi thật.

**Hệ quả:** thứ tự tuyến (đi qua zone nào) vẫn dùng được, nhưng **không được dùng tổng thời gian theo đồ thị làm ETA**. ETA lấy từ model (mục 7). Đã xử lý ở mục trên.
