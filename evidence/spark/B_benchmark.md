# B. Thí nghiệm hiệu năng: broadcast hash join so với sort-merge join

Số đo thật, nguồn `B_benchmark_full.json`.

**Thiết lập:** cùng một câu lệnh join `trip_features` (1,673,303 dòng) với `zone_hour_speed` (72,142 dòng) theo `(zone, hour, is_weekend)`. Ghi ra sink `noop` để buộc tính toàn bộ mà không tốn thời gian ghi đĩa. Hai cách chạy xen kẽ, mỗi cách 1 lần khởi động (không tính) và 7 lần đo. Shuffle lấy từ Spark REST API (`/api/v1/applications/<id>/stages`), cộng các stage mới hoàn thành của từng lần chạy. Loại join được kiểm tra trên plan cuối sau khi chạy. Spark 4.0.1, `local[*]`, 48 shuffle partition.

| | Broadcast hash join | Sort-merge join |
|---|---|---|
| Join trong plan cuối (cả 7 lần) | BroadcastHashJoin | SortMergeJoin |
| Shuffle đọc (MB) | 0.00 | 51.30 |
| Shuffle ghi (MB) | 0.00 | 51.30 |
| Thời gian, trung vị (giây) | 0.239 | 0.286 |
| Thời gian, min đến max (giây) | 0.208 đến 0.282 | 0.258 đến 0.339 |
| 7 lần đo (giây) | 0.282, 0.249, 0.244, 0.208, 0.209, 0.216, 0.239 | 0.318, 0.339, 0.294, 0.260, 0.286, 0.258, 0.261 |
| Lần khởi động, không tính (giây) | 1.456 | 1.623 |

**Kết luận từ số đo:**
- **Shuffle:** khác biệt rõ và lặp lại y hệt ở cả 7 lần. Broadcast không shuffle (0 MB), sort-merge shuffle 51.3 MB vì phải phân vùng lại cả hai bảng.
- **Thời gian:** broadcast nhanh hơn khoảng 16% theo trung vị, nhưng khoảng min-max của hai cách **chồng lên nhau một phần**. Với khối lượng nhỏ cỡ này (dưới 0,4 giây mỗi lần), chênh lệch thời gian chỉ ở mức vừa phải.
- **Không suy rộng quá số đo:** đây là một máy ở chế độ local, shuffle chỉ là ghi và đọc đĩa cục bộ. Trên cụm thật, shuffle phải đi qua mạng nên chênh lệch có thể lớn hơn, nhưng đó là **suy luận, chưa đo**.
- Số tham chiếu lần trước (0,48 MB so với 36,55 MB) đo trên một query khác nên không so trực tiếp với kết quả này.
