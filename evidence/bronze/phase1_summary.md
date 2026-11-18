# Phase 1: bronze và EDA (số đo thật)

Nguồn: `evidence/bronze/reject_counts_full.json`, `evidence/eda/eda_full.json`, `eda_sample.json`.
Chạy bằng `scripts/run_eda.sh --sample` rồi `--full`; bản full chạy 22.2 giây. Unit test: `evidence/validation/phase1_tests.txt`.

## Bronze giữ mọi dòng

| reject_reason | Số dòng | Tỉ lệ |
|---|---|---|
| (hợp lệ) | 1,673,305 | 97.82% |
| too_few_points | 36,508 | 2.13% |
| too_long (> 180 phút) | 847 | 0.05% |
| missing_data | 10 | 0.0006% |
| bad_json | 0 | 0% |
| **Tổng** | **1,710,670** | 100% |

Tổng các lý do cộng với số dòng hợp lệ bằng đúng số dòng của `train.csv` (1.710.670), nên không dòng nào bị mất. Thứ tự ưu tiên khi một dòng vi phạm nhiều điều kiện: missing_data > bad_json > too_few_points > too_long.

## Chuyến hợp lệ

- Thời lượng (phút): min 0.25, p50 10.25, p90 20.25, p99 45.75, max 180.0.
- Khoảng thời gian (UTC): 2013-07-01 00:00:53 đến 2014-06-30 23:59:56.
- Chuyến theo tháng (giờ Lisbon): tháng 05/2014 có 158,617, tháng 06/2014 có 149,926. Có 124 chuyến rơi sang 07/2014 theo giờ địa phương (xuất phát cuối ngày 30/06 UTC), đúng với việc tính giờ theo `Europe/Lisbon`.

## Chất lượng GPS (chuyến hợp lệ)

- Chuyến có ít nhất một bước nhảy > 150 km/h: 174,431 / 1,673,305 = **10.42%**.
- Điểm GPS nằm ngoài bbox Porto: 1,072,092 / 82,448,673 = 1.30%.
- Mẫu 1% (seed 42) cho 10.21% chuyến có bước nhảy, sát với bản full.

## holidays.csv

10 dòng, ngày không trùng, tất cả nằm trong 01/07/2013 đến 30/06/2014: đạt. Danh sách tự tổng hợp, chưa đối chiếu nguồn chính thức.
