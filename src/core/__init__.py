"""core: phần dùng chung cho mọi cụm.

common.py  đọc conf/pipeline.yaml, tạo SparkSession, tìm thư mục gốc dự án (ROOT), first_row
geo.py     hàm địa lý dạng biểu thức Spark: haversine, hướng (bearing), bbox, zone_id của lưới 0,005°
"""
