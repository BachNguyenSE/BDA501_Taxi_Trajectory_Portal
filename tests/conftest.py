"""Cho pytest import được các cụm trong src/ (core, batch, ml, serving, streaming, mapreduce, checks).
Trong Docker biến môi trường PYTHONPATH=/workspace/src đã làm việc này; file này để chạy pytest từ máy host."""
import pathlib
import sys

SRC = str(pathlib.Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
