"""Pecah CSV bulanan jadi satu file per tanggal di data/landing/date=YYYY-MM-DD/.
Sekali jalan, di luar DAG. Output-nya yang nanti diupload ke Databricks Volume.
"""
import shutil
from pathlib import Path

import pandas as pd

SRC = Path("data/raw/2019-Oct.csv")
DEST = Path("data/landing")
CHUNK = 1_000_000

# ponytail: hapus-ulang biar idempotent. landing = data turunan, aman dibuang.
if DEST.exists():
    shutil.rmtree(DEST)

seen = set()
total = 0

# dtype=str: landing zone itu passthrough mentah, tipe data urusan bronze.
for chunk in pd.read_csv(SRC, chunksize=CHUNK, dtype=str):
    total += len(chunk)
    for date, part in chunk.groupby(chunk["event_time"].str[:10]):
        out = DEST / f"date={date}"
        out.mkdir(parents=True, exist_ok=True)
        part.to_csv(
            out / "events.csv", mode="a", header=date not in seen, index=False
        )
        seen.add(date)
    print(f"{total:,} baris, {len(seen)} tanggal", end="\r")

print(f"\nselesai: {total:,} baris -> {len(seen)} tanggal")
assert len(seen) == 31, f"harusnya 31 tanggal Oktober, dapet: {sorted(seen)}"