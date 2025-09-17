# generate_data.py
import os
from pathlib import Path
import pandas as pd
import numpy as np
import duckdb

OUT = Path("data")
OUT.mkdir(parents=True, exist_ok=True)

def make_df(year, n=200, seed=None):
    rng = np.random.default_rng(seed)
    ids = np.arange(1, n+1)
    names = [f"user_{i}" for i in rng.integers(1, 1000, size=n)]
    start = pd.Timestamp(f"{year}-01-01")
    end = pd.Timestamp(f"{year}-12-31")
    dates = pd.to_datetime(rng.integers(int(start.timestamp()), int(end.timestamp()), size=n), unit='s')
    df = pd.DataFrame({
        "id": ids,
        "name": names,
        "date": dates
    })
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["year_month"] = df["date"].dt.strftime("%Y-%m")
    return df

# Génère 2024 et 2025
df2024 = make_df(2024, n=300, seed=42)
df2025 = make_df(2025, n=250, seed=7)

# Sauvegarde CSV d'exemple (upload)
sample_csv = Path("data/sample_upload.csv")
pd.concat([df2024.head(10), df2025.head(10)], ignore_index=True).to_csv(sample_csv, index=False)
print(f"Sample CSV écrit: {sample_csv}")

# Essaye d'écrire parquet via duckdb (si duckdb installé)
try:
    con = duckdb.connect()
    con.register("df2024_tmp", df2024)
    con.register("df2025_tmp", df2025)
    con.execute("COPY (SELECT * FROM df2024_tmp) TO 'output/2024_data.parquet' (FORMAT PARQUET)")
    con.execute("COPY (SELECT * FROM df2025_tmp) TO 'output/2025_data.parquet' (FORMAT PARQUET)")
    print("Parquet écrits via duckdb dans dossier output/")
except Exception as e:
    print("duckdb non disponible/raté pour écrire parquet :", e)