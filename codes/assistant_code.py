# codes/assistant_code.py (RAG enrichi)
import os
import glob
import time
import subprocess
import webbrowser
import requests
from pathlib import Path

# ==================== RAG ENRICHIE ====================
RAG_KNOWLEDGE = """
# Mappings PySpark → Dask (Dask Docs + Coiled Guide)
spark.read.csv(path) → dd.read_csv(path)
df.filter(condition) → df[condition]
df.select(cols) → df[cols]
df.withColumn("new", expr) → df.assign(new=expr)  # ou df.with_columns pour multi
df.groupBy("key").agg(sum("val")) → df.groupby("key")["val"].sum().compute()
df.join(other, "key") → df.merge(other, on="key", how="left")
F.when(cond, val).otherwise(x) → np.where(cond, val, x)  # import numpy as np
F.udf(func) → df.map_partitions(lambda part: part.apply(func))  # Éviter UDFs !
df.cache() → df.persist()  # Pour réutilisation sans recalcul
df.repartition(n) → df = df.repartition(npartitions=n)  # Gérer shuffle
df.corr() → df.corr().compute()  # Full shuffle, optimiser partitions

Pièges Coiled :
- Shuffle/groupBy : Repartition avant pour <100 partitions ; monitor avec dask dashboard.
- UDFs : Lent en sérialisation → Préférer vectorisé (pandas methods).
- Pas de SQL/optimizer : Écrire ops séquentielles ; termine TOUJOURS par .compute().
- Parallélisation : Utilise dask.distributed pour clusters ; local[*] pour tests.
- Performance : Dask 3x plus rapide sur ETL Python-native ; moins CPU que Spark.

Best practices : Persist() après read/join ; test sémantique avec small data ; benchmark temps/mémoire.
"""

EXTRACTED_DIR = "../data/extracted_scripts"
DATA_DIR = "../data"
CSV_FILES = glob.glob(f"{DATA_DIR}/*.csv")
PY_FILES = glob.glob(f"{EXTRACTED_DIR}/*_pyspark.py")

def translate_to_dask(pyspark_code: str):
    prompt = f"""{RAG_KNOWLEDGE}

Traduis en Dask DataFrame optimisé (Préserve sémantique, ajoute .compute(), gère hotspots).
Génère aussi : 1) Patch diff. 2) Test unit (assert shapes/vals). 3) Explication tuning.

Code PySpark:
{pyspark_code}

Code Dask + Patch/Test/Expli:"""
    print("\nPROMPT RAG → Colle dans Grok/Claude :\n" + "="*80 + "\n" + prompt + "\n" + "="*80)
    return prompt

def generate_benchmark_notebook():
    # Génère IPYNB basique pour benchmarks (ajoute tes runs)
    nb = {
        "cells": [{"cell_type": "markdown", "source": ["# Benchmarks PySpark vs Dask"]},
                  {"cell_type": "code", "source": ["# Ex: temps = %timeit df.compute()\n"]}],
        "metadata": {"kernelspec": {"name": "python3"}}
    }
    with open("benchmark_comparison.ipynb", "w") as f:
        import json; json.dump(nb, f)
    print("Benchmark IPYNB généré ! Ajoute tes profils.")

# Menu
if __name__ == "__main__":
    print("ASSISTANT DASK MIGRATION")
    print("1. Traduire code → Dask (RAG)")
    print("2. Profiler PySpark (hotspots)")
    print("3. Générer benchmark notebook")
    choice = input("Choix: ")
    if choice == "1":
        code = input("Colle PySpark: ")
        translate_to_dask(code)
    elif choice == "2":
        for f in PY_FILES[:2]:  # Test 2 d'abord
            # Intègre profile_single de Étape 4 ici si besoin
            pass
    elif choice == "3":
        generate_benchmark_notebook()