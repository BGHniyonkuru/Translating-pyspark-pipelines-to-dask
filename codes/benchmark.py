# benchmark.py (amélioré – parallel + Dask Client + mémoire/CPU)
import time, subprocess, glob, os, psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dask.distributed import Client
import multiprocessing as mp

scripts = [
    ("end-to-end", "extracted_scripts/end-to-end-pyspark-project_pyspark.py"),
    ("predict-sales", "extracted_scripts/predict-sales-spark-etl-eda_pyspark.py"),
    ("pakistan", "extracted_scripts/pyspark-pakistan-ecommerce-dataset-analysis_pyspark.py"),
]

dask_scripts = [
    "dask_translations/01_end_to_end.py",
    "dask_translations/02_udf_cleaning.py",
    "dask_translations/03_correlation.py",
]

def run_benchmark(file, tech):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024**2
    cpu_before = process.cpu_percent(interval=0.1)
    start = time.time()
    subprocess.run(["python", file], timeout=60)
    duration = time.time() - start
    mem_after = process.memory_info().rss / 1024**2
    cpu_after = process.cpu_percent(interval=0.1)
    return {
        "nom": os.path.basename(file).replace(".py", ""),
        "tech": tech,
        "temps": duration,
        "memoire": mem_after - mem_before,
        "cpu": cpu_after - cpu_before,
        "status": "OK"
    }

print("SPARK vs DASK BENCHMARK\n" + "="*50)

# Parallel benchmark for Spark
with ThreadPoolExecutor(max_workers=mp.cpu_count() // 8) as executor:  # 8 sur 64 CPU
    futures = [executor.submit(run_benchmark, f, "Spark") for name, f in scripts if os.path.exists(f)]
    for future in as_completed(futures):
        result = future.result()
        print(f"Spark  {result['nom']:20} → Temps {result['temps']:.2f}s | Mémoire +{result['memoire']:.2f}MB | CPU +{result['cpu']:.2f}%")

# Parallel benchmark for Dask (avec Client pour internal parallel)
with Client(n_workers=mp.cpu_count() // 4) as client:  # 16 workers
    with ThreadPoolExecutor(max_workers=mp.cpu_count() // 8) as executor:
        futures = [executor.submit(run_benchmark, f, "Dask") for f in dask_scripts if os.path.exists(f)]
        for future in as_completed(futures):
            result = future.result()
            print(f"Dask   {result['nom']:20} → Temps {result['temps']:.2f}s | Mémoire +{result['memoire']:.2f}MB | CPU +{result['cpu']:.2f}%")