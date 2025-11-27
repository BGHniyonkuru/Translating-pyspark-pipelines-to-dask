# benchmark.py
import time, subprocess, glob, os

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

print("SPARK vs DASK BENCHMARK\n" + "="*50)
for name, spark_file in scripts:
    if os.path.exists(spark_file):
        t = time.time()
        subprocess.run(["python", spark_file], timeout=60)
        print(f"Spark  {name:20} → {time.time()-t:.2f}s")

for i, dask_file in enumerate(dask_scripts):
    t = time.time()
    subprocess.run(["python", dask_file])
    name = ["end-to-end", "udf", "correlation"][i]
    print(f"Dask   {name:20} → {time.time()-t:.2f}s")