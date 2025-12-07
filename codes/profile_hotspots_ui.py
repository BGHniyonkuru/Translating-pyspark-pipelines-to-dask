# codes/profile_hotspots_ui.py (amélioré 2025 – ports dynamiques + pool 16)
import os
import glob
import time
import psutil
import webbrowser
import subprocess
import requests
from multiprocessing import Pool, cpu_count
from pyspark.sql import SparkSession

EXTRACTED_DIR = "../data/extracted_scripts"
DATA_DIR = "../data"
CSV_FILES = glob.glob(f"{DATA_DIR}/*.csv")

def get_spark_ui_metrics(app_name, port=4040):
    try:
        jobs = requests.get(f"http://localhost:{port}/api/v1/applications/{app_name}/jobs", timeout=5).json()
        stages = requests.get(f"http://localhost:{port}/api/v1/applications/{app_name}/stages", timeout=5).json()
        return {
            "has_groupby": any("group" in j.get("name", "").lower() for j in jobs),
            "has_udf": any("udf" in s.get("name", "").lower() for s in stages),
            "shuffle_mb": sum(s.get("shuffleRead", 0) for s in stages) / (1024**2)
        }
    except:
        return {"has_groupby": False, "has_udf": False, "shuffle_mb": 0}

def profile_single(args):
    py_file, index = args
    name = os.path.basename(py_file)
    print(f"\nPROFILAGE : {name}")
    
    # Temp file pour exécution
    temp_file = py_file.replace(".py", "_run.py")
    with open(py_file, 'r') as f:
        content = f.read()
    
    app_name = f"Profile_{name.replace('.py', '')}"
    ui_port = 4040 + index  # Dynamique : 4040, 4041, etc. pour éviter conflits
    
    executable = f"""
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

{content}

if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("{app_name}") \\
        .master("local[2]") \\
        .config("spark.ui.enabled", "true") \\
        .config("spark.ui.port", "{ui_port}") \\
        .config("spark.driver.memory", "2g") \\
        .config("spark.dynamicAllocation.enabled", "true") \\
        .getOrCreate()
    time.sleep(2)
    paths = {CSV_FILES}
    try:
        run(spark, paths)
    except Exception as e:
        print(f"ERREUR: {{e}}")
    input("Entrée pour stop...")
    spark.stop()
"""
    with open(temp_file, 'w') as f:
        f.write(executable)
    
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024**2
    start = time.time()
    
    try:
        subprocess.run(["python", temp_file], timeout=60)
        duration = time.time() - start
        mem_after = process.memory_info().rss / 1024**2
        metrics = get_spark_ui_metrics(app_name, ui_port)
        
        print(f"Temps: {duration:.2f}s | Mémoire +{mem_after - mem_before:.2f}MB")
        print("Hotspots:")
        if metrics["has_groupby"]: print("- GroupBy → Shuffle détecté (optimiser en Dask avec repartition)")
        if metrics["has_udf"]: print("- UDF → Sérialisation (remplacer par map_partitions)")
        print(f"- Shuffle: {metrics['shuffle_mb']:.1f}MB")
        webbrowser.open(f"http://localhost:{ui_port}")
    except Exception as e:
        print(f"ERREUR: {e}")
    finally:
        os.remove(temp_file)
        input("Entrée pour suivant...")

def main():
    py_files = glob.glob(f"{EXTRACTED_DIR}/*_pyspark.py")
    print(f"{len(py_files)} scripts à profiler")
    args = [(py, i) for i, py in enumerate(py_files)]
    with Pool(cpu_count() // 4) as p:  # Ex: 16 sur 64 CPU – équilibré
        p.map(profile_single, args)

if __name__ == "__main__":
    main()