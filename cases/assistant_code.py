# cases/assistant_code.py
import os
import glob
import time
import psutil
import subprocess
import webbrowser
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

# --- CONFIG ---
extracted_dir = "./extracted_scripts"
data_dir = "../data"
csv_files = glob.glob(f"{data_dir}/*.csv")

# --- VRAIE LECTURE SPARK UI ---
def get_spark_ui_metrics():
    try:
        # Jobs
        jobs = requests.get("http://localhost:4040/api/v1/applications/local-*/jobs", timeout=5).json()
        # Stages
        stages = requests.get("http://localhost:4040/api/v1/applications/local-*/stages", timeout=5).json()
        # SQL
        sql = requests.get("http://localhost:4040/api/v1/applications/local-*/sql", timeout=5).json()
        
        metrics = {
            "has_groupby": any("group" in j.get("name", "").lower() for j in jobs),
            "has_corr": any("correlation" in s.get("name", "").lower() for s in stages),
            "has_udf": any("udf" in s.get("name", "").lower() for s in stages),
            "shuffle_read": sum(s.get("shuffleRead", 0) for s in stages),
            "duration": sum(j.get("duration", 0) for j in jobs) / 1000
        }
        return metrics
    except:
        return {"has_groupby": False, "has_corr": False, "has_udf": False, "shuffle_read": 0, "duration": 0}

# --- LLM PRÉDICTIONS ---
LLM_HOTSPOTS = {
    "end-to-end-pyspark-project_pyspark.py": {"groupBy": True},
    "predict-sales-spark-etl-eda_pyspark.py": {"UDF": True},
    "pyspark-pakistan-ecommerce-dataset-analysis_pyspark.py": {"Correlation.corr()": True}
}

# --- VÉRIFICATION ---
def verify_with_llm(py_file, ui_metrics):
    base_name = os.path.basename(py_file)
    expected = LLM_HOTSPOTS.get(base_name, {})
    
    print(f"\nVÉRIFICATION LLM vs SPARK UI : {base_name}")
    print("─" * 60)
    
    match = 0
    for op, should_have in expected.items():
        detected = False
        if "groupBy" in op and ui_metrics["has_groupby"]:
            detected = True
        elif "Correlation" in op and ui_metrics["has_corr"]:
            detected = True
        elif "UDF" in op and ui_metrics["has_udf"]:
            detected = True
        
        status = "OK" if detected else "KO"
        print(f"{status} : {op} {'détecté' if detected else 'NON détecté'}")
        if detected:
            match += 1
    
    accuracy = match / len(expected) if expected else 0
    print(f"Précision LLM : {accuracy*100:.0f}%")
    print(f"Shuffle Read : {ui_metrics['shuffle_read'] / 1024**2:.1f} MB")
    return accuracy

# --- PROFILAGE ---
py_files = glob.glob(f"{extracted_dir}/*_pyspark.py")

for py_file in py_files:
    print(f"\nPROFILAGE + VÉRIFICATION : {os.path.basename(py_file)}")
    
    temp_file = py_file.replace(".py", "_run.py")
    with open(py_file, 'r') as f:
        content = f.read()

    executable = f"""
import time, webbrowser
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

{content}

if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("Verify") \\
        .master("local[2]") \\
        .config("spark.ui.enabled", "true") \\
        .config("spark.ui.port", "4040") \\
        .getOrCreate()
    
    time.sleep(3)
    webbrowser.open("http://localhost:4040")
    
    paths = {csv_files}
    run(spark, paths)
    
    print("Appuie Entrée pour continuer...")
    input()
    spark.stop()
"""
    with open(temp_file, 'w') as f:
        f.write(executable)

    # --- LANCE ---
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024**2
    start = time.time()

    subprocess.Popen(["python", temp_file])
    
    # Attends que Spark démarre
    time.sleep(8)
    
    # Lit Spark UI
    ui_metrics = get_spark_ui_metrics()
    
    duration = time.time() - start
    mem_after = process.memory_info().rss / 1024**2

    # Vérifie
    accuracy = verify_with_llm(py_file, ui_metrics)
    
    print(f"Temps : {duration:.1f}s | Mémoire : +{mem_after-mem_before:.1f} MB")
    print(f"LLM vs Spark UI : {accuracy*100:.0f}% de correspondance")
    
    input("Appuie Entrée pour le suivant...")
    os.remove(temp_file)