# cases/profile_extracted.py
import os
import glob
import time
import psutil
import webbrowser
import subprocess

# --- CONFIG ---
extracted_dir = "./extracted_scripts"
data_dir = "../data"
csv_files = glob.glob(f"{data_dir}/*.csv")
if not csv_files:
    print("Aucun CSV dans ../data")
    exit(1)

py_files = glob.glob(f"{extracted_dir}/*_pyspark.py")
print(f"{len(py_files)} scripts à profiler\n")

for py_file in py_files:
    print(f"PROFILAGE : {os.path.basename(py_file)}")
    print(f"CSV : {', '.join([os.path.basename(p) for p in csv_files[:2]])}{'...' if len(csv_files)>2 else ''}")

    # --- FICHIER TEMPORAIRE ---
    temp_file = py_file.replace(".py", "_run.py")
    with open(py_file, 'r', encoding='utf-8') as f:
        content = f.read()

    executable = f"""
import time
import webbrowser
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

# --- CODE EXTRAIT ---
{content}

# --- LANCEMENT ---
if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("Profile") \\
        .master("local[2]") \\
        .config("spark.ui.enabled", "true") \\
        .config("spark.ui.port", "4040") \\
        .config("spark.driver.memory", "2g") \\
        .getOrCreate()
    
    # OUVRE UI APRÈS DÉMARRAGE
    time.sleep(2)
    webbrowser.open("http://localhost:4040")
    
    print("Spark démarré ! UI → http://localhost:4040")
    paths = {csv_files}
    try:
        run(spark, paths)
        print("SUCCÈS : Pipeline exécuté")
    except Exception as e:
        print(f"ERREUR : {{e}}")
    finally:
        input("Appuie Entrée pour continuer...")
        spark.stop()
"""
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(executable)

    # --- MESURE ---
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024**2
    start = time.time()

    try:
        result = subprocess.run(["python", temp_file], capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"ERREUR : {result.stderr.splitlines()[0]}")
        else:
            print("SUCCÈS : Pipeline exécuté")
    except Exception as e:
        print(f"ERREUR : {e}")

    duration = time.time() - start
    mem_after = process.memory_info().rss / 1024**2

    print(f"Temps : {duration:.2f}s")
    print(f"Mémoire : +{mem_after - mem_before:.2f} MB")
    print("Appuie Entrée pour le suivant...")
    input()
    os.remove(temp_file)