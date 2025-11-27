from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import shutil
from pathlib import Path

# Chemins absolus (comme dans extract_pyspark.py)
BASE_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = BASE_DIR / "data"
KAGGLE_NB_DIR = DATA_DIR / "kaggle_notebooks"
GITHUB_DIR = DATA_DIR / "github_repos"

# Nettoyage propre
for p in [KAGGLE_NB_DIR, GITHUB_DIR]:
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True, exist_ok=True)

print("Téléchargement optimisé des sources PySpark (Kaggle + GitHub)...")

# === LISTES (les mêmes que toi) ===
kernels = [
    "nikitakudriashov/predict-sales-spark-etl-eda",
    "tauqeersajid/pyspark-pakistan-ecommerce-dataset-analysis",
    "towhidultonmoy/end-to-end-pyspark-project",
    "sharanharsoor/pyspark-everything-you-need-to-know",
    "tientd95/advanced-pyspark-for-exploratory-data-analysis"
]

repos = [
    "https://github.com/AlexIoannides/pyspark-example-project.git",
    "https://github.com/rvilla87/ETL-PySpark.git",
    "https://github.com/hyunjoonbok/PySpark.git",
    "https://github.com/JANHMS/Advanced-ETL-Azure-Databricks-Pyspark.git",
    "https://github.com/telia-oss/birgitta-example-etl.git"
]

# === FONCTIONS ULTRA-RAPIDES ===
def pull_kernel(kernel_name: str):
    cmd = f"kaggle kernels pull {kernel_name} -p \"{KAGGLE_NB_DIR}\" -m"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"OK Kaggle : {kernel_name.split('/')[-1]}")
        return True
    else:
        print(f"ÉCHEC Kaggle : {kernel_name} → {result.stderr.strip()[:100]}")
        return False

def clone_repo(url: str):
    name = url.split("/")[-1].replace(".git", "")
    target = GITHUB_DIR / name
    # --depth 1 = clone rapide (seulement dernier commit)
    # -q = silencieux
    # --single-branch = encore plus rapide
    cmd = f"git clone --depth 1 --single-branch -q \"{url}\" \"{target}\""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"OK GitHub : {name}")
        return True
    else:
        print(f"ÉCHEC GitHub : {name}")
        return False

# === EXÉCUTION PARALLÈLE MAXIMALE + TIMEOUT + PROGRESSION PROPRE ===
def main_import():
    total_tasks = len(kernels) + len(repos)
    completed = 0

    with ThreadPoolExecutor(max_workers=12) as executor:
        # On lance TOUT en parallèle
        futures = []
        for k in kernels:
            futures.append(executor.submit(pull_kernel, k))
        for r in repos:
            futures.append(executor.submit(clone_repo, r))

        # Progression en temps réel
        for future in as_completed(futures):
            completed += 1
            print(f"Progression : {completed}/{total_tasks} tâches terminées", end="\r")

    print(f"\nTéléchargement terminé : {completed}/{total_tasks} sources récupérées avec succès !")

# Lancement
if __name__ == "__main__":
    main_import()