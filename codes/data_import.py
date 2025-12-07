from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import shutil
import json
from pathlib import Path

# ============================
# Chemins
# ============================
BASE_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = BASE_DIR / "data"
KAGGLE_DIR = DATA_DIR / "kaggle_notebooks"
GITHUB_DIR = DATA_DIR / "github_repos"
LOG_FILE = DATA_DIR / "import_log.json"

# Reset propre
for p in [KAGGLE_DIR, GITHUB_DIR]:
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True, exist_ok=True)

print("🚀 Importation des sources PySpark (GitHub + Kaggle) ...")

# ============================
# Listes de sources
# ============================

# Notebooks Kaggle à importer
KAGGLE_KERNELS = [
    "roshansharma/pyspark-eda-tutorial",
    "sagarnild/pyspark-tutorial",
    "ahmedbelkacem/pyspark-basics"
]

# Repos GitHub à cloner
GITHUB_REPOS = [
    "https://github.com/databricks/LearningSparkV2.git",
    "https://github.com/databricks/Spark-The-Definitive-Guide.git",
    "https://github.com/joaoffb/pyspark-examples.git",
    "https://github.com/srivathsanc/PySpark-Data-Engineering.git"
]


# ============================
# Logging
# ============================

def log_update(entry):
    """Écrit dans import_log.json"""
    if LOG_FILE.exists():
        data = json.loads(LOG_FILE.read_text())
    else:
        data = {"kaggle": [], "github": []}

    if entry["type"] == "kaggle":
        data["kaggle"].append(entry)
    else:
        data["github"].append(entry)

    LOG_FILE.write_text(json.dumps(data, indent=4))


# ============================
# Kaggle — Téléchargement
# ============================

def download_kaggle_kernel(kernel: str):
    name = kernel.replace("/", "__")
    dest = KAGGLE_DIR / name
    dest.mkdir(exist_ok=True)

    print(f"➡️ Kaggle pull : {kernel}")

    cmd = f"kaggle kernels pull {kernel} -p '{dest}' --force -q"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        log_update({"type": "kaggle", "kernel": kernel, "status": "OK"})
        print(f"   ✔️ Téléchargé : {kernel}")
        return True

    log_update({"type": "kaggle", "kernel": kernel, "status": "ERROR"})
    print(f"   ❌ Échec : {kernel}")
    return False


# ============================
# GitHub — Clonage
# ============================

def clone_github_repo(url: str):
    name = url.split("/")[-1].replace(".git", "")
    dest = GITHUB_DIR / name

    print(f"➡️ Git clone : {name}")

    cmd = f"git clone --depth 1 --single-branch -q '{url}' '{dest}'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        log_update({"type": "github", "repo": url, "status": "OK"})
        print(f"   ✔️ Cloné : {name}")
        return True

    log_update({"type": "github", "repo": url, "status": "ERROR"})
    print(f"   ❌ Échec clonage : {name}")
    return False


# ============================
# Exécution parallèle
# ============================

def main_import():

    tasks = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        # Kaggle tasks
        for k in KAGGLE_KERNELS:
            tasks.append(executor.submit(download_kaggle_kernel, k))

        # GitHub tasks
        for repo in GITHUB_REPOS:
            tasks.append(executor.submit(clone_github_repo, repo))

        for f in as_completed(tasks):
            pass

    print("\n✨ Import terminé !")
    print(f"📄 Log enregistré dans : {LOG_FILE.absolute()}")


if __name__ == "__main__":
    main_import()
