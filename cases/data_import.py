# importer ces bibliothèques avant d'exécuter les scripts
# API Kaggle (pour datasets + notebooks)
# pip install kaggle

# Git (déjà installé sur Mac, sinon : brew install git)
# Pandas + json pour extraire code des IPYNB
# pip install pandas ipython


# Télécharger les datasets utilisés dans les notebooks

import os
import subprocess
from pathlib import Path

# Dossiers de sortie (tu peux adapter)
DATA_DIR = Path("../data").resolve()
NB_DIR = Path("./kaggle_notebooks").resolve()

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd: str):
    print(f"\n➡️ Exécution : {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"⚠️ Erreur lors de la commande : {cmd}")
    else:
        print("✅ OK")

def main():
    # 1. Créer les dossiers si besoin
    ensure_dir(DATA_DIR)
    ensure_dir(NB_DIR)

    # 2. Télécharger les DATASETS
    run_cmd(f"kaggle datasets download -d rohitsahoo/sales-forecasting -p {DATA_DIR} --unzip")
    run_cmd(f"kaggle datasets download -d sjrdiaz/pakistans-largest-ecommerce-dataset -p {DATA_DIR} --unzip")
    run_cmd(f"kaggle datasets download -d mmohaiminulislam/ecommerce-data-analysis -p {DATA_DIR} --unzip")

    # 3. Télécharger les NOTEBOOKS
    run_cmd(f"kaggle kernels pull nikitakudriashov/predict-sales-spark-etl-eda -p {NB_DIR}")
    run_cmd(f"kaggle kernels pull tauqeersajid/pyspark-pakistan-ecommerce-dataset-analysis -p {NB_DIR}")
    run_cmd(f"kaggle kernels pull towhidultonmoy/end-to-end-pyspark-project -p {NB_DIR}")

if __name__ == "__main__":
    main()
print("Téléchargement terminé !")