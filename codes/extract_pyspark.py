# codes/extract_pyspark.py → VERSION FINALE 100% FONCTIONNELLE – Berline Niyonkuru
import json
import glob
from pathlib import Path
from multiprocessing import Pool, cpu_count

# === CHEMINS ABSOLUS ===
BASE_DIR = Path(__file__).parent.parent.resolve()
KAGGLE_NB_DIR = BASE_DIR / "data" / "kaggle_notebooks"
GITHUB_REPOS_DIR = BASE_DIR / "data" / "github_repos"
OUTPUT_DIR = BASE_DIR / "data" / "extracted_scripts"
OUTPUT_DIR.mkdir(exist_ok=True)

# === MOTS-CLÉS PYSPARK (large mais efficace) ===
PYSPARK_KEYWORDS = [
    "SparkSession", "spark.read", "spark.sql", "from pyspark.sql",
    "spark.createDataFrame", ".toDF(", ".cache()", ".repartition(",
    "spark-submit", "SparkContext", "spark.sparkContext"
]

# === MOTS-CLÉS QUI INDIQUENT UN VRAI PIPELINE (au moins un suffit) ===
REAL_PIPELINE_INDICATORS = [
    ".read.", ".csv", ".parquet", ".json", ".format(",
    ".write.", ".save(", ".parquet(", ".csv(",
    ".join(", ".groupBy(", ".agg(", ".filter(", ".withColumn(",
    ".select(", ".drop(", ".union(", ".orderBy("
]

# === FICHIERS À EXCLURE (tests, logging, etc.) ===
EXCLUDE_FILES = [
    "test_", "logging.py", "logger", "setup.py", "__init__.py",
    "conf", "config", "util", "helper", "example_test"
]

def is_excluded(filepath: str) -> bool:
    return any(excl in filepath.lower() for excl in EXCLUDE_FILES)

def contains_pyspark_and_pipeline(content: str) -> bool:
    lower = content.lower()
    has_pyspark = any(kw.lower() in lower for kw in PYSPARK_KEYWORDS)
    has_pipeline = any(ind.lower() in lower for ind in REAL_PIPELINE_INDICATORS)
    return has_pyspark and has_pipeline

# === EXTRACTION .ipynb ===
def extract_from_ipynb(nb_path: Path):
    try:
        with open(nb_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
    except:
        return None

    blocks = []
    for i, cell in enumerate(nb.get('cells', [])):
        if cell.get('cell_type') != 'code':
            continue
        source = ''.join(cell.get('source', []))
        if len(source.strip()) < 30:
            continue
        if contains_pyspark_and_pipeline(source):
            blocks.append(f"# Cellule {i+1}\n{source.strip()}")

    if blocks:
        safe_name = "".join(c if c.isalnum() else "_" for c in nb_path.stem)
        origin = "kaggle" if "kaggle" in str(nb_path) else "github"
        output_file = OUTPUT_DIR / f"{safe_name}_{origin}.py"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# EXTRAIT DE → {nb_path.name}\n")
            f.write(f"# Source → {nb_path.relative_to(BASE_DIR)}\n\n")
            f.write("\n\n# =======================================\n\n".join(blocks))
        print(f"EXTRAIT → {output_file.name} ({len(blocks)} blocs)")
        return str(output_file)
    return None

# === EXTRACTION .py ===
def extract_from_py(py_path: Path):
    if is_excluded(str(py_path)):
        return None
    try:
        content = py_path.read_text(encoding='utf-8')
    except:
        return None

    if contains_pyspark_and_pipeline(content):
        safe_name = "".join(c if c.isalnum() else "_" for c in py_path.stem)
        output_file = OUTPUT_DIR / f"{safe_name}_github.py"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# EXTRAIT DE → {py_path.relative_to(BASE_DIR)}\n\n")
            f.write(content)
        print(f"EXTRAIT → {output_file.name}")
        return str(output_file)
    return None

# === RECHERCHE DES FICHIERS ===
def find_all_candidates():
    candidates = []
    # .ipynb
    for pattern in [
        str(KAGGLE_NB_DIR / "**" / "*.ipynb"),
        str(GITHUB_REPOS_DIR / "**" / "*.ipynb")
    ]:
        candidates.extend(glob.glob(pattern, recursive=True))
    # .py
    for py_file in GITHUB_REPOS_DIR.rglob("*.py"):
        if not any(excl in py_file.parts for excl in ["__pycache__", ".git", "venv", "env"]):
            candidates.append(str(py_file))
    return candidates

# === TRAITEMENT PARALLÈLE ===
def process_file(path_str: str):
    path = Path(path_str)
    if path.suffix == ".ipynb":
        return extract_from_ipynb(path)
    elif path.suffix == ".py":
        return extract_from_py(path)
    return None

# === MAIN ===
def main():
    candidates = find_all_candidates()
    print(f"{len(candidates)} fichiers trouvés")

    if not candidates:
        print("Aucun fichier → vérifie data/kaggle_notebooks et data/github_repos")
        return

    workers = min(cpu_count(), 8)
    print(f"Extraction parallèle sur {workers} cœurs...")

    with Pool(workers) as pool:
        results = pool.map(process_file, candidates)

    extracted = [r for r in results if r]
    print(f"\nEXTRACTION TERMINÉE ! {len(extracted)} VRAIS PIPELINES ETL extraits")
    print("Prêts pour traduction Dask !")

if __name__ == "__main__":
    main()