import re
import shutil
from pathlib import Path

# Dossiers
BASE = Path(__file__).parent.parent.resolve()
DATA = BASE / "data"
KAGGLE = DATA / "kaggle_notebooks"
GITHUB = DATA / "github_repos"

OUTPUT = DATA / "extracted_scripts"
OUTPUT.mkdir(parents=True, exist_ok=True)

# Reset extraction directory
shutil.rmtree(OUTPUT, ignore_errors=True)
OUTPUT.mkdir(parents=True, exist_ok=True)

# =============================
# 1) PATTERN PySpark à détecter
# =============================

PYSPARK_PATTERNS = [
    "from pyspark.sql",
    "spark.read",
    "pyspark.sql",
    "from pyspark",
    "spark = SparkSession"
]

SPARK_READ_PATTERN = re.compile(
    r'(spark\.read\.(csv|json|parquet)\(["\'](.+?)["\'])'
)

# =============================
# 2) DATASETS DE TEST
# =============================

TEST_DATASETS = {
    "csv": "data/raw_datasets/fact_table.csv",
    "json": "data/raw_datasets/customer_dim.csv",
    "parquet": "data/raw_datasets/fact_table.csv"
}

def patch_paths(code: str):
    """Remplace tous les chemins spark.read.* par les datasets locaux."""
    def repl(match):
        format_ = match.group(2)
        new_path = TEST_DATASETS.get(format_, TEST_DATASETS["csv"])
        return f'spark.read.{format_}("{new_path}"'

    return SPARK_READ_PATTERN.sub(repl, code)


# =============================
# 3) CHERCHER DU CODE PYSPARK
# =============================

def extract_from_file(file_path: Path):
    """Detect PySpark inside a file."""
    try:
        txt = file_path.read_text(errors="ignore")
    except:
        return None

    if any(pat in txt for pat in PYSPARK_PATTERNS):
        return txt
    return None


def search_in_directory(directory: Path, scripts: list):
    for file in directory.rglob("*"):
        if file.suffix not in [".py", ".ipynb", ".txt"]:
            continue

        content = extract_from_file(file)
        if content:
            scripts.append((file, content))


# =============================
# 4) EXTRACTION PRINCIPALE
# =============================

def extract_all_pyspark_scripts():
    scripts = []

    print("🔍 Scan Kaggle notebooks...")
    search_in_directory(KAGGLE, scripts)

    print("🔍 Scan GitHub repositories...")
    search_in_directory(GITHUB, scripts)

    if not scripts:
        print("❌ Aucun code PySpark trouvé.")
        return

    print(f"✨ {len(scripts)} fichiers PySpark trouvés")

    # Enregistrer les scripts patchés
    for idx, (source_path, raw_code) in enumerate(scripts, start=1):
        patched = patch_paths(raw_code)
        outfile = OUTPUT / f"script_{idx:03d}.py"
        outfile.write_text(patched)
        print(f"   ➜ script_{idx:03d}.py extrait depuis {source_path}")


if __name__ == "__main__":
    extract_all_pyspark_scripts()
    print("\n🎉 Extraction complète !")


