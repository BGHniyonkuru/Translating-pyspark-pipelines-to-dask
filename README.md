# Translating PySpark Pipelines to Dask

# Présentation générale et fonctionnement

Ce projet analyse et convertit des pipelines PySpark en Dask pour améliorer la performance et la portabilité. Il extrait des notebooks Kaggle contenant du code PySpark, les profils pour identifier les opérations coûteuses (groupBy, UDF, corrélations), et vérifie la correspondance avec les métriques de l'interface Spark UI. Le projet inclut des scripts pour télécharger des datasets, extraire le code PySpark, et profiler les performances.

# Arborescence du projet

```
Translating-pyspark-pipelines-to-dask/
├── codes/
│   ├── assistant_code.py
│   ├── benchmark.py
│   ├── data_import.py
│   ├── extract_pyspark.py
│   └── profile_hotspots_ui.py
├── data/
│   ├── extracted_scripts/
│   ├── customer_dim.csv
│   ├── fact_table.csv
│   ├── item_dim.csv
│   ├── store_dim.csv
│   ├── time_dim.csv
│   ├── train.csv
│   └── Trans_dim.csv
├── docs/
│    └── Final_presentation.pdf
├── app.py
├── benchmark_comparison.ipynb
├── migration_checklist.md
├── prompt_templates.md
└── README.md

```

# Bibliothèques nécessaires pour le projet

- PySpark
- Dask
- Pandas
- NumPy
- Matplotlib
- Seaborn
- NLTK
- Geopy
- Kaggle API
- seaborn


# Résumé fichier par fichier

## Scripts Python

- **assistant_code.py**: Fonctions utilitaires pour la manipulation de données et l'analyse.
- **benchmark.py**: Fonctions utilitaires pour la manipulation de données et l'analyse.
- **data_import.py**: Télécharge des datasets depuis Kaggle et des notebooks.
- **extract_pyspark.py**: Extrait le code PySpark des notebooks Kaggle en fichiers Python.
- **profile_hotspots_ui.py**: Analyse Spark UI + hotspots

## Données

- **Fichiers_csv**: Contient les datasets CSV utilisés pour les analyses.
- **extracted_scripts/**: Contient les scripts PySpark extraits des notebooks Kaggle.
- **kaggle_notebooks/**: Contient les notebooks Kaggle téléchargés.
- **github_repos/**: Contient les repos provenant de Github

## Documentation

- **docs/**: Contient des documents de recherche et d'analyse.

## Interface utilisateur

- **app.py**: Interface pour lancer la pipeline de manière automatisée

## Benchmark

- **benchmark_comparison.ipynb**: Interface pour lancer la pipeline de manière automatisée

## Checklist

- **migration_checklist.md**: Migration PySpark → Dask — Checklist
- **prompt_templates.md**: Prompt LLM — Assistant de migration automatique



# Comment lancer le projet
Il suffira de lancer la pipeline automatisée avec streamlit.
   ```bash
   streamlit run app.py
   ```
 Si on veut apporter des modifications sur la pipeline. Il faudra dérouler le code ainsi:

1. **Installer les dépendances**:
   ```bash
   pip install pyspark dask pandas numpy matplotlib seaborn nltk geopy kaggle
   ```

2. **Configurer Kaggle API**:
   - Créer un fichier `kaggle.json` avec vos identifiants API.
   - Placer le fichier dans `~/.kaggle/`.

3. **Télécharger les données et notebooks**:
   ```bash
   python cases/data_import.py
   ```

4. **Extraire le code PySpark**:
   ```bash
   python cases/extract_pyspark.py
   ```

5. **Profiler les scripts**:
   ```bash
   python cases/profile_hotspots_ui.py
   ```
6. **Benchmark l'utilisation Pyspark vs Dask**:
   ```bash
   python cases/benchmark.py
   ```


# Avertissements et limitations

- Le projet nécessite une connexion Internet pour télécharger les datasets et notebooks.
- Les performances peuvent varier selon la configuration matérielle.
- Certaines fonctionnalités dépendent de l'API Kaggle, qui peut être limitée.


# Vidéo de démonstration
La vidéo de démonstration se trouve sur ce lien drive: https://drive.google.com/file/d/1DN0hUbd8kfWM40b-6Wdj3NjdvmVgXhoS/view?usp=sharing