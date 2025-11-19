Tools used
python3 -m venv venv
Python --version: 3.13.2

source venv/bin/activate   # Linux/Mac
or venv\Scripts\activate # Windows
Pip: 25.3

Install dependencies
pip install pyspark "dask[complete]" pandas jupyter matplotlib pyyaml psutil

Install java
brew install openjdk@17

Activate Java 17
sudo ln -sfn $(brew --prefix openjdk@17)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk


# Translating PySpark Pipelines to Dask – Assistant Code

> **Berline Niyonkuru** – Projet Data Engineering – 2025

## Objectif
Créer un **assistant semi-automatisé** pour migrer des pipelines PySpark vers Dask avec :
- Profiling
- Prédiction IA (LLM)
- Traduction automatique
- Benchmarks

## Structure

pyspark-to-dask-assistant/
├── docs/               ← Rapport, slides
├── cases/              ← Scripts PySpark extraits
├── ai_assistant/       ← Traducteur + LLM
├── benchmarks/
├── data/               ← Datasets
├── venv/               ← Environnement virtuel
└── README.md

├── cases/          ← Scripts PySpark test
├── ai_assistant/   ← Traducteur + LLM
├── benchmarks/     ← Résultats
├── docs/           ← Rapport, slides
└── data/

## Installation
```bash
python -m venv venv
source venv/bin/activate
pip install pyspark dask[complete] pandas jupyter