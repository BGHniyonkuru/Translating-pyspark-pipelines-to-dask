# RAG EXPERT — Migration PySpark → Dask (2025)

## MAPPINGS DIRECTS (à utiliser en priorité)
spark.read.parquet(path) → dd.read_parquet(path, engine="pyarrow")
spark.read.csv(path, header=True) → dd.read_csv(path)
df.write.parquet(path, partitionBy="date") → df.to_parquet(path, partition_on="date", write_index=False)
df.groupBy("col").count() → df.groupby("col").size().compute()
df.join(other, "id") → df.merge(other, on="id", how="inner")

## ANTI-PATTERNS À ÉLIMINER
- UDF Python → remplacer par map_partitions + pandas vectorisé
- .collect() / .toPandas() → interdit sauf à la fin
- repartition() sans raison → supprimer
- cache() → remplacer par persist() si nécessaire

## BEST PRACTICES DASK 2025
- Toujours utiliser dask.dataframe pour >90% des cas
- Utiliser divisions connues quand possible
- Ajouter client = Client() pour voir le dashboard
- Utiliser compute() uniquement à la fin
- Ajouter des commentaires explicatifs

## EXEMPLES PARFAITS (à imiter)
```python
# PySpark → Dask (pattern gagnant)
# df = spark.read.parquet("s3://...")
# df = df.filter(df.age > 18)
# df = df.groupBy("country").agg({"salary": "mean"})
# df.write.parquet("output/")

import dask.dataframe as dd
df = dd.read_parquet("s3://...")
df = df[df.age > 18]
result = df.groupby("country").salary.mean()
result.to_parquet("output/")