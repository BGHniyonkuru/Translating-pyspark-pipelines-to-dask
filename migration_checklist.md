# Migration PySpark → Dask — Checklist

| Opération PySpark              | Équivalent Dask                            | Attention particulière                  |
|--------------------------------|--------------------------------------------|-----------------------------------------|
| `spark.read.csv()`             | `dd.read_csv()`                            | même paramètres                         |
| `df.filter()`                  | `df[df.condition]`                         | syntaxe Pandas                          |
| `df.groupBy().agg()`           | `df.groupby().agg()`                       | très performant localement              |
| `withColumn(UDF)`              | `map_partitions` ou `str` methods          | éviter les UDF si possible              |
| `join()`                       | `df.merge()`                               | bien spécifier `how`                    |
| `Correlation.corr()`           | `df.corr()` (Pandas)                       | instantané sur machine unique           |
| `spark.sql()`                  | `dask-sql` ou Pandas                       | pour requêtes complexes                 |