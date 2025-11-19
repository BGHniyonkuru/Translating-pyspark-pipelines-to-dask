# test_ui.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("TestUI") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("Spark démarré ! Ouvre http://localhost:4040")

# Lecture
df = spark.read.csv("../data/fact_table.csv", header=True, inferSchema=True)
print("Colonnes :", df.columns)

# CORRIGÉ : store_key au lieu de store_id
# CORRIGÉ : total_price au lieu de sales
result = df.groupBy("store_key").agg(_sum("total_price").alias("total_sales"))

print("Résultat :")
result.show(5)

input("Appuie Entrée pour arrêter...")
spark.stop()