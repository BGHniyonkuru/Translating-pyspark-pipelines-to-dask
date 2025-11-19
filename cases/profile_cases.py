# cases/profile_cases.py  ← Remplace juste le début
import time
import psutil
import webbrowser  # ← AJOUTE ÇA
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean

def profile_spark_script(script_name, csv_path):
    print(f"\nPROFILAGE : {script_name}")
    print(f"Dataset : {csv_path}")

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(f"Profile_{script_name}") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    # OUVRE LE NAVIGATEUR AUTOMATIQUEMENT
    time.sleep(2)
    webbrowser.open("http://localhost:4040")

    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024**2
    start_time = time.time()

    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print(f"Lignes : {df.count()} | Colonnes : {len(df.columns)}")

        if script_name == "group_by_agg":
            result = df.groupBy("store_key").agg({"total_price": "sum"})
            result.show(5)

        elif script_name == "join_with_column":
            premium = spark.createDataFrame([(1, "Premium")], ["customer_key", "tier"])
            df_joined = df.join(premium, "customer_key", "left")
            df_final = df_joined.withColumn("discount", 
                when(col("tier") == "Premium", 0.1).otherwise(0.0))
            df_final.select("store_key", "discount").show(5)

        elif script_name == "full_pipeline":
            result = (df.filter(col("quantity") > 10)
                           .groupBy("item_key")
                           .agg(mean("total_price").alias("avg_price"))
                           .orderBy("avg_price", ascending=False))
            result.show(5)

    except Exception as e:
        print(f"ERREUR : {e}")

    end_time = time.time()
    mem_after = process.memory_info().rss / 1024**2
    print(f"Temps : {end_time - start_time:.2f}s")
    print(f"Mémoire : +{mem_after - mem_before:.2f} MB")
    print("Spark UI → http://localhost:4040")
    input("Appuie Entrée pour continuer...")
    spark.stop()

if __name__ == "__main__":
    profile_spark_script("group_by_agg", "../data/fact_table.csv")
    profile_spark_script("join_with_column", "../data/fact_table.csv")
    profile_spark_script("full_pipeline", "../data/fact_table.csv")