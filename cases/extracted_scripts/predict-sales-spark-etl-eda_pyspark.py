# Extrait de : kaggle_notebooks/predict-sales-spark-etl-eda.ipynb

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from geopy.geocoders import Nominatim
from pyspark.sql.window import Window
import pandas as pd
import missingno
import copy
import re

from nltk.corpus import stopwords
import nltk

import plotly.express as px
import plotly.graph_objects as go

INPUTFOLDER = "/kaggle/input/competitive-data-science-predict-future-sales/"
spark = SparkSession.builder.config("spark.driver.memory", "15g").getOrCreate()
read_spark = lambda p : spark.read.csv(f'{INPUTFOLDER}{p}.csv', inferSchema=True, header=True)

hardcode_replacer = {'360, ан', 'с"", иг', ' (7,5 г', 'я"", ар', 'и"", Кр', 'PS3, ру', 'Рид, То', '8"", се', 'м"", ар', 'а"", De', '[PC, Ци', 'в"", ар', 'k)", ар', 'м"", ию', 'а"", ар', 'PS4, ан', '"" , ар', 'с"",Кри', 'ень, ро', 'алл, Дэ', 'тер, Дж', 'ижу, ни', 'MP3, Го', 'ени, 2 ', '""9,8""', 'и"", кр', ' PC, Ци', '[PC, ру', 'а"", ма', 'и"", 3 ', 'уин, Хо', 'шт., ро', 'm"", ар', 'вом, ра', 'й"", 9*', 'аук, Но', 'й"", А.', 'MP3, ИД', '(11,5 г', 'e)",арт', '"Ну, по', 'y)", ар', 'и"", ар', 'С"", N ', '- 4, 5 ', 'КИ), 25', 'я"", Ма', 'ртс, мя', ')"", ар', 'изд, Ба', 'ски, Ба', 'г"", ар', '8"", ""', 'орд, Га', 'ах", 7 ', ' 11,5 г', 'и"", же', 'и"", че', 'зки, бе','een, Re', 'ах", 7 ', 'PS4, ру', 'ора, Ло', '[PC, Je', 'тка, 3 ', 'Н.Ю, Ба', 'кин, Ф.', 'old, Gr', '9см, се', 'ack, Re', 'ова, Ло', 'йсе, ар', 'нто, Ло', 'ска, бе', ' S4, иг', 'юкс, Ло', 'онс, Си', 'сса, Ми', 'рик, ла', 'sen, MP', 'сси, Ло', 'e)",арт', 'N 8, ав', 'лки, 2 ', 'ах", 7 ', 'рый, ар', 'wel, ру', 'e)",арт','ах", 7 ', 'e)",арт', 's Blue)"",арт.', 'Black)"", а', 'y)"", ар', 'en)"", ар','тах"", 7', 'ветах, 3 и'}
with open(f'{INPUTFOLDER}items.csv','r') as file:
    a = file.read()
    for r in hardcode_replacer:
        a = a.replace(r,r.replace(",","."))
with open(f'./items.csv','w+') as file:
    file.write(a)
items = spark.read.csv(f'./items.csv', inferSchema=True, header=True)

shops = read_spark("shops")
cat = read_spark("item_categories")
train = read_spark("sales_train")
test = read_spark("test")
sample = read_spark("sample_submission")

def clean_text(txt):
    txt = re.sub('[^+A-Za-zА-Яа-я0-9]+', ' ', str(txt).lower()).strip()
    txt = " ".join([word for word in txt.split(" ") if len(word)>1])
    return txt
spark_clean_text = f.UserDefinedFunction(clean_text)
items = items.withColumn('item_name', spark_clean_text(items["item_name"]))
items.show(truncate=False)

def drop_stopwords(txt):
    from nltk.corpus import stopwords # stopwords load problem with spark
    txt = " ".join([s for s in txt.split() \
                    if (s not in stopwords.words('english')) \
                    and (s not in stopwords.words('russian'))])
    return txt
spark_drop_stopwords = f.UserDefinedFunction(drop_stopwords)
items = items.withColumn('item_name', spark_drop_stopwords(items["item_name"]))
items.show(truncate=False)

for k in extra_cat:
    spark_get_extra_cat = f.UserDefinedFunction(lambda x: 1 if k in x else 0)
    items = items.withColumn(k, spark_get_extra_cat(items["item_name"]).cast(pyspark.sql.types.IntegerType()))

shops = shops.withColumn('shop_name', spark_clean_text(shops["shop_name"]))
shops = shops.withColumn('shop_name', spark_drop_stopwords(shops["shop_name"]))
shops.show(truncate=False)

def get_city(shop_name):
    return shop_name.split(" ")[0]
spark_get_city = f.UserDefinedFunction(get_city)
shops = shops.withColumn('city', spark_get_city(shops["shop_name"]))
print(shops.select("city").distinct().count())
shops.show(truncate=False)

spark_get_latlon = f.UserDefinedFunction(lambda k: city_to_latlon[k][0])
shops = shops.withColumn('lat', spark_get_latlon(shops["city"]))
spark_get_latlon = f.UserDefinedFunction(lambda k: city_to_latlon[k][1])
shops = shops.withColumn('lon', spark_get_latlon(shops["city"]))
shops.show(truncate=False)

def get_type(shop_name):
    for i in ["трк","тц","трц","тк"]:
        if i in shop_name:
            return i
    return 'no_shop_type'
spark_get_type = f.UserDefinedFunction(get_type)
shops = shops.withColumn('shop_type', spark_get_type(shops["shop_name"]))
shops.show(truncate=False)

