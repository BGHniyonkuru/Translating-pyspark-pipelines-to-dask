# Extrait de : kaggle_notebooks/pyspark-pakistan-ecommerce-dataset-analysis.ipynb

# install pyspark


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import itertools

from fbprophet import Prophet
from fbprophet.plot import add_changepoints_to_plot
from fbprophet.diagnostics import cross_validation, performance_metrics
from fbprophet.plot import add_changepoints_to_plot, plot_cross_validation_metric

from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import classification_report, confusion_matrix

# Import Sparksession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

import warnings
warnings.filterwarnings('ignore')

spark=SparkSession.builder.appName("Data_Wrangling").getOrCreate()

# Print PySpark and Python versions
print('Python version: '+sys.version)
print('Spark version: '+spark.version)

# Read data
file_location = "../input/pakistans-largest-ecommerce-dataset/Pakistan Largest Ecommerce Dataset.csv"
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type)\
.option("inferSchema", infer_schema)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load(file_location)

from pyspark.ml.stat import Correlation
data = df1.select('price', 'qty_ordered', 'grand_total', 'discount_amount', 'Year', 'Month')
vector_col = "corr_features"
assembler = VectorAssembler(inputCols=data.columns, 
                            outputCol=vector_col)
myGraph_vector = assembler.transform(data).select(vector_col)
matrix = Correlation.corr(myGraph_vector, vector_col).collect()[0][0]

correlation_dataframe = spark.createDataFrame(corrmatrix, data.columns)
correlation_dataframe.show()

