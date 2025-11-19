# Extrait de : kaggle_notebooks/end-to-end-pyspark-project.ipynb

import pyspark as sp

sc = sp.SparkContext.getOrCreate()
print(sc)
print(sc.version)

#import SparkSeccion pyspark.sql
from pyspark.sql import SparkSession

#Create my_spark
spark = SparkSession.builder.getOrCreate()

#print my_spark
print(spark)


import pandas as pd
import numpy as np

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())

file_path = '../input/datasets-for-pyspark-project/airports.csv'

#Read in the airports path
airports = spark.read.csv(file_path, header=True)

airports.show()


spark.catalog.listDatabases()

spark.catalog.listTables()

flights = spark.read.csv('../input/datasets-for-pyspark-project/flights_small.csv', header=True)
flights.show()

flights.name = flights.createOrReplaceTempView('flights')
spark.catalog.listTables()

# Create the DataFrame flights
flights_df = spark.table('flights')
print(flights_df.show())

#Using the Spark DataFrame method .selectExpr() 
speed_2 =flights.selectExpr('origin','dest','tailnum','distance/(air_time/60) as avg_speed')
speed_2.show()

# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

#convert to dep_delay to numeric column
flights = flights.withColumn('dep_delay', flights.dep_delay.cast('float'))

# Group by month and dest
by_month_dest = flights.groupBy('month', 'dest')

planes = spark.read.csv('../input/datasets-for-pyspark-project/planes.csv', header=True)
planes.show()


from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Assemble a  Vector
from pyspark.ml.feature import  VectorAssembler


# #### Create the pipeline
# You're finally ready to create a` Pipeline!` Pipeline is a class in the `pyspark.ml module` that combines all the Estimators and Transformers that you've already created.

from pyspark.ml import Pipeline

flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])


from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression()


# #### Create the evaluator
# The first thing you need when doing cross validation for model selection is a way to compare different models. Luckily, the pyspark.ml.evaluation submodule has classes for evaluating different kinds of models. Your model is a binary classification model, so you'll be using the `BinaryClassificationEvaluator` from the `pyspark.ml.evaluation` module. This evaluator calculates the area under the ROC. This is a metric that combines the two kinds of errors a binary classifier can make (false positives and false negatives) into a simple number.

import pyspark.ml.evaluation as evals

evaluator = evals.BinaryClassificationEvaluator(metricName='areaUnderROC')

# Import the tuning submodule
import pyspark.ml.tuning as tune

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0,1])

# Build the grid
grid = grid.build()

