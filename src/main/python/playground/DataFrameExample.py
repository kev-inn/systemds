from pyspark.sql.types import * #StructType, StructField, DoubleType,...
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext

import os
import sys
path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..\\")
sys.path.insert(0, path)
from systemds import MLContext, dml, dmlFromResource, dmlFromFile, dmlFromUrl

from random import random

# Spark Entrypoint
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#Table created in sparkContext
numRows = 10
numCols = 10
data = sc.parallelize(range(numRows)).map(lambda x : [ random() for i in range(numCols) ])
schema = StructType([ StructField("C" + str(i), DoubleType(), True) for i in range(numCols) ])

#DataFrame created in sparkSession
df = spark.createDataFrame(data, schema)
df.show()

#making available systemds commands
ml = MLContext(sc)

minMaxMean = """
minOut = min(Xin)
maxOut = max(Xin)
meanOut = mean(Xin)
"""
minMaxMeanScript = dml(minMaxMean).input("Xin", df).output("minOut", "maxOut", "meanOut")
min, max, mean = ml.execute(minMaxMeanScript).get("minOut", "maxOut", "meanOut")

print('Min: ' + str(min))
print('Max: ' + str(max))
print('Mean: ' + str(mean))

sc.stop()
