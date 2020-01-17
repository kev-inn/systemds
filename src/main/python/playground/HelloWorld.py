from pyspark.context import SparkContext

import os
import sys
path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..\\")
sys.path.insert(0, path)
from systemds import MLContext, dml, dmlFromResource, dmlFromFile, dmlFromUrl

# Spark entrypoint
sc = SparkContext.getOrCreate()

# Making Available the systemds dml syntax
ml = MLContext(sc)
helloScript = dml("print('Hello World')")
ml.execute(helloScript)