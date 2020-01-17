from pyspark.sql.types import * #StructType, StructField, DoubleType,...
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext

import os
import sys

path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..\\")
sys.path.insert(0, path)

from systemds import MLContext, dml, dmlFromResource, dmlFromFile, dmlFromUrl

# Create rdd1 and rdd2 in SparkContext
sc = SparkContext.getOrCreate()

rdd1 = sc.parallelize(["1.0,2.0", "3.0,4.0"])
rdd2 = sc.parallelize(["5.0,6.0", "7.0,8.0"])

# Execute the function in dml
ml = MLContext(sc)

sums = """
s1 = sum(m1);
s2 = sum(m2);
if (s1 > s2) {
  message = "s1 is greater"
} else if (s2 > s1) {
  message = "s2 is greater"
} else {
  message = "s1 and s2 are equal"
}
"""

#with open("sums.dml", "w") as text_file:
#   text_file.write(sums)

sumScript = dmlFromFile("C:\\Richard\\002_SystemDS\\pythonwrapper\\systemds\\sums.dml").input(m1=rdd1, m2= rdd2).output("s1", "s2", "message")
sumResults = ml.execute(sumScript)
s1 = sumResults.get("s1")
s2 = sumResults.get("s2")
message = sumResults.get("message")