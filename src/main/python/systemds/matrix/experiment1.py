import os
import sys


path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../")
sys.path.insert(0, path)


import numpy as np
import pandas as pd
from systemds import matrix, dml, MLContext, getOrCreateMLContext

#from pyspark.context import SparkContext
#sc = SparkContext.getOrCreate()
#ml = MLContext(sc)

ml = getOrCreateMLContext()


# First trial of a matrix
m0 = matrix(np.ones((3,3)))
print(m0.shape)

# Second trial of a matrix
dim = 10
np.random.seed(1304)
m1 = np.array(np.random.randint(100, size=dim * dim) + 1.01, dtype=np.double)
m1.shape = (dim, dim)

#m2 = matrix(np.array(np.random.randint(100, size=dim * dim) + 1.01, dtype=np.double))
#m2.shape = (dim, dim)
#print(m2.data)

m3 = np.zeros((dim, 1))
for i in range(dim):
    if np.random.random() > 0.5:
        m3[i][0] = 1
m4 = matrix(m3)

print(m4.data)
script = dml("model = l2svm(X=X, Y=Y)").output("model").input(X=m1, Y=m3)


