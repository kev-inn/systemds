import numpy as np
import pandas as pd
import os
import sys

try:
    import py4j.java_gateway
    from py4j.java_gateway import JavaObject
    from pyspark import SparkContext
    from pyspark.conf import SparkConf
    import pyspark.mllib.common
    from pyspark.sql import SparkSession
except ImportError:
    raise ImportError(
        'Unable to import `pyspark`. Hint: Make sure you are running with PySpark.')
path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
sys.path.insert(0, path)

from .mlcontext import MLContext

ml_context = None

__all__ = ['getOrCreateMLContext', 'worker', 'matrix']

SUPPORTED_TYPES = (np.ndarray, pd.DataFrame) #, spmatrix))

def getOrCreateMLContext():
    global ml_context
    if ml_context is None:
        ml_context = MLContext(SparkContext.getOrCreate())
    return ml_context

################################################################################

class worker(object):
    """
    sets the ip and the port as well as the data that shall be retrieved
    """
    def __init__(self, ip, port, document):
        #        ToDo: check whether Imput is valid! (if statements)
        self.document = document
        self.ip = ip
        self.port = port


    def getaddress(self):
        return str(self.ip + ":" + self.port)

# #################################################################################
#
# class document(object):
#
#     def __init__(self, docname, startcoor, endcoor):
#         self.docname = docname
#         self.startcoor = endcoor
#         self.endcoor = startcoor
#
#         # def getDocName():
#         #     return self.docname
#         #
#         # def getStartCoor():
#         #     return self.startcoor
#         #
#         # def getEndCoor():
#         #     return self.startcoor
#         #
#         # def setStartCoor(set):
#         #     self.startcoor = set
#         #
#         # def setEndCoor(set):
#         #     self.startcoor = set
#
# ####################################################################################

class matrix(object):

    def __init__(self, data):
        self.data = data
        self.shape = None
        if isinstance(data, SUPPORTED_TYPES):
            self.shape = data.shape
        if not (isinstance(data, SUPPORTED_TYPES)):
            raise TypeError('Unsupported input type')

###################################################################################

class fedmatrix(object,worker):

    def __init__(self):

        dml('X = federated(addresses=list($1, $2), ranges=list(list(0, 0), list($5, $4), list($5, 0), list($3, $4)))').output('X').input('$1'=address1, '$2'=address2, )



# class fedmatrix(metamatrix):
#     pass
#
#
#
# class abstract matrix(object):
#
#     def __init__(self, javaMatrix, sc):
#         self._java_matrix = javaMatrix
#         self._sc = sc
#
#     # M E T H O D S
#     def l2svm(self, Y, intercept = False, epsilon = 0.001, lmbda=1.0, max_iter=100):
#             script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
#                 .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
#             return getOrCreateMLContext().execute(script).get("model")
#
#     def l2svm(self, Y, fed, intercept = False, epsilon = 0.001, lmbda=1.0, max_iter=100):
#         #assigning variables
#         if (fed = 1):
#             l2svm(self, Y, intercept = False, epsilon = 0.001, lmbda=1.0, max_iter=100)
#         elif():
#
#         script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
#             .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
#         return getOrCreateMLContext().execute(script).get("model")
#
#
#     #
#     # def l2svm(self, Y, FedObj, intercept=False, epsilon=0.001, lmbda=1.0, max_iter=100):
#     #     """
#     #     Executes l2svm on the given data with the given parameters. Returns a `MLContext.Matrix` object.
#     #     """
#     #
#     #
#     #     script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
#     #         .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
#     #     return getOrCreateMLContext().execute(script).get("model")
#     #
#     #     #$1 Adress1
#     #     #$2 Adress2
#     #     #$3 size of columns
#     #     #$4 size of
#     #     #$5 size of
#     #     #   (0,0) . . . . (0,M)  #   ( 0, 0) . . . . ( 0, M )  #
#     #     #   .                    #   .
#     #     #   .                    #   .
#     #     #   (n,0) . . . . (n,M)  #   ($5, 0)  . . . . ($5, $4) #
#     #     #   .                    #   .
#     #     #   .                    #   .
#     #     #   (N,0) . . . . (N,M)  #   ($3, 0) . . . . ($3, $4)  #
#     #     #$6
#     #
#     #     X = federated(addresses=list($1, $2),
#     #     ranges=list(list(0, 0), list($5, $4), list($5, 0), list($3, $4)))
#     #     Y = read($6)
#     #     model= l2svm(X=X,  Y=Y, intercept = FALSE, epsilon = 1e-12, lambda = 1, maxiterations = 100)
#     #     write(model, $7, format="binary")
#     #     print("L2SVM model")
#     #     print(toString(model))
#
