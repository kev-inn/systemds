import numpy as np
from random import random



class randomMatrix(object):

    # CREATE CONSTRUCTOR // maybe here can allready be used numpy
    def __init__(self, dim):
        self.dim = dim
        self.seed = seed
        np.random.seed(1304)
        m1 = np.array(np.random.randint(100, size=dim * dim) + 1.01, dtype=np.double)
        m1.shape = (dim, dim)
        return m1

        # m2 = np.zeros((dim, 1))
        # for i in range(dim):
        #     if np.random.random() > 0.5:
        #         m2[i][0] = 1

    # def matrixFactory(dim=10):
    #
    # def test_10x10_func(self):
    #     dim = 10
    #     np.random.seed(1304)
    #     m1 = np.array(np.random.randint(100, size=dim * dim) + 1.01, dtype=np.double)
    #     m1.shape = (dim, dim)
    #     m2 = np.zeros((dim, 1))
    #     for i in range(dim):
    #         if np.random.random() > 0.5:
    #             m2[i][0] = 1
    #
    #
    #
    #
    #
    # # M E T H O D S
    # def l2svm(self, Y, intercept = False, epsilon = 0.001, lmbda=1.0, max_iter=100):
    #         script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
    #             .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
    #         return getOrCreateMLContext().execute(script).get("model")
    #
    # def l2svm(self, Y, fed, intercept = False, epsilon = 0.001, lmbda=1.0, max_iter=100):
    #     #assigning variables
    #     if (fed = 1):
    #         l2svm(self, Y, intercept = False, epsilon = 0.001, lmbda=1.0, max_iter=100)
    #     elif():
    #
    #     script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
    #         .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
    #     return getOrCreateMLContext().execute(script).get("model")
    #
    # port1 =
    # port2 =
    #
    # #
    # # def l2svm(self, Y, FedObj, intercept=False, epsilon=0.001, lmbda=1.0, max_iter=100):
    # #     """
    # #     Executes l2svm on the given data with the given parameters. Returns a `MLContext.Matrix` object.
    # #     """
    # #
    # #
    # #     script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
    # #         .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
    # #     return getOrCreateMLContext().execute(script).get("model")
    # #
    # #     #$1 Adress1
    # #     #$2 Adress2
    # #     #$3 size of columns
    # #     #$4 size of
    # #     #$5 size of
    # #     #   (0,0) . . . . (0,M)  #   ( 0, 0) . . . . ( 0, M )  #
    # #     #   .                    #   .
    # #     #   .                    #   .
    # #     #   (n,0) . . . . (n,M)  #   ($5, 0)  . . . . ($5, $4) #
    # #     #   .                    #   .
    # #     #   .                    #   .
    # #     #   (N,0) . . . . (N,M)  #   ($3, 0) . . . . ($3, $4)  #
    # #     #$6
    # #
    # #     X = federated(addresses=list($1, $2),
    # #     ranges=list(list(0, 0), list($5, $4), list($5, 0), list($3, $4)))
    # #     Y = read($6)
    # #     model= l2svm(X=X,  Y=Y, intercept = FALSE, epsilon = 1e-12, lambda = 1, maxiterations = 100)
    # #     write(model, $7, format="binary")
    # #     print("L2SVM model")
    # #     print(toString(model))