Matrix API
==========

OperationNode
-------------

An `OperationNode` represents an operation which we will execute in SystemDS.

A lot of magic methods are overloaded for `OperationNode`, but all of them return an `OperationNode`, even
comparisons like ``__eq__``, ``__lt__`` etc., therefore one has to call ``.compute()`` to get the actual result.

.. note::

  All operations are evaluated lazily, meaning before calling ``.compute()`` nothing will be executed in SystemDS,
  therefore some errors will not immediately be recognized.

.. autoclass:: systemds.matrix.OperationNode
  :members:

Matrix
------

A `Matrix` is represented either by an `OperationNode`, or the derived class `Matrix`. We can recognize it
by checking the ``output_type`` of the object.

Matrices are the most fundamental objects we operate on. If we can generate the matrix in SystemDS directly via a function
call, we can use an function which will generate an `OperationNode` e.g. `federated`, `full`, `seq`.

If we want to work on an numpy array we need to use the class `Matrix`.

.. autoclass:: systemds.matrix.Matrix
    :members:

.. autofunction:: systemds.matrix.federated

.. autofunction:: systemds.matrix.full

.. autofunction:: systemds.matrix.seq

