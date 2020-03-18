DAGNode
========

`DAGNode` is a central class for building a directed acyclic graph (DAG) of our operations before sending
them to a java instance which runs SystemDS and does the actual calculations.

`DAGNode`s represent each operation we do and if we call `.compute()` we will create a `DML`-script, the language
SystemDS uses, and execute this on a java instance which is either started manually, or which will automatically be
started lazily once we need it.

.. autoclass:: systemds.DAGNode
    :members:
