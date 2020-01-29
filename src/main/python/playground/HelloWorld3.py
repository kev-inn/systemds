import systemds
from py4j.java_gateway import JavaGateway

gateway = JavaGateway()
stack = gateway.entry_point.getStack()
stack.push("Bla")
stack.push("Second item")
print(stack.getInternalList())