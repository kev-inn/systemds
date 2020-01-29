#from py4j.java_gateway import JavaGateway

#gateway = JavaGateway()
#print(gateway)
#gg = JavaGateway.launch_gateway(classpath="/path/my_jar.jar")
#print(gg)

from py4j.java_gateway import JavaGateway, Py4JNetworkError, JavaObject
try:
    java_gateway = JavaGateway(eager_load=True)
    print("JVM accepting connection")
except Py4JNetworkError:
    print("No JVM listening")
except Exception:
    print("Another type of problem... maybe with the JVM")

object = java_gateway.JavaObject
print(object)
jvmfoo = java_gateway.jvm
print(jvmfoo)
script = java_gateway.jvm.org.tugraz.sysds.api.mlcontext.ScriptFactory.dmlFromString("print('Hello World')")
