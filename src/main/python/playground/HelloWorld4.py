#from py4j.java_gateway import JavaGateway
import numpy as np
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

javaobjectfoo = java_gateway.JavaObject
print(javaobjectfoo)
jvmfoo = java_gateway.jvm
print(jvmfoo)
#script = java_gateway.jvm.org.tugraz.sysds.api.mlcontext.MLContext()
#print(script)
entryfoo = java_gateway.entry_point
print(entryfoo)

conn = jvmfoo.org.tugraz.sysds.api.jmlc.Connection()
#conn.convertToStringFrame("test",1,1)
test = jvmfoo.org.tugraz.sysds.runtime.io.IOUtilFunctions.toInputStream("test")
print(test)
jvmfoo.org.tugraz.sysds.api.jmlc.Connection.convertToStringFrame("test",1,1)

#print(conn)
#dml = "print('Hello World')"
#input = jvmfoo.org.tugraz.sysds.runtime.util.toString()
#string_class = java_gateway.jvm.String
#input = jvmfoo.org.tugraz.sysds.pythonapi.pythonDMLScript.createString()
#output = jvmfoo.org.tugraz.sysds.pythonapi.pythonDMLScript.createString()

#print(input)
#print(output)
#a = 2.0
#output = jvmfoo.org.tugraz.sysds.runtime.util.DataConverter.toString(a)
#print(input)
#boolean = False
#print(dml)

#conn.prepareScript(dml, input, output)

#conn.prepareScript(dml, input, output)
#script = conn.prepareScript(dml,'','')
#print(script)

#Connection conn = new Connection();
#String dml = "print('hello world');";
#PreparedScript script = conn.prepareScript(dml, new String[0], new String[0], false);
#script.executeScript();