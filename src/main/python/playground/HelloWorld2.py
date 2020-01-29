import os
import sys
import subprocess
from py4j.java_gateway import JavaGateway
# import numpy as np
path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..\\..\\..\\")
sys.path.insert(0, path)
path2 = os.path.realpath(__file__)
path3 = os.path.dirname(os.path.realpath(__file__))
path4 = os.path.dirname(path3)
path5 = os.path.join(os.path.dirname(path4), "java/org/tugraz/sysds/api/DMLScript.java")
#print(path2)
#print(path3)
#print(path4)
#print(path5)

#path6 = "C:\\Richard\\002_SystemDS\\pythonwrapper2\\systemds\\src\\main\\java\\org\\tugraz\\sysds\\api\\DMLScript"
#path7 = "C:\\Richard\\002_SystemDS\\pythonwrapper2\\systemds\\target\\SystemDS.jar "
#os.system("java " + path6)
#print(path6)
#from py4j.java_gateway import JavaGateway

#os.chdir("C:\\Richard\\002_SystemDS\\pythonwrapper2\\systemds\\target")
#os.system("CMD")
#subprocess.call("java -cp 'lib/*;SystemDS.jar' org.tugraz.sysds.api.DMLScript org.tugraz.sysds.api.DMLScript")
#subprocess.run('dir', shell=True)
#subprocess.run('java -cp "lib/*;SystemDS.jar" org.tugraz.sysds.api.DMLScript -config conf\SystemDS-config.xml.template ', shell=True, cwd = "C:\\Richard\\002_SystemDS\\pythonwrapper2\\systemds\\target")
subprocess.run('java -cp "lib/*;SystemDS.jar" org.tugraz.sysds.api.DMLScript -s "print("""HelloWorld""")" ', shell=True, cwd = "C:\\Richard\\002_SystemDS\\pythonwrapper2\\systemds\\target")


#subprocess.run()
#gateway.entry_point.readDMLScript("print('Hello World')")



#gateway.entry_point.executeScript("print('Hello World')")

#os.system("java -cp 'lib/*;SystemDS.jar' org.tugraz.sysds.api.DMLScript")
#print(path7)
#os.system("CMD")

#gg = JavaGateway.launch_gateway(classpath="target/lib/py4j-0.10.4.jar")
#gg.jvm.org.tugraz.sysds.api.DMLScript.java.DMLScript()


#dml("print('Hello World')")


# from pyspark.context import SparkContext
#
# from systemds import matrix, MLContext, dml, dmlFromResource, dmlFromFile, dmlFromUrl
#
# # Spark entrypoint
# sc = SparkContext.getOrCreate()
#
# # Making Available the systemds dml syntax
# ml = MLContext(sc)
# helloScript = dml("print('Hello World')")
# ml.execute(helloScript)
#
# k = np.ones((3,3))
# print(m)
# print(k)

