from pyspark import SparkContext
logFile = "D:/programming_projects/pycharm_projects/pyspark_training/test_text.txt"
sc = SparkContext("local", "first app")
sc.setLogLevel("ERROR")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
