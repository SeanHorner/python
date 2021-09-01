from pyspark import SparkConf, SparkContext
sc = SparkContext(master="local", appName="Spark Demo")
print(sc.textFile("D:/programming_projects/pycharm_projects/pyspark_training/deckofcards.txt").first())

# output will be:
# "BLACK|SPADE|2"
