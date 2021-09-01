from functools import reduce
from pyspark import SparkContext
from pyspark.sql import SparkSession

# The basic building block of big data analysis with tools like PySpark is parallel code, code that is easy to run
# in multiple pieces on multiple machines. Parallel code is most often written in the functional paradigm. Functional
# programming's core idea is that functions should take in data, output their result, and not create any state changes.
# This means that code should avoid using global variables and always returns new data rather than modify data in place.
# One of the best ways to achieve this level of pureness in functions is the use of anonymous functions, which are
# called using the lambda keyword in Python. Anonymous functions are written inline with code and used once by the
# program. An example follows:
x = ['Python', 'programming', 'is', 'fun!']
print(sorted(x))
#     ['Python', 'fun!', 'is', 'programming']
print(sorted(x, key=lambda arg: arg.lower()))
#     ['fun!', 'is', 'programming', 'Python']
# Here the keyword 'lambda' is called, telling the sorted() method that an inline anonymous function needs to be applied
# in conjunction with the sort. Some other Python functions that take the keyword lambda are filter(), map(), and
# reduce(). All of them can take either anonymous functions or externally defined functions, including user functions.
# Here's a example of the filter function (using the previously defined list of words):
print(list(filter(lambda arg: len(arg) > 8, x)))
#     ['programming']
# The filter() function takes a lambda (or predefined function), applies it to every item in the iterable that is
# passed (here, a list), and returns only items from the iterable which pass the lambda.

# The list() wrapper is necessary because filter() returns an iterable, that is each item comes in as the filter
# loop is passed through. The list() wrapper collects all of the items into memory and returns them once the filter is
# finished. But this iterable behavior can also be quite useful. Since it returns each item one at a time, the machine
# running the code doesn't need to have enough RAM to hold all of the results at once, an extremely useful behavior
# for dealing with extremely large datasets.

# The map() function takes a lambda and an iterable, applies the function to each item in the iterable, and returns the
# modified items. For example:
print(list(map(lambda arg: arg.upper(), x)))
#     ['PYTHON', 'PROGRAMMING', 'IS', 'FUN!']

# Unlike the other functions, the reduce() function doesn't return a new iterable, it reduces all of the values in the
# passed iterable down to a single value. The passed function should take two variables, for example:
print(reduce(lambda val1, val2: val1 + val2, x))
#     Pythonprogrammingisfun!

# Another important paradigm is the use of Sets, because they can hold information like a list, but ensure the
# distinctness of each element, meaning they help reduce memory usage in big data situations.

# PySpark leverages these parallelizations to split up work among clusters of computers. A simple PySpark example is as
# follows:
sc = SparkContext("local[4]", "Tut App")

txt = sc.textFile('file:////usr/share/doc/python/copyright')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())

# The beginning point for every Spark program is setting up the SparkContext object. It is an object that allows the
# program to interact with and leverage the resources of a Spark cluster. The 'local[*]' parameter tells Spark that the
# cluster is the local machine and to create as many worker threads as there are cores in the machine's CPU. This is
# about the most simple a SparkConfiguration as can be passed. More complicated SparkConfigurations, e.g. ones dealing
# with clusters, can be crafted piecemeal by building a SparkConf (blocked to avoid problems):
#   conf = pyspark.SparkConf()
#   conf.setMaster('spark://head_node:56887')
#   conf.set('spark.authenticate', True)
#   conf.set('spark.authenticate.secret', 'secret-key')
#   sc = pyspark.SparkContext(conf=conf)

# Once a SparkContext has been created, the program can utilize RDDs. Most often RDDs are created using PySpark's
# parallelize() function, which takes a collection and converts it into an RDD. The following code snip-it shows some
# RDD functionality:
big_list = range(10000)
rdd = sc.parallelize(big_list)
odds = rdd.filter(lambda arg: arg % 2 != 0)
print(odds.take(5))
#     [1, 3, 5, 7, 9]
# This example uses the take() function to grab a small subset of numbers for testing/quality assurance. It's very
# very useful especially when the dataset is too large for a single machine's RAM to hold, so checking the entire
# dataset would be impossible.

# Another commonly used method to create RDDs (especially of larger datasets) is to read them in from file with the
# SparkContext's textFile() function.

# After this, PySpark reacts a lot like Spark does normally.
