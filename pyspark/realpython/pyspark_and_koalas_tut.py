import pandas as pd
from databricks import koalas as ks
from pyspark.sql import SparkSession, functions as F

# define the data - example taken from https://koalas.readthedocs.io/en/latest/getting_started/10min.html
data = {'a': [1, 2, 3, 4, 5, 6],
        'b': [100, 200, 300, 400, 500, 600],
        'c': ["one", "two", "three", "four", "five", "six"]}

index = [10, 20, 30, 40, 50, 60]

pdf = pd.DataFrame(data, index=index)

pdf.head()
#     a    b      c
# 10  1  100    one
# 20  2  200    two
# 30  3  300  three
# 40  4  400   four
# 50  5  500   five


pdf.groupby(['a']).sum()
#      b
# a
# 1  100
# 2  200
# 3  300
# 4  400
# 5  500
# 6  600


# start from raw data
kdf = ks.DataFrame(data, index=index)

# or from a pandas dataframe
kdf2 = ks.from_pandas(pdf)

kdf.head()  # same for kdf2.head()
#     a    b      c
# 10  1  100    one
# 20  2  200    two
# 30  3  300  three
# 40  4  400   four
# 50  5  500   five

kdf.groupby(['a']).sum()
#      b
# a
# 1  100
# 2  200
# 3  300
# 4  400
# 5  500
# 6  600

# vs
spark_session = SparkSession.builder \
    .appName('KoalasTest') \
    .getOrCreate()

# add the index as a column, since we cannot set the index to a spark dataframe
# (not easily at least,
# see https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6 )
spark_data = data.copy()
spark_data.update({'index': index})

# we need to transform the spark_data from a columnary format to a row format
# basically perform a transpose
n_rows = len(list(spark_data.values())[0])
spark_df_data = [{} for i in range(n_rows)]
for k, v in spark_data.items():
    for i, s in enumerate(v):
        print(i, s)
        spark_df_data[i][k] = s

"""
which results to:
[{
  'a': 1,
  'b': 100,
  'c': 'one',
  'index': 10
}, {
  'a': 2,
  'b': 200,
  'c': 'two',
  'index': 20
}, {
  'a': 3,
  'b': 300,
  'c': 'three',
  'index': 30
}, {
  'a': 4,
  'b': 400,
  'c': 'four',
  'index': 40
}, {
  'a': 5,
  'b': 500,
  'c': 'five',
  'index': 50
}, {
  'a': 6,
  'b': 600,
  'c': 'six',
  'index': 60
}]
"""

sdf = spark_session.createDataFrame(spark_df_data)

# or

sdf2 = spark_session.createDataFrame(pdf)
sdf2.show()
# +---+---+-----+
# |  a|  b|    c|
# +---+---+-----+
# |  1|100|  one|
# |  2|200|  two|
# |  3|300|three|
# |  4|400| four|
# |  5|500| five|
# |  6|600|  six|
# +---+---+-----+

sdf.show()
# +---+---+-----+-----+
# |  a|  b|    c|index|
# +---+---+-----+-----+
# |  1|100|  one|   10|
# |  2|200|  two|   20|
# |  3|300|three|   30|
# |  4|400| four|   40|
# |  5|500| five|   50|
# |  6|600|  six|   60|
# +---+---+-----+-----+

sdf.groupBy('a').sum().show()
# +---+------+------+----------+
# |  a|sum(a)|sum(b)|sum(index)|
# +---+------+------+----------+
# |  6|     6|   600|        60|
# |  5|     5|   500|        50|
# |  1|     1|   100|        10|
# |  3|     3|   300|        30|
# |  2|     2|   200|        20|
# |  4|     4|   400|        40|
# +---+------+------+----------+

sdf.to_koalas()
# 19/10/28 12:29:02 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
#    a    b      c  index
# 0  1  100    one     10
# 1  2  200    two     20
# 2  3  300  three     30
# 3  4  400   four     40
# 4  5  500   five     50
# 5  6  600    six     60

# given an index column - which does not trigger the warning about no partition defined
sdf.to_koalas('index')
#        a    b      c
# index
# 10     1  100    one
# 20     2  200    two
# 30     3  300  three
# 40     4  400   four
# 50     5  500   five
# 60     6  600    six
