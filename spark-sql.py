# filename: spark-sql.py
# requied file: fiends.csv
# Prerequisites: Spark 1.6, Python 2.7

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import collections

conf = SparkConf().setMaster("local").setAppName("SparkSQL")
sc = SparkContext(conf = conf)
# Here gets in the world of SparkSQL #
sqlContext = SQLContext(sc) 

# this function creates/extends row objects of a "Dataframe" object
def mapper(line):
    fields = line.split(',')
    return Row( ID=int(fields[0]), name=fields[1].encode("utf-8"), age=int(fields[2]), numFriends=int(fields[3]) )

print "Reading friends.csv to rdd..."
lines = sc.textFile("friends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
print "Printing teenagers from the results..."
for teen in teenagers.collect():
  print(teen)
  
print "SparkSQL completed."
