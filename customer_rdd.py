import sys
import configparser as cp
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

props = cp.RawConfigParser()
props.read("resources/application.properties")
env = sys.argv[1]
print(sys.argv[1])
print(env+".execution.mode")
print(props.get(env,env+".execution.mode"))
conf = SparkConf(). \
    setAppName("Customer_Details_App"). \
    setMaster(props.get(env,env+".execution.mode"))

sc = SparkContext(conf=conf)
iniData = sc.parallelize(
    [("Spark", 1), ("Scala", 2), ("Spark", 1), ("Spark", 4), ("Spark", 8), ("Java", 9), ("Scala", 5), ("Spark", 6)])

spark = SparkSession. \
    builder. \
    appName("Customer_Details_App"). \
    master(sys.argv[1]). \
    enableHiveSupport(). \
    getOrCreate()
'''
list = [("john", 19), ("Smith", 29), ("Adam", 35), ("Henry", 40)]
rdd1 = sc.parallelize(list)
data = rdd1.map(lambda x: Row(name=x[0], xage=int(x[1])))
df = spark.createDataFrame(data, ["Employee"])
df.show()
df.printSchema()

df = sc.parallelize([("Raja", 21, "HYD"), ("Ramya", 34, "BAN"), ("Rani", 30, "MUM")]).toDF(["name", "age", "address"])
df.show()

df.where("age>30").show()
'''
schemaCustomer = StructType([StructField("customer_id", IntegerType(), False),
                             StructField("first_name", StringType(), True),
                             StructField("last_name", StringType(), True),
                             StructField("phone_number", StringType(), True)])

customerDF = spark.read. \
    load("Dataset-Customer.txt", format="csv", sep="|", header=False, schema=schemaCustomer)
customerDF.show()
# reduceSumOfIniData = iniData.groupByKey().mapValues(len) #  ( lambda x,y : (1+1))
# reduceSumOfIniData = iniData.map(lambda x:x[0]).map(lambda y:(y,1)).reduceByKey(lambda a,b:a+b)
# reduceSumOfIniData = iniData.sortBy(lambda x: x, True)
# print(reduceSumOfIniData.collect())


data1 = sc.parallelize([ ("Spark",10),("Scala",20),("Java",30) ])
data2 = sc.parallelize([ ("Phython","PaltformDep"),("Scala","JVM"),("Spark","Cache") ])
jd = data1.fullOuterJoin(data2)
data1.red

#joinData1 = reduceSumOfIniData.join(data1)

'''print(joinData.collect())'''
# print(joinData1.collect())

iniData = sc.parallelize(
    [("Spark", 1), ("Scala", 2), ("Spark", 3), ("Spark", 4), ("Spark", 8), ("Java", 9), ("Scala", 5), ("Spark", 6)])
countOfKeys = iniData.lookup("Spark")
print(countOfKeys)
'''Max and Min
maxByKeyData = iniData.reduceByKey(lambda x,y:max(x,y))
minByKeyData = iniData.reduceByKey(lambda x,y:min(x,y))'''

# groupSumOfIniData = iniData.groupByKey()
# print((groupSumOfIniData.keys().collect(),groupSumOfIniData.mapValues().collect()))
'''cust_extRDD = sc.textFile("Dataset-Customer.txt",5)
#print(cust_extRDD.groupBy(lambda word:word[0:4]).collect())
cust_len_extRDD = cust_extRDD.map(lambda l:len(l))
print(cust_len_extRDD.collect())
a = cust_len_extRDD.collect()
print(type(a))
print(cust_len_extRDD.zipWithIndex().collect())
print(cust_len_extRDD.zipWithIndex().groupByKey().collect())
print(cust_len_extRDD.zipWithIndex().reduceByKey().collect())'''
# print(cust_len_extRDD.zipWithIndex().groupBy(1).collect())
# cust_len_extRDD.zipWithIndex().coalesce(1).saveAsTextFile("Output.txt")


'''cust_line_extRDD= cust_extRDD.map(lambda l:(l,len(l),1.zipWithIndex() ))
cust_273 = cust_line_extRDD.filter(lambda l: l[1]==37).map(lambda a:(a[0],a[2]))
cust_flatMap = cust_extRDD.flatMap(lambda l:l.split("|"))
a = cust_273.collect()
print(a)
print(cust_flatMap.collect())
print(cust_extRDD.getNumPartitions() )
print(cust_273.getNumPartitions())
print(cust_flatMap.getNumPartitions())
print(cust_len_extRDD.collect())
print(cust_len_extRDD.getNumPartitions())
print(cust_len_extRDD.mapPartitions(adder).collect())
print(cust_len_extRDD.sum())'''

'''tech = sc.parallelize(["Spark","Scala","Java"])
technew = sc.parallelize(["Perl","Scala","Phython"])
unionTech = tech.zip(technew)
print(unionTech.collect() )

names = sc.parallelize(["AAA","BBB","CCC","DDD"])
zipWithIndexNames = names.zipWithIndex()
print(zipWithIndexNames.collect())

data=sc.parallelize(range(6,100))
print(data.collect())'''

'''log4jLogger = sc._jvm.org.apache.log4j
sc.setLogLevel("INFO")
log = log4jLogger.LogManager.getLogger("CustData")
log.warn("warning message")
log.info("hello info")
log.info(str(cust_273))
'''
