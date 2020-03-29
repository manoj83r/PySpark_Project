from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Cust_orc").master("local").enableHiveSupport().getOrCreate()
    cust_schema = StructType([StructField("customer_id", StringType(), False),
                              StructField("age", IntegerType(), False),
                              StructField("customer_segment", StringType(), False,
                              StructField("customer_vintage_group", StringType(), False))])
    cust_df = spark.read.load("000000_0", format="orc", sep=",",  header=True)
    #cust_df.schema(schema = cust_schema)
    # cust1_df= spark.createDataFrame(data=cust_df, schema=cust_schema)
    cust1_df = cust_df.select(cust_df._col0.alias("customer_id"),
                              cust_df._col1.alias("age"),
                              cust_df._col2.alias("customer_segment"),
                              cust_df._col3.alias("customer_vintage_group"))
    cust1_df.printSchema()
    cust1_df.show(10)
    cust2_df = cust1_df.withColumn("newage",cust1_df.age+5)
    cust2_df.coalesce(1).write.format("orc").save("CustBase_Out", mode="overwrite",
                                                header=True, sep=",")
    cust3_df = spark.read.load("CustBase_Out",format="orc", sep=",", header = True)
    print("This from written to orc")
    cust3_df.show(10)
    # cust1_df.write.format("orc").save("/user/manoj83r/Cust_23rdMar/main_table_dir/CustBase_Out")

'''cust_df= spark.read.load("/user/manoj83r/Cust_23rdMar/main_table_dir/CustomerBase_R/000000_0", format= "orc", sep=",", inferSchema=True, header=True)
/user/manoj83r/Cust_23rdMar/main_table_dir/CustomerBase_R/000000_0

cust_df= spark.read.load("/user/manoj83r/Cust_23rdMar/main_table_dir/CustomerBase_R/000000_0", format= "orc", sep=",", inferSchema=True,header=True)




from pyspark import SparkContext, SQLContext
sc = SparkContext("yarn", "SQL App")
sqlContext = SQLContext(sc)
df = sqlContext.read.format('orc').load('/user/manoj83r/Cust_23rdMar/main_table_dir/CustomerBase_R/000000_0')'''
