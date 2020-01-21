from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, unix_timestamp, from_unixtime, year, month

if __name__ == "__main__":
    spark = SparkSession.builder.appName("app1").master("local").enableHiveSupport().getOrCreate()
    # spark = SparkSession.builder.appName("app").master("yarn").enableHiveSupport().getOrCreate()

    # Load Product data to product Data Frame
    productSchema = StructType([StructField("product_id", IntegerType(), False),
                                StructField("product_name", StringType(), False),
                                StructField("product_type", StringType(), False),
                                StructField("product_version", StringType(), False),
                                StructField("product_price", StringType(), False)])
    productDF = spark.read.load("Dataset-Product.txt", format="csv", sep="|", header=False, schema=productSchema)
    productDF = productDF.withColumn("product_price",
                                     regexp_replace(productDF.product_price, '\$', '').cast(IntegerType()))
    productDF.show(5)

    # Load Customer data to product Data Frame
    customerSchema = StructType([StructField("customer_id", IntegerType(), False),
                                 StructField("customer_first_name", StringType(), True),
                                 StructField("customer_last_name", StringType(), True),
                                 StructField("phone_number", StringType(), True)])
    customerDF = spark.read.load("Dataset-Customer.txt", format="csv", sep="|", header=False,
                                 schema=customerSchema)
    customerDF.show(5)

    # Load Sales data to product Data Frame
    salesSchema = StructType([StructField("transaction_id", IntegerType(), False),
                              StructField("customer_id", IntegerType(), False),
                              StructField("product_id", IntegerType(), False),
                              StructField("timestamp", StringType(), False),
                              StructField("total_amount", StringType(), False),
                              StructField("total_quantity", IntegerType(), False)])
    salesDF = spark.read.load("Dataset-Sales.txt", format="csv", sep="|", header=False, schema=salesSchema)
    salesDF = salesDF.withColumn("total_amount", regexp_replace(salesDF.total_amount, "\$", "").cast(IntegerType())). \
        withColumn("timestamp",
                   from_unixtime(unix_timestamp(salesDF.timestamp, 'MM/dd/yy HH:mm:SS')).cast(TimestampType()))
    salesDF.show(5)

    # Load Refund data to product Data Frame
    refundSchema = StructType([StructField('refund_id', IntegerType(), False),
                               StructField('original_transaction_id', IntegerType(), False),
                               StructField('customer_id', IntegerType(), False),
                               StructField('product_id', IntegerType(), False),
                               StructField('timestamp', StringType(), False),
                               StructField('refund_amount', StringType(), False),
                               StructField('refund_quantity', IntegerType(), False)])
    refundDF = spark.read.load("Dataset-Refund.txt", format="csv", sep="|", header=False, schema=refundSchema)
    refundDF = refundDF.withColumn('timestamp',
                                   from_unixtime(unix_timestamp(refundDF.timestamp, "MM/dd/yy HH:mm:ss")).cast(
                                       TimestampType())). \
        withColumn('refund_amount', regexp_replace(refundDF.refund_amount, '\$', '').cast(IntegerType()))
    refundDF.show(5)

    distOfSales_DF = salesDF.join(productDF, salesDF.product_id == productDF.product_id, 'outer'). \
        select(productDF.product_name, productDF.product_type, salesDF.product_id)
    distOfSales_DF.printSchema()

    # Question2: Display the distribution of sales by product name and product type.
    distOfSales_DF_count = distOfSales_DF.groupBy(distOfSales_DF.product_name, distOfSales_DF.product_type).count()
    distOfSales_DF_count.coalesce(1).write.save("DistOfSales_count", format="csv", mode="overwrite",
                                                header=True, sep=",")
    print("Distribution of sales by product name and product type is stored "
          "in the Directory DistOfSales_count in the csv format")

    # Question3: Get the Top 100 customers who are dosing the most of the transactions and store those
    # tables in hive table to give it to Downstream
    saleByCustomerDF = salesDF.join(customerDF, salesDF.customer_id == customerDF.customer_id, "left"). \
        select(salesDF.transaction_id, salesDF.customer_id, customerDF.customer_first_name,
               customerDF.customer_last_name)
    saleByCustomerDF.printSchema()
    saleByCustomerDF.show(5)
    transCountByCustDF = saleByCustomerDF. \
        groupBy(saleByCustomerDF.customer_first_name, saleByCustomerDF.customer_last_name).count()
    transCountByCustDF.printSchema()
    transCountByCustDF.show(5)
    tCountTop100CustDF = transCountByCustDF.orderBy("count", ascending=False).limit(100)
    # Uncomment below line while running in cluster
    tCountTop100CustDF.write.saveAsTable("manoj_spark1.top_100_customer_transaction", mode="overwrite",
                                         partitionBy=None)

    # Question4:Calculate the total amount of all transactions that happened in year 2013
    # and have not been refunded as of today.
    sales2013DF = salesDF.where(year(salesDF.timestamp) == '2013')
    # Join Sales table with Refund Table
    sales2013NotRefunded = sales2013DF.join(refundDF, salesDF.transaction_id == refundDF.original_transaction_id,
                                            "left")
    # Get Not Refunded Transaction by checking trans id in refunded table is Null
    sales2013NotRefunded = sales2013NotRefunded.where(sales2013NotRefunded.original_transaction_id.isNull())
    totAmtSales2013NotRefunded = \
        sales2013NotRefunded.select(sales2013NotRefunded.total_amount).groupBy().sum().collect()[0][0]
    print("Total amount 2013 transactions that have not been refunded=" + str(totAmtSales2013NotRefunded))

    # Question5:Display the customer name who made the second most purchases in the month of May2013.
    # Refunds should be excluded.
    salesMay2013DF = salesDF.where((year(salesDF.timestamp) == '2013') & (month(salesDF.timestamp) == '05'))
    salesMay2013NotRefunded = salesMay2013DF.join(refundDF, salesDF.transaction_id == refundDF.original_transaction_id,
                                                  "left").select(salesMay2013DF.transaction_id.alias("transaction_id1"),
                                                                 salesMay2013DF.customer_id.alias("customer_id1"),
                                                                 salesMay2013DF.product_id.alias("product_id1"),
                                                                 salesMay2013DF.timestamp.alias("timestamp1"),
                                                                 salesMay2013DF.total_amount.alias("total_amount1"),
                                                                 salesMay2013DF.total_quantity.alias("total_quantity1"),
                                                                 refundDF.refund_id.alias("refund_id2"),
                                                                 refundDF.original_transaction_id.alias(
                                                                     "original_transaction_id2"),
                                                                 refundDF.customer_id.alias("customer_id2"),
                                                                 refundDF.product_id.alias('product_id2'),
                                                                 refundDF.timestamp.alias('timestamp2'),
                                                                 refundDF.refund_amount.alias('refund_amount2'),
                                                                 refundDF.refund_quantity.alias('refund_quantity2'))
    salesMay2013NotRefunded = salesMay2013NotRefunded.where(salesMay2013NotRefunded.original_transaction_id2.isNull())
    salesMay2013NotRefundedDF = salesMay2013NotRefunded.select(salesMay2013NotRefunded.transaction_id1,
                                                               salesMay2013NotRefunded.customer_id1,
                                                               (year(salesMay2013NotRefunded.timestamp1)).alias(
                                                                   "col_year"))
    custSalesMay2013DF = salesMay2013NotRefundedDF.join(customerDF,
                                                        salesMay2013NotRefundedDF.customer_id1 == customerDF.
                                                        customer_id, 'inner'). \
        select(customerDF.customer_id,
               customerDF.customer_first_name,
               customerDF.customer_last_name,
               salesMay2013NotRefundedDF.col_year,
               salesMay2013NotRefundedDF.transaction_id1)
    custSalesMay2013DF.show(5)
    custSalesMay2013count_DF = custSalesMay2013DF.groupBy(customerDF.customer_id, customerDF.customer_first_name,
                                                          customerDF.customer_last_name, custSalesMay2013DF.col_year). \
        agg(f.count(salesMay2013NotRefundedDF.transaction_id1).alias("transaction_count"))
    custSalesMay2013count_DF.show(5)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank

    spec = Window.partitionBy(custSalesMay2013count_DF.col_year). \
        orderBy(custSalesMay2013count_DF.transaction_count.desc())
    custSalesMay2013CountRanked_DF = custSalesMay2013count_DF.withColumn('rnk', dense_rank().over(spec))
    secondMostPurchaseMay2013DF = custSalesMay2013CountRanked_DF.where(custSalesMay2013CountRanked_DF.rnk == 2)
    secondMostPurchaseMay2013DF.show(5)
    secondMostPurchaseMay2013DF.write.saveAsTable("manoj_spark1.second_most_purchase_may2013")

    # Question6:Find a product that has not been sold at least once (if any).
    distinct_soldProdidDF = salesDF.select(salesDF.product_id.alias("sold_prod_id")).distinct()
    distinct_soldProdidDF.show(5)
    prodSalesJoinDF = productDF.join(distinct_soldProdidDF,
                                     productDF.product_id == distinct_soldProdidDF.sold_prod_id, "left")
    prodNotSoldDF = prodSalesJoinDF.where(prodSalesJoinDF.sold_prod_id.isNull())
    prodNotSoldDF.show(5)
    prodNotSoldDF.write.saveAsTable("manoj_spark1.product_not_sold", mode="overwrite", partitionBy=None)

    # Question7:Calculate the total number of users who purchased the same product
    # consecutively at least 2 times on a given day.
    salesCustDateGrpDF = salesDF.groupBy(salesDF.customer_id, salesDF.timestamp.cast(DateType())). \
        agg(f.count(salesDF.transaction_id).alias("transCount"))
    numOfUserConsPur = salesCustDateGrpDF.where(salesCustDateGrpDF.transCount > 1).count()
    print("Number of User did Consecutive Purchase of Same Product on same day=" + str(numOfUserConsPur))

    # Question8:Calculate the total Sale happened year wise and store that data in Hive Tables to give to downstream
    saleCountDF = salesDF.groupBy((year(salesDF.timestamp)).alias("sale_year")). \
        agg(f.count(salesDF.transaction_id).alias("sale_count")).orderBy("sale_year")
    saleCountDF.show()
    saleCountDF.write.saveAsTable("manoj_spark1.sale_count_yearwise", mode="overwrite", partitionBy=None)
