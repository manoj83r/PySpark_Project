from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import year, month

# Req-5:Display the customer name who made the Nth most purchases in the specific month.Refunds should be excluded.
def cust_with_n_rank_purchase(salesDF, refundDF, customerDF, cust_rank=1, monthVal = '12', yearVal = '9999'):
    sales_refunded_df = salesDF.where((year(salesDF.timestamp) == yearVal) &
                                                (month(salesDF.timestamp) == monthVal)).\
            join(refundDF, salesDF.transaction_id == refundDF.original_transaction_id,
                                                      "left").select(salesDF.transaction_id.alias("transaction_id1"),
                                                                     salesDF.customer_id.alias("customer_id1"),
                                                                     salesDF.timestamp.alias("timestamp1"),
                                                                     refundDF.original_transaction_id.alias(
                                                                         "original_transaction_id2"))
    sales_may2013_not_refunded_df = sales_refunded_df.where(sales_refunded_df.original_transaction_id2.isNull()).\
                                        select(sales_refunded_df.transaction_id1,
                                                sales_refunded_df.customer_id1,
                                                (year(sales_refunded_df.timestamp1)).alias("col_year"))

    cust_sales_may2013count_df = sales_may2013_not_refunded_df.join(customerDF,
                                                        sales_may2013_not_refunded_df.customer_id1 == customerDF.
                                                        customer_id, 'inner'). \
                                                select(customerDF.customer_id,
                                                       customerDF.customer_first_name,
                                                       customerDF.customer_last_name,
                                                       sales_may2013_not_refunded_df.col_year,
                                                       sales_may2013_not_refunded_df.transaction_id1). \
        groupBy(customerDF.customer_id, customerDF.customer_first_name,
                customerDF.customer_last_name, sales_may2013_not_refunded_df.col_year). \
        agg(f.count(sales_may2013_not_refunded_df.transaction_id1).alias("transaction_count"))

    spec = Window.partitionBy(cust_sales_may2013count_df.col_year). \
        orderBy(cust_sales_may2013count_df.transaction_count.desc())
    custSalesMay2013CountRanked_DF = cust_sales_may2013count_df.withColumn('rnk', dense_rank().over(spec))
    secondMostPurchaseMay2013DF = custSalesMay2013CountRanked_DF.where(custSalesMay2013CountRanked_DF.rnk == cust_rank)
    secondMostPurchaseMay2013DF.show(5)
    # secondMostPurchaseMay2013DF.write.saveAsTable("manoj_spark1.second_most_purchase_may2013")'''