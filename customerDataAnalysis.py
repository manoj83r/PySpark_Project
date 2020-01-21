from pysession import SparkSess
from base_df_create import CreateDF
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import year
import dist_of_sales
import top_customers
import total_trans_without_refund
import cust_details_most_purchase


if __name__ == "__main__":
    spark = SparkSess().session_create("CustDataAnalysis", "LOCAL")

    createDF = CreateDF()
    # Load Product data to product Data Frame
    productDF = createDF.product_df(spark)
    productDF.show(5)
    # Load Customer data to product Data Frame
    customerDF = createDF.customer_df(spark)
    customerDF.show(5)
    # Load Sales data to product Data Frame
    salesDF = createDF.sales_df(spark)
    salesDF.show(5)
    # Load Refund data to product Data Frame
    refundDF = createDF.refund_df(spark)
    refundDF.show(5)

    # Question2: Display the distribution of sales by product name and product type.
    dist_of_sales.dist_of_sales(salesDF, productDF)
    # Question3: Get the Top 100 customers who are dosing the most of the transactions and store those
    # tables in hive table to give it to Downstream
    top_customers.top_cust_trans(salesDF, customerDF, 100)
    # Question4:Calculate the total amount of all transactions that happened in year 2013 and
    # have not been refunded as of today.
    total_trans_without_refund.total_trans_not_refunded(salesDF, refundDF, '2013')
    # Question5:Display the customer name who made the second most purchases in the month of May2013.
    # Refunds should be excluded.
    cust_details_most_purchase.cust_with_n_rank_purchase(salesDF, refundDF, customerDF, 2, '05', '2013')
    # Question6:Find a product that has not been sold at least once (if any). Get Distinct product id
    distinct_soldProdidDF = salesDF.select(salesDF.product_id.alias("sold_prod_id")).distinct()
    prodSalesJoinDF = productDF.join(distinct_soldProdidDF,
                                     productDF.product_id == distinct_soldProdidDF.sold_prod_id, "left")
    prodNotSoldDF = prodSalesJoinDF.where(prodSalesJoinDF.sold_prod_id.isNull())
    prodNotSoldDF.show(5)
    # prodNotSoldDF.write.saveAsTable("manoj_spark1.product_not_sold", mode="overwrite", partitionBy=None)

    # Question7:Calculate the total number of users who purchased the same product at least 2 times on a given day.
    salesCustDateGrpDF = salesDF.groupBy(salesDF.customer_id, salesDF.timestamp.cast(DateType())). \
        agg(f.count(salesDF.transaction_id).alias("transCount"))
    numOfUserConsPur = salesCustDateGrpDF.where(salesCustDateGrpDF.transCount > 1).count()
    print("Number of User did Consecutive Purchase of Same Product on same day=" + str(numOfUserConsPur))

    # Question8:Calculate the total Sale happened year wise and store that data in Hive Tables to give to downstream
    saleCountDF = salesDF.groupBy((year(salesDF.timestamp)).alias("sale_year")). \
        agg(f.count(salesDF.transaction_id).alias("sale_count")).orderBy("sale_year")
    saleCountDF.show()
    # saleCountDF.write.saveAsTable("manoj_spark1.sale_count_yearwise", mode="overwrite", partitionBy=None)