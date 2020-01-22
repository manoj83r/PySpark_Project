from pyspark.sql.functions import year

# Req-4:Calculate the total amount of all transactions that happened in 2013 and have not been refunded as of today
def total_trans_not_refunded(salesDF, refundDF, yearValue='9999'):
    sales2013_df = salesDF.where(year(salesDF.timestamp) == yearValue)
    # Join Sales table with Refund Table
    sales2013_not_refunded = sales2013_df.join(refundDF, salesDF.transaction_id == refundDF.original_transaction_id,
                                            "left")
    # Get Not Refunded Transaction by checking trans id in refunded table is Null
    sales2013_not_refunded = sales2013_not_refunded.where(sales2013_not_refunded.original_transaction_id.isNull())
    tot_amt_sales2013_not_refunded = \
        sales2013_not_refunded.select(sales2013_not_refunded.total_amount).groupBy().sum().collect()[0][0]
    print("Total amount 2013 transactions that have not been refunded=" + str(tot_amt_sales2013_not_refunded))
