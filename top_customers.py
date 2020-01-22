# Req-3: Get the Top 100 customers who are dosing the most of the transactions and store those in hive table
# to give it to Downstream
def top_cust_trans(salesDF, customerDF, limit_value=0):
    trans_count_by_cust_df = salesDF.join(customerDF, salesDF.customer_id == customerDF.customer_id, "left"). \
        select(salesDF.transaction_id, salesDF.customer_id, customerDF.customer_first_name,
               customerDF.customer_last_name). \
        groupBy('customer_first_name', 'customer_last_name').count()
    top100_cust_df = trans_count_by_cust_df.orderBy("count", ascending=False).limit(limit_value)
    top100_cust_df.show(10)
    # Uncomment below line while running in cluster
    # top100_cust_df.write.saveAsTable("manoj_spark1.top_100_customer_transaction", mode="overwrite",
    #                                   partitionBy=None)
