def dist_of_sales(salesDF, productDF):
    dist_of_sales_df = salesDF.join(productDF, salesDF.product_id == productDF.product_id, 'outer'). \
        select(productDF.product_name, productDF.product_type, salesDF.product_id)
    dist_of_sales_df.printSchema()

    dist_of_sales_df_count = dist_of_sales_df.groupBy(dist_of_sales_df.product_name,
                                                      dist_of_sales_df.product_type).count()
    dist_of_sales_df_count.coalesce(1).write.save("DistOfSales_count", format="csv", mode="overwrite",
                                                  header=True, sep=",")
    print("Distribution of sales by product name and product type is stored "
          "in the Directory DistOfSales_count in the csv format")
