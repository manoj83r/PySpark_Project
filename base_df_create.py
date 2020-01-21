from pyspark.sql.types import *
from schema_file import SchemaFile as sf
from pyspark.sql.functions import regexp_replace, unix_timestamp, from_unixtime

class CreateDF:
    def __init__(self):
        pass

    @staticmethod
    def data_frame_create(spark, directory=None, file_name=None, file_format=None,
                          sep=",", headerAvailable=False, schemaName=None):
        try:
            data_frame = spark.read.load(file_name, format=file_format, sep=sep,
                                         header=headerAvailable, schema=schemaName)
            print("Data Frame create successfully for:",file_name)
            return data_frame
        except:
            print("Problem in creating Data Frame for:",file_name)

    # UDF to Load Product data to product Data Frame
    def product_df(self, spark):
        product_tmp_df = self.data_frame_create(spark, None, "Dataset-Product.txt", "csv", "|", False, sf.productSchema)
        product_df = product_tmp_df.withColumn("product_price", regexp_replace(product_tmp_df.product_price, '\$', ''). \
                                            cast(IntegerType()))
        return product_df

    # Load Customer data to product Data Frame
    def customer_df(self, spark):
        customer_df = self.data_frame_create(spark, None, "Dataset-Customer.txt", "csv", "|", False, sf.customerSchema)
        return customer_df

    # Load Sales data to product Data Frame
    def sales_df(self, spark):
        sales_tmp_df = self.data_frame_create(spark, None, "Dataset-Sales.txt", "csv", "|", False, sf.salesSchema)
        sales_df = sales_tmp_df.withColumn("total_amount", regexp_replace(sales_tmp_df.total_amount, "\$", "").
                                    cast(IntegerType())).withColumn("timestamp",
                                                                    from_unixtime(unix_timestamp(sales_tmp_df.timestamp,
                                                                                                 'MM/dd/yy HH:mm:SS')).
                                                                                                cast(TimestampType()))
        return sales_df

    def refund_df(self, spark):
        refund_tmp_df = self.data_frame_create(spark, None, "Dataset-Refund.txt", "csv", "|", False, sf.refundSchema)
        refund_df = refund_tmp_df.withColumn('timestamp', from_unixtime(unix_timestamp(refund_tmp_df.timestamp,
                                                                                    "MM/dd/yy HH:mm:ss")).
                                          cast(TimestampType())). \
            withColumn('refund_amount', regexp_replace(refund_tmp_df.refund_amount, '\$', '').cast(IntegerType()))
        return refund_df
