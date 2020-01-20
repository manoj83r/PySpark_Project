from pyspark.sql.types import *


class SchemaFile:
    def __init__(self):
        pass

    # Schema of Product DF
    productSchema = StructType([StructField("product_id", IntegerType(), False),
                                StructField("product_name", StringType(), False),
                                StructField("product_type", StringType(), False),
                                StructField("product_version", StringType(), False),
                                StructField("product_price", StringType(), False)])

    # Schema of Customer DF
    customerSchema = StructType([StructField("customer_id", IntegerType(), False),
                                 StructField("customer_first_name", StringType(), True),
                                 StructField("customer_last_name", StringType(), True),
                                 StructField("phone_number", StringType(), True)])

    # Schema of Sales DF
    salesSchema = StructType([StructField("transaction_id", IntegerType(), False),
                              StructField("customer_id", IntegerType(), False),
                              StructField("product_id", IntegerType(), False),
                              StructField("timestamp", StringType(), False),
                              StructField("total_amount", StringType(), False),
                              StructField("total_quantity", IntegerType(), False)])

    # Schema of Refund DF
    refundSchema = StructType([StructField('refund_id', IntegerType(), False),
                               StructField('original_transaction_id', IntegerType(), False),
                               StructField('customer_id', IntegerType(), False),
                               StructField('product_id', IntegerType(), False),
                               StructField('timestamp', StringType(), False),
                               StructField('refund_amount', StringType(), False),
                               StructField('refund_quantity', IntegerType(), False)])
