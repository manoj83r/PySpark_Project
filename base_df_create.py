import logging


class CreateDF:
    def __init__(self):
        pass

    def dataFrameCreate(self, spark, directory=None, file_name=None, file_format=None,
                        seperator=",", headerAvailable=False, schemaName=None):
        spark_logger = logging.Log4j(spark)
        try:
            data_frame = spark.read.load(file_name, format=file_format, sep=seperator,
                                         header=headerAvailable, schema=schemaName)
            spark_logger.info("Data Frame create successfully")
            return data_frame
        except:
            spark_logger.info("Problem in creating Data Frame")
