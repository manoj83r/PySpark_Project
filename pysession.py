from pyspark.sql import SparkSession


class SparkSess:
    def __init__(self):
        pass

    @staticmethod
    def session_create(appName, env=None):
        spark = None
        if env == "LOCAL":
            spark = SparkSession.builder.appName(appName).master("local").enableHiveSupport().getOrCreate()
        elif env == "CLUSTER":
            spark = SparkSession.builder.appName(appName).master("YARN").enableHiveSupport().getOrCreate()
        return spark
