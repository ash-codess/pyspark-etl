from pyspark.sql import SparkSession

class SparkInitializer:
    _spark = None

    @staticmethod
    def get_spark():
        if SparkInitializer._spark is None:
            try:
                SparkInitializer._spark = (
                    SparkSession
                    .builder
                    .appName("ETL_Pyspark")
                    .config("spark.driver.extraClassPath", "mysql-connector-java-8.0.13.jar")
                    .getOrCreate()
                )
            except Exception as e:
                # Perform custom error handling, such as logging or raising custom exceptions
                error_message = "Error initializing SparkSession: {0}".format(str(e))
                raise RuntimeError(error_message)
        return SparkInitializer._spark
