import json
from pyspark.sql.utils import ParseException
from py4j.protocol import Py4JJavaError
from Spark_Initializer import SparkInitializer

def extract_tables(extract_json):
    try:
        # Read the JSON configuration file
        with open(extract_json) as file:
            extract = json.load(file)

        # Extract extracturation parameters
        mysql_host = extract["mysql_host"]
        mysql_port = extract["mysql_port"]
        driver = extract["driver"]
        mysql_database = extract["mysql_database"]
        mysql_user = extract["mysql_user"]
        mysql_password = extract["mysql_password"]
        table_names = extract["table_names"]
        parquet_folder_path = extract["parquet_folder_path"]

        
        # Create a SparkSession
        spark = SparkInitializer.get_spark()

        # JDBC URL for MySQL connection
        mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"

        properties = {
            "url": mysql_url,
            "driver": driver,
            "user": mysql_user,
            "password": mysql_password
        }

        for table_name in table_names:
            try:
                # Read data from the JDBC source
                df = spark.read.format("jdbc") \
                    .options(**properties) \
                    .option("dbtable", table_name) \
                    .load()

                # Write the extracted data to Parquet files
                parquet_file_path = f"{parquet_folder_path}/{table_name}.parquet"
                df.write.mode("overwrite").format("parquet").option("compression", "snappy").save(parquet_file_path)


            except ParseException as pe:
                raise Exception("JDBC URL parsing error") from pe
            except Py4JJavaError as pje:
                raise Exception("Py4J Java error occurred") from pje
            except Exception as e:
                raise Exception("Database error occurred") from e

    except FileNotFoundError as fe:
        raise Exception("extracturation file not found") from fe
    except json.JSONDecodeError as je:
        raise Exception("Invalid JSON extracturation") from je

# Path to the JSON extracturation file
extract_json = "Json_Folder/extract.json"
extract_tables(extract_json)
