import json
from Spark_Initializer import SparkInitializer
from py4j.protocol import Py4JJavaError

def load_data(load_json):
    try:
        # Read the JSON configuration file
        with open(load_json) as file:
            load = json.load(file)

        # Extract extracturation parameters
        mysql_host = load["mysql_host"]
        mysql_port = load["mysql_port"]
        driver = load["driver"]
        mysql_database = load["mysql_database"]
        mysql_user = load["mysql_user"]
        mysql_password = load["mysql_password"]
        database_table=load['target_table']
        parquet_folder_path = load["parquet_folder_path"]


        # JDBC URL for MySQL connection
        mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"

        # Extract load parameters
        jdbc_properties = {
            "url":mysql_url,
            "driver":driver,
            "user": mysql_user,
            "password": mysql_password,
            "dbtable":database_table
        }

        # Create a SparkSession
        spark = SparkInitializer.get_spark()

        # Read the data from the Parquet files
        final_df = spark.read.parquet(parquet_folder_path)

        # Write the data to the target table using JDBC
        final_df.write \
            .format("jdbc") \
            .options(**jdbc_properties) \
            .mode("append") \
            .save()

    except FileNotFoundError as fe:
        raise Exception("Load file not found") from fe
    except json.JSONDecodeError as je:
        raise Exception("Invalid JSON load configuration") from je
    except Py4JJavaError as pje:
        raise Exception("Py4J Java error occurred") from pje

    finally:
        spark.stop()
        
# Path to the JSON load configuration file
load_json = "Json_Folder/load.json"
load_data(load_json)
