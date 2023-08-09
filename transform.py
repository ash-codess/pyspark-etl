import json

from pyspark.sql import functions as F

from Spark_Initializer import SparkInitializer


def change_date_format(df, source_column, target_format):
    if source_column not in df.columns:
        raise Exception("Error occurred while performing change_date_format operation")
    formatted_df = df.select(
        *[
            F.date_format(col, target_format).alias(source_column)
            if col == source_column
            else col
            for col in df.columns
        ]
    )
    return formatted_df


def split_column(df, source_column, target_columns, separator):
    if source_column not in df.columns:
        raise Exception("Error occurred while performing split operation")
    split_col = F.split(df[source_column], separator)
    split_df = df.select(
        "*",
        split_col[0].alias(target_columns[0]),
        split_col[1].alias(target_columns[1]),
        split_col[2].alias(target_columns[2]),
    )
    return split_df


def concatenate_columns(df, source_columns, target_column):
    try:
        concatenated_df = df.select(
            "*",
            F.concat_ws(" ", *[df[col] for col in source_columns]).alias(target_column),
        )
        return concatenated_df
    except Exception as e:
        raise Exception(
            f"Error occurred while performing concatenate_columns operation: {str(e)}"
        )


def drop_column(df, source_columns):
    if not all(column in df.columns for column in source_columns):
        raise ValueError(
            "source_columns must be valid columns present in the DataFrame"
        )
    dropped_df = df.select([col for col in df.columns if col not in source_columns])
    return dropped_df


def join_tables(df1, df2, source_column, join_type):
    if source_column not in df1.columns or source_column not in df2.columns:
        raise ValueError(
            "source_column must be a valid column present in both DataFrames"
        )
    joined_df = df1.join(df2, source_column, join_type)
    return joined_df


# Initialize Spark
spark = SparkInitializer.get_spark()

# Set additional options for parquet reading
parquet_options = {"header": "true", "inferSchema": "true"}

# Read the 'employees.parquet' file into a DataFrame
employees_df = (
    spark.read.format("parquet")
    .options(**parquet_options)
    .load("extract/employees.parquet")
)

# Read the 'dept_emp.parquet' file into a DataFrame
dept_emp_df = (
    spark.read.format("parquet")
    .options(**parquet_options)
    .load("extract/dept_emp.parquet")
)

# Cache the larger DataFrame if it will be reused multiple times
employees_df.cache()

# Load the transformation configuration from JSON file
json_path = "Json_Folder/transformation.json"
try:
    with open(json_path) as ConfigurationFile:
        config = json.load(ConfigurationFile)
        transformations = config["transformations"]

        for transformation in transformations:
            operation = transformation["transformation"]

            if operation == "change_date_format":
                source_column = transformation["source_column"]
                target_format = transformation["target_format"]
                employees_df = change_date_format(
                    employees_df, source_column, target_format
                )

            elif operation == "split":
                source_column = transformation["source_column"]
                target_columns = transformation["target_columns"]
                separator = transformation["separator"]
                employees_df = split_column(
                    employees_df, source_column, target_columns, separator
                )

            elif operation == "concatenate_columns":
                source_columns = transformation["source_columns"]
                target_column = transformation["target_column"]
                employees_df = concatenate_columns(
                    employees_df, source_columns, target_column
                )

            elif operation == "drop_column":
                source_columns = transformation["source_columns"]
                employees_df = drop_column(employees_df, source_columns)

            elif operation == "join_tables":
                source_tables = transformation["source_tables"]
                source_column = transformation["source_column"]
                join_type = transformation["join_type"]
                employees_df = join_tables(
                    employees_df, dept_emp_df, source_column, join_type
                )

            else:
                raise Exception(f"Unknown transformation is used: {operation}")

        employees_df.show()


except FileNotFoundError as fe:
    raise Exception("Extraction file not found") from fe

except json.JSONDecodeError as je:
    raise Exception("Invalid JSON extraction") from je

# Save the DataFrame as a parquet file
try:
    employees_df.write.mode("overwrite").parquet("transform/final.parquet")
except Exception as e:
    raise Exception("Error writing Parquet file") from e
