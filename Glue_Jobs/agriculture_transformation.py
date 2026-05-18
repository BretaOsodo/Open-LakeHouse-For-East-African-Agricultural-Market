import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import boto3

# Initialize the glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'bronze_database',
    'silver_bucket',
    'silver_database'
])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# configurations
BRONZE_BUCKET = args['bronze_bucket']
SILVER_BUCKET = args['silver_bucket']
BRONZE_DATABASE = args['bronze_database']
SILVER_DATABASE = args['silver_database']

print(f'processing from :s3://{BRONZE_BUCKET}')
print(f'writing to: s3://{SILVER_BUCKET}')
print(f'Bronze database: {BRONZE_DATABASE}')
print(f'Silver Database: {SILVER_DATABASE}')


# Helper functions
def extract_partitions_columns(df, source_path_patterns):
    # add file path column
    df_with_path = df.withColumn('file_path', input_file_name())

    # Extract partitions values from paths
    df_with_partitions = df_with_path.withColumn(
        "country", regexp_extract(col("file_path"), "country=([^/]+)", 1)
    ).withColumn(
        "city", regexp_extract(col("file_path"), "city=([^/]+)", 1)
    ).withColumn(
        "year", regexp_extract(col("file_path"), "year=([^/]+)", 1)
    ).withColumn(
        "month", regexp_extract(col("file_path"), "month=([^/]+)", 1)
    ).withColumn(
        "day", regexp_extract(col("file_path"), "day=([^/]+)", 1)
    ).drop("file_path")

    return df_with_partitions


def spark_to_glue_type(dtype):
    mapping = {
        "string": "string",
        "int": "int",
        "bigint": "bigint",
        "double": "double",
        "float": "float",
        "boolean": "boolean",
        "timestamp": "timestamp",
        "date": "date"
    }

    return mapping.get(dtype, "string")


def add_processing_metadata(df):
    """Add processing metadata columns"""
    return df.withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("processing_job", lit(args['JOB_NAME'])) \
        .withColumn("processing_date", current_date())


def write_to_silver_layer(df, table_name, partition_cols):
    output_path = f"s3://{SILVER_BUCKET}/silver/{table_name}"

    df.write \
        .mode('append') \
        .format('parquet') \
        .partitionBy(*partition_cols) \
        .option('compression', 'snappy') \
        .save(output_path)

    print(f'Written to: {output_path}')

    # register table in Glue catalog
    register_table_in_catalog(table_name, output_path, partition_cols, df)

    return output_path


def register_table_in_catalog(table_name, s3_location, partition_cols, df):
    try:
        # Get schema for dataframe
        schema = df.schema

        # Convert pyspark schema to GLue format
        columns = []
        for field in schema.fields:
            columns.append({
                "Name": field.name,
                "Type": spark_to_glue_type(
                    filed.dataType.simpleString()),
                "Comment": f'Column from {table_name}'
            })

        # prepare partition keys
        partition_keys = [{
            "Name": col,
            "Type": 'string'
        } for col in partition_cols]

        # create table in glue catalog
        glue_client = boto3.client('glue')

        try:
            # check if table exists
            glue_client.get_table(
                DatabaseName=SILVER_DATABASE,
                Name=table_name
            )
            print(f'Table {table_name} already exists, updating...')

            # Update existing table_name
            glue_client.update_table(
                DatabaseName=SILVER_DATABASE,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': s3_location,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'PartitionKeys': partition_keys,
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification': 'parquet',
                        'compressionType': 'snappy',
                        'last_updated': datetime.now().isoformat()
                    }
                }

            )
            glue_client.exceptions.EntityNotFoundException:
                # create new Table
                glue_client.create_table(
                    DatabaseName=SILVER_DATABASE,
                    TableInput={
                        'Name': table_name,
                        'Description': f'Silver layer table - {table_name}',
                        'StorageDescriptor': {
                            'Columns': columns,
                            'Location': s3_location,
                            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                            'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                            }
                        },
                        'PartitionKeys': partition_keys,
                        'TableType': 'EXTERNAL_TABLE',
                        'Parameters': {
                            'created_by': 'glue_job',
                            'created_at': datetime.now().isoformat(),
                            'source_database': BRONZE_DATABASE
                        }
                    }
                )


    except Exception as e:
        print(f'Error registering table in glue catalog: {str(e)}')

# 1. Transform weather data