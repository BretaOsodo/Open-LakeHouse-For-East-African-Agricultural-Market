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

# ============================================
# Initialize Glue Context
# ============================================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'SILVER_BUCKET',
    'SOURCE_DATABASE',
    'SILVER_DATABASE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SOURCE_BUCKET = args['SOURCE_BUCKET']
SILVER_BUCKET = args['SILVER_BUCKET']
SOURCE_DATABASE = args['SOURCE_DATABASE']
SILVER_DATABASE = args['SILVER_DATABASE']

print(f"Processing from: s3://{SOURCE_BUCKET}")
print(f"Writing to: s3://{SILVER_BUCKET}")
print(f"Source Database: {SOURCE_DATABASE}")
print(f"Target Database: {SILVER_DATABASE}")


# ============================================
# Helper Functions
# ============================================

def extract_partition_columns(df, source_path_pattern):
    """
    Extract partition columns from S3 path structure
    Example: country=Kenya/city=Nairobi/year=2025/month=03/
    """
    # Add file path column
    df_with_path = df.withColumn("file_path", input_file_name())

    # Extract partition values from path
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


def add_processing_metadata(df):
    """Add processing metadata columns"""
    return df.withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("processing_job", lit(args['JOB_NAME'])) \
        .withColumn("processing_date", current_date())


def write_to_silver_layer(df, table_name, partition_cols):
    """Write transformed data to Silver layer in Parquet format"""

    output_path = f"s3://{SILVER_BUCKET}/silver/{table_name}/"

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy(partition_cols) \
        .option("compression", "snappy") \
        .save(output_path)

    print(f"✅ Written to: {output_path}")

    # Register table in Glue Catalog
    register_table_in_catalog(table_name, output_path, partition_cols, df)

    return output_path


def register_table_in_catalog(table_name, s3_location, partition_cols, df):
    """Register the transformed table in Glue Data Catalog"""

    try:
        # Get the schema from DataFrame
        schema = df.schema

        # Convert PySpark schema to Glue format
        columns = []
        for field in schema.fields:
            columns.append({
                'Name': field.name,
                'Type': field.dataType.simpleString().upper(),
                'Comment': f'Column from {table_name}'
            })

        # Prepare partition keys
        partition_keys = [{'Name': col, 'Type': 'string'} for col in partition_cols]

        # Create table in Glue Catalog
        glue_client = boto3.client('glue')

        try:
            # Check if table exists
            glue_client.get_table(
                DatabaseName=SILVER_DATABASE,
                Name=table_name
            )
            print(f"Table {table_name} already exists, updating...")

            # Update existing table
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

        except glue_client.exceptions.EntityNotFoundException:
            # Create new table
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
                        'source_database': SOURCE_DATABASE
                    }
                }
            )
            print(f"✅ Created table: {SILVER_DATABASE}.{table_name}")

    except Exception as e:
        print(f"❌ Error registering table in Glue Catalog: {str(e)}")