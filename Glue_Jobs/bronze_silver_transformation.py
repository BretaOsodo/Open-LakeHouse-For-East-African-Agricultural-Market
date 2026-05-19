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

# Initialize the Glue context
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

# Configurations
BRONZE_BUCKET = args['bronze_bucket']
SILVER_BUCKET = args['silver_bucket']
BRONZE_DATABASE = args['bronze_database']
SILVER_DATABASE = args['silver_database']

print(f'Processing from: s3://{BRONZE_BUCKET}')
print(f'Writing to: s3://{SILVER_BUCKET}')
print(f'Bronze database: {BRONZE_DATABASE}')
print(f'Silver database: {SILVER_DATABASE}')


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def extract_partitions_columns(df, source_path_patterns):
    """
    Adds file_path column and extracts partition values from the path.
    NOTE: file_path is intentionally NOT dropped here — callers that need
    extra path extraction (e.g. crop_type) must drop it themselves afterward.
    """
    df_with_path = df.withColumn('file_path', input_file_name())

    df_with_partitions = df_with_path \
        .withColumn("country", regexp_extract(col("file_path"), "country=([^/]+)", 1)) \
        .withColumn("city",    regexp_extract(col("file_path"), "city=([^/]+)",    1)) \
        .withColumn("year",    regexp_extract(col("file_path"), "year=([^/]+)",    1)) \
        .withColumn("month",   regexp_extract(col("file_path"), "month=([^/]+)",   1)) \
        .withColumn("day",     regexp_extract(col("file_path"), "day=([^/]+)",     1))

    return df_with_partitions


def add_processing_metadata(df):
    """Add processing metadata columns."""
    return df \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("processing_job",      lit(args['JOB_NAME'])) \
        .withColumn("processing_date",     current_date())


def write_to_silver_layer(df, table_name, partition_cols):
    """
    Write DataFrame to Silver S3 location using partition overwrite mode
    so retries do not produce duplicate rows (fixes D6).
    """
    output_path = f"s3://{SILVER_BUCKET}/silver/{table_name}"

    # leaving all other existing partitions untouched.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.write \
        .mode('overwrite') \
        .format('parquet') \
        .partitionBy(*partition_cols) \
        .option('compression', 'snappy') \
        .save(output_path)

    print(f'Written to: {output_path}')

    register_table_in_catalog(table_name, output_path, partition_cols, df)
    register_partitions(table_name, output_path, partition_cols, df)

    return output_path


def register_table_in_catalog(table_name, s3_location, partition_cols, df):
    """
    Register or update the transformed table in the Glue Data Catalog.
    because Glue (and Athena) expect them only in PartitionKeys.
    """
    try:
        schema = df.schema
        partition_set = set(partition_cols)  # FIX D7

        # Only non-partition columns go into StorageDescriptor.Columns
        columns = [
            {
                'Name': field.name,
                'Type': field.dataType.simpleString(),
                'Comment': f'Column from {table_name}'
            }
            for field in schema.fields
            if field.name not in partition_set  # FIX D7
        ]

        partition_keys = [{'Name': c, 'Type': 'string'} for c in partition_cols]

        glue_client = boto3.client('glue')

        storage_descriptor = {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat':  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        }

        try:
            glue_client.get_table(DatabaseName=SILVER_DATABASE, Name=table_name)
            print(f"Table {table_name} already exists, updating...")

            glue_client.update_table(
                DatabaseName=SILVER_DATABASE,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': storage_descriptor,
                    'PartitionKeys': partition_keys,
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification':   'parquet',
                        'compressionType':  'snappy',
                        'last_updated':     datetime.now().isoformat()
                    }
                }
            )

        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_table(
                DatabaseName=SILVER_DATABASE,
                TableInput={
                    'Name':        table_name,
                    'Description': f'Silver layer table - {table_name}',
                    'StorageDescriptor': storage_descriptor,
                    'PartitionKeys': partition_keys,
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'created_by':        'glue_job',
                        'created_at':        datetime.now().isoformat(),
                        'source_database':   BRONZE_DATABASE
                    }
                }
            )
            print(f"Created table: {SILVER_DATABASE}.{table_name}")

    except Exception as e:
        print(f'Error registering table in Glue Catalog: {str(e)}')


def register_partitions(table_name, s3_location, partition_cols, df):

    try:
        glue_client = boto3.client('glue')

        # Collect distinct partition value combinations from the batch
        distinct_partitions = df.select(*partition_cols).distinct().collect()

        if not distinct_partitions:
            return

        partition_inputs = []
        for row in distinct_partitions:
            values = [str(row[c]) for c in partition_cols]

            # Build the S3 prefix for this partition combination
            partition_path = s3_location
            for c, v in zip(partition_cols, values):
                partition_path += f"/{c}={v}"

            partition_inputs.append({
                'Values': values,
                'StorageDescriptor': {
                    'Location': partition_path,
                    'InputFormat':  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'Parameters': {'classification': 'parquet'}
            })

        # Glue API accepts max 100 partitions per call — batch if needed
        batch_size = 100
        for i in range(0, len(partition_inputs), batch_size):
            batch = partition_inputs[i:i + batch_size]
            try:
                glue_client.batch_create_partition(
                    DatabaseName=SILVER_DATABASE,
                    TableName=table_name,
                    PartitionInputList=batch
                )
                print(f"Registered {len(batch)} partition(s) for {table_name}")
            except glue_client.exceptions.AlreadyExistsException:
                # Partitions already registered — safe to ignore on reruns
                print(f"Partitions already exist for {table_name}, skipping registration")

    except Exception as e:
        print(f'Error registering partitions for {table_name}: {str(e)}')


# ---------------------------------------------------------------------------
# 1. TRANSFORM WEATHER DATA
# ---------------------------------------------------------------------------

def transform_weather_data():
    """Read, transform, and write weather data to the Silver layer."""
    print('Processing weather data')
    weather_path = f's3://{BRONZE_BUCKET}/weather/'

    try:
        weather_df = spark.read.option("recursiveFileLookup", "true").json(weather_path)

        # FIX B5: cache and count once
        weather_df = weather_df.cache()
        record_count = weather_df.count()

        if record_count > 0:
            print(f'Read {record_count} weather records')

            weather_df = extract_partitions_columns(weather_df, weather_path)
            weather_df = weather_df.drop("file_path")  # no extra path extraction needed
            weather_df = add_processing_metadata(weather_df)

            # Temperature conversion
            if 'temp_avg_c' in weather_df.columns:
                weather_df = weather_df.withColumn(
                    "avg_temperature_fahrenheit",
                    round((col("temp_avg_c") * 9 / 5) + 32, 1)
                )

            # Crop suitability score

            if all(c in weather_df.columns for c in ['temp_avg_c', 'precipitation_mm', 'soil_moisture_vol_pct']):
                weather_df = weather_df.withColumn(
                    "crop_suitability_score",
                    when(col("temp_avg_c").between(18, 28), 40).otherwise(0) +
                    when(col("precipitation_mm").between(50, 150), 40).otherwise(0) +
                    when(col("soil_moisture_vol_pct") > 20, 20).otherwise(0)
                )

            # Heat stress index
            if 'temp_max_c' in weather_df.columns:
                weather_df = weather_df.withColumn(
                    'heat_stress_index',
                    when(col('temp_max_c') > 32, "High")
                    .when(col("temp_max_c") > 28, "Moderate")
                    .otherwise("Low")
                )

            # Water stress level
            # FIX B3: loop variable renamed to `c`
            if all(c in weather_df.columns for c in ['precipitation_mm', 'evapotranspiration_mm']):
                weather_df = weather_df.withColumn(
                    "water_stress_level",
                    when(col("precipitation_mm") < col("evapotranspiration_mm"), "Water Deficit")
                    .when(col("precipitation_mm") > col("evapotranspiration_mm") * 1.5, "Water Surplus")
                    .otherwise("Balanced")
                )

            silver_weather_df = weather_df.select(
                col("date"),
                col("country"),
                col("city"),
                col("year"),
                col("month"),
                col("day"),
                col("temp_avg_c").alias("avg_temperature_celsius"),
                col("temp_min_c").alias("min_temperature_celsius"),
                col("temp_max_c").alias("max_temperature_celsius"),
                col("avg_temperature_fahrenheit"),
                col("humidity_pct").alias("humidity_percentage"),
                col("precipitation_mm").alias("precipitation_millimeters"),
                col("evapotranspiration_mm").alias("evapotranspiration_millimeters"),
                col("crop_suitability_score"),
                col("heat_stress_index"),
                col("water_stress_level"),
                col("processed_timestamp"),
                col("processing_job")
            ).distinct()

            write_to_silver_layer(silver_weather_df, "weather_transformed", ["country", "city", "year", "month"])
            print(f'Weather transformation complete: {silver_weather_df.count()} records')

        else:
            print('No weather data found')

    except Exception as e:
        print(f'Error processing weather data: {str(e)}')


# ---------------------------------------------------------------------------
# 2. TRANSFORM CROP GROWTH DATA
# ---------------------------------------------------------------------------

def transform_crop_data():
    """Transform crop growth data to the Silver layer."""
    print("Processing crop growth data")
    crop_path = f"s3://{BRONZE_BUCKET}/crops/"

    try:
        crop_df = spark.read.option("recursiveFileLookup", "true").json(crop_path)

        # FIX B5: cache and count once
        crop_df = crop_df.cache()
        record_count = crop_df.count()

        if record_count > 0:
            print(f'Read {record_count} crop records')



            crop_df = extract_partitions_columns(crop_df, crop_path)
            crop_df = crop_df.withColumn(
                "crop_type",
                regexp_extract(col("file_path"), "crop=([^/]+)", 1)
            ).drop("file_path")

            crop_df = add_processing_metadata(crop_df)

            # Growth efficiency

            if all(c in crop_df.columns for c in ['health_score', 'day_after_planting']):
                crop_df = crop_df.withColumn(
                    "growth_efficiency",
                    when(
                        col("day_after_planting") > 0,
                        round(col("health_score") / col("day_after_planting") * 100, 2)
                    ).otherwise(None)
                )

            # Yield category
            if 'estimated_yield_tons_ha' in crop_df.columns:
                crop_df = crop_df.withColumn(
                    "yield_category",
                    when(col("estimated_yield_tons_ha") > 3, "High")
                    .when(col("estimated_yield_tons_ha") > 1.5, "Medium")
                    .otherwise("Low")
                )

            silver_crop_df = crop_df.select(
                col("date"),
                col("country"),
                col("city"),
                col("crop_type"),
                col("planting_date"),
                col("day_after_planting"),
                col("growth_stage"),
                col("canopy_cover_pct").alias("canopy_cover_percentage"),
                col("plant_height_cm"),
                col("health_score"),
                col("stress_level"),
                col("estimated_yield_tons_ha").alias("estimated_yield_tons_per_hectare"),
                col("yield_category"),
                col("growth_efficiency"),
                col("pest_risk"),
                col("processed_timestamp"),
                col("processing_job")
            ).distinct()

            # FIX B1: indentation corrected
            write_to_silver_layer(silver_crop_df, "crop_growth_transformed", ["country", "crop_type"])
            print(f"Crop transformation complete: {silver_crop_df.count()} records")

        else:
            print('No crop data found')

    except Exception as e:
        print(f'Error processing crop data: {str(e)}')


# ---------------------------------------------------------------------------
# 3. TRANSFORM FARMER DATA
# ---------------------------------------------------------------------------

def transform_farmer_data():
    """Transform farmer profile data to the Silver layer."""
    print("Processing farmer data")
    farmer_path = f"s3://{BRONZE_BUCKET}/farmers/"

    try:
        farmer_df = spark.read.option("recursiveFileLookup", "true").json(farmer_path)

        # FIX B5: cache and count once
        farmer_df = farmer_df.cache()
        record_count = farmer_df.count()

        if record_count > 0:
            print(f"Read {record_count} farmer records")

            farmer_df = extract_partitions_columns(farmer_df, farmer_path)
            farmer_df = farmer_df.drop("file_path")
            farmer_df = add_processing_metadata(farmer_df)


            if all(c in farmer_df.columns for c in ['mobile_phone_owner', 'uses_weather_app', 'access_to_extension']):
                farmer_df = farmer_df.withColumn(
                    "tech_adoption_score",
                    when(col("mobile_phone_owner") == True, 40).otherwise(0) +
                    when(col("uses_weather_app") == True,   30).otherwise(0) +
                    when(col("access_to_extension") == True, 30).otherwise(0)
                )

            # Farmer classification by farm size
            if 'farm_size_hectares' in farmer_df.columns:
                farmer_df = farmer_df.withColumn(
                    "farmer_classification",
                    when(col("farm_size_hectares") <= 2, "Smallholder")
                    .when(col("farm_size_hectares") <= 5, "Medium")
                    .otherwise("Large")
                )

            # Explode crops_grown array
            if 'crops_grown' in farmer_df.columns:
                farmer_df = farmer_df.withColumn(
                    "crop", explode(col("crops_grown"))
                ).drop("crops_grown")

            silver_farmer_df = farmer_df.select(
                col("farmer_id"),
                col("country"),
                col("city"),
                col("crop"),
                col("farm_size_hectares"),
                col("farming_practice"),
                col("irrigation_type"),
                col("access_to_extension"),
                col("tech_adoption_score"),
                col("farmer_classification"),
                col("registered_date"),
                col("processed_timestamp"),
                col("processing_job")
            ).distinct()

            write_to_silver_layer(silver_farmer_df, "farmer_profiles_transformed", ["country", "farmer_classification"])
            print(f"Farmer transformation complete: {silver_farmer_df.count()} records")

        else:
            print("No farmer data found")

    except Exception as e:
        print(f"Error processing farmer data: {str(e)}")


# ---------------------------------------------------------------------------
# 4. TRANSFORM FIELD / SOIL DATA
# ---------------------------------------------------------------------------

def transform_field_data():
    """Transform field and soil data to the Silver layer."""
    print("Processing field data")
    field_path = f"s3://{BRONZE_BUCKET}/fields/"

    try:
        field_df = spark.read.option("recursiveFileLookup", "true").json(field_path)


        field_df = field_df.cache()
        record_count = field_df.count()

        if record_count > 0:
            print(f"Read {record_count} field records")

            field_df = extract_partitions_columns(field_df, field_path)
            field_df = field_df.drop("file_path")
            field_df = add_processing_metadata(field_df)

            # Soil fertility score

            if all(c in field_df.columns for c in ['soil_ph', 'organic_matter_pct', 'nitrogen_ppm']):
                field_df = field_df.withColumn(
                    "soil_fertility_score",
                    round(
                        (when(col("soil_ph").between(6.0, 7.5), 100).otherwise(50) * 0.3) +
                        (col("organic_matter_pct") / 5 * 100 * 0.4) +
                        (col("nitrogen_ppm") / 100 * 100 * 0.3),
                        1
                    )
                ).withColumn(
                    "soil_health_category",
                    when(col("soil_fertility_score") > 70, "Good")
                    .when(col("soil_fertility_score") > 40, "Moderate")
                    .otherwise("Poor")
                )

            silver_field_df = field_df.select(
                col("field_id"),
                col("country"),
                col("city"),
                col("soil_type"),
                col("soil_ph"),
                col("organic_matter_pct").alias("organic_matter_percentage"),
                col("nitrogen_ppm"),
                col("phosphorus_ppm"),
                col("potassium_ppm"),
                col("drainage_class"),
                col("erosion_risk"),
                col("soil_fertility_score"),
                col("soil_health_category"),
                col("last_soil_test_date"),
                col("processed_timestamp"),
                col("processing_job")
            ).distinct()

            write_to_silver_layer(silver_field_df, "field_data_transformed", ["country", "soil_health_category"])
            print(f"Field transformation complete: {silver_field_df.count()} records")

        else:
            print("No field data found")

    except Exception as e:
        print(f"Error processing field data: {str(e)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Job Name: {args['JOB_NAME']}")
    print(f"Start Time: {datetime.now().isoformat()}")

    transform_weather_data()
    transform_crop_data()
    transform_farmer_data()
    transform_field_data()

    job.commit()
    print("Glue job completed successfully")