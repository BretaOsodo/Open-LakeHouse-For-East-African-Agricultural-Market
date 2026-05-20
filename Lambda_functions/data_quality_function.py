import json
import boto3
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta, timezone
import re
from botocore.exceptions import ClientError

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
cloudwatch_client = boto3.client('cloudwatch')
# NOTE: glue_client removed — was initialized but never used.
# Re-add if you wire up Glue catalog partition discovery later.

# Configurations
SILVER_BUCKET = "breta-open-lakehouse-agriculture-silver"
SNS_TOPIC_ARN = "arn:aws:sns:eu-north-1:237124340255:open-lakehouse-agricultre-alerts"
SILVER_DATABASE = "agriculture_silver_bd"  # Reserved for future Glue integration

# Data quality thresholds
QUALITY_THRESHOLDS = {
    "max_null_percentage": 5,    # Max 5% null values allowed
    "min_record_count": 10,      # Minimum records expected
    "max_outlier_percentage": 2, # Max 2% outliers
    "freshness_hours": 24        # Data should be updated within 24 hours
}


def lambda_handler(event, context):
    """
    Main handler for data quality validation.
    Can be triggered by CloudWatch schedule or S3 events.
    """
    print(f"Received event: {json.dumps(event)}")

    results = {
        "execution_time": datetime.now().isoformat(),
        "checks_performed": [],
        "failed_checks": [],
        "passed_checks": [],
        "warning_checks": [],
        "overall_status": "PASSED"
    }

    try:
        # 1. Validate Weather Data
        weather_results = validate_weather_data()
        results["checks_performed"].append("weather_data_quality")
        _bucket_result(results, weather_results)

        # 2. Validate Crop Data
        crop_results = validate_crop_data()
        results["checks_performed"].append("crop_data_quality")
        _bucket_result(results, crop_results)

        # 3. Validate Farmer Data
        farmer_results = validate_farmer_data()
        results["checks_performed"].append("farmer_data_quality")
        _bucket_result(results, farmer_results)

        # 4. Validate Field/Soil Data
        field_results = validate_field_data()
        results["checks_performed"].append("field_data_quality")
        _bucket_result(results, field_results)

        # Derive overall status from aggregated results
        results["overall_status"] = "FAILED" if results["failed_checks"] else "PASSED"

        if results["overall_status"] == "FAILED":
            send_quality_alert(results)
            push_metrics_to_cloudwatch(results)
        else:
            send_success_notification(results)

        # Always push metrics so CloudWatch dashboards show passing datasets too
        push_metrics_to_cloudwatch(results)

        # Save full report to S3
        save_quality_report(results)

    except Exception as e:
        print(f"Error in data quality validation: {str(e)}")
        send_error_alert(str(e))
        results["overall_status"] = "ERROR"
        results["error"] = str(e)

    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }


def _bucket_result(results, dataset_result):
    """Helper: route a dataset result into the correct bucket."""
    if dataset_result["failed"]:
        results["failed_checks"].append(dataset_result)
    elif any(c.get("status") == "WARNING" for c in dataset_result.get("checks", [])):
        results["warning_checks"].append(dataset_result)
    else:
        results["passed_checks"].append(dataset_result)


# ---------------------------------------------------------------------------
# WEATHER DATA VALIDATION
# ---------------------------------------------------------------------------

def validate_weather_data():
    """Validate weather data quality."""
    results = {
        "dataset": "weather_data",
        "failed": False,
        "checks": []
    }

    try:
        weather_path = f"s3://{SILVER_BUCKET}/silver/weather/"

        partitions = list_s3_partitions(weather_path)

        if not partitions:
            results["checks"].append({
                "check": "data_exists",
                "status": "FAILED",
                "message": "No weather data found in silver bucket"
            })
            results["failed"] = True
            return results

        # Read data from last 7 days
        # 7-day window chosen for weather because daily refresh is expected
        df = read_recent_partitions(weather_path, days=7)

        if df is None or len(df) == 0:
            results["checks"].append({
                "check": "data_exists",
                "status": "FAILED",
                "message": "No data available for last 7 days"
            })
            results["failed"] = True
            return results

        # Check 1: Record count
        record_count = len(df)
        min_records = QUALITY_THRESHOLDS["min_record_count"]
        if record_count < min_records:
            results["checks"].append({
                "check": "record_count",
                "status": "FAILED",
                "message": f"Low record count: {record_count} (minimum: {min_records})",
                "actual": record_count,
                "expected": min_records
            })
            results["failed"] = True
        else:
            results["checks"].append({
                "check": "record_count",
                "status": "PASSED",
                "message": f"Record count: {record_count}",
                "actual": record_count
            })

        # Check 2: Null values
        null_checks = check_null_values(df, [
            'temp_avg_c', 'temp_min_c', 'temp_max_c',
            'precipitation_mm', 'humidity_pct'
        ])
        results["checks"].extend(null_checks)
        if any(c["status"] == "FAILED" for c in null_checks):
            results["failed"] = True

        # Check 3: Data freshness
        freshness_check = check_data_freshness(df, 'date')
        results["checks"].append(freshness_check)
        if freshness_check["status"] == "FAILED":
            results["failed"] = True

        # Check 4: Valid ranges
        range_checks = check_value_ranges(df, {
            'temp_avg_c':        (-10, 45),
            'temp_min_c':        (-15, 40),
            'temp_max_c':        (-5,  50),
            'humidity_pct':      (0,   100),
            'precipitation_mm':  (0,   200),
            'wind_speed_kmh':    (0,   100)
        })
        results["checks"].extend(range_checks)
        if any(c["status"] == "FAILED" for c in range_checks):
            results["failed"] = True

        # Check 5: Partition completeness
        partition_check = check_partition_completeness(
            partitions, ['country', 'city', 'year', 'month', 'day']
        )
        results["checks"].append(partition_check)
        if partition_check["status"] == "FAILED":
            results["failed"] = True

        # Check 6: Logical consistency
        logical_checks = check_logical_consistency(df, {
            'temp_min_c <= temp_avg_c <= temp_max_c': {
                'condition': (
                    (df['temp_min_c'] <= df['temp_avg_c']) &
                    (df['temp_avg_c'] <= df['temp_max_c'])
                ),
                'message': 'Temperature hierarchy violated'
            },
            'precipitation_mm >= 0': {
                'condition': df['precipitation_mm'] >= 0,
                'message': 'Negative precipitation values'
            }
        })
        results["checks"].extend(logical_checks)
        if any(c["status"] == "FAILED" for c in logical_checks):
            results["failed"] = True

        # Summary
        results["summary"] = {
            "total_checks": len(results["checks"]),
            "passed":   sum(1 for c in results["checks"] if c["status"] == "PASSED"),
            "warnings": sum(1 for c in results["checks"] if c["status"] == "WARNING"),
            "failed":   sum(1 for c in results["checks"] if c["status"] == "FAILED")
        }

    except Exception as e:
        results["checks"].append({
            "check": "validation_error",
            "status": "FAILED",
            "message": f"Error during validation: {str(e)}"
        })
        results["failed"] = True

    return results


# ---------------------------------------------------------------------------
# CROP DATA VALIDATION
# ---------------------------------------------------------------------------

def validate_crop_data():
    """Validate crop data quality."""
    results = {
        "dataset": "crop_data",
        "failed": False,
        "checks": []
    }

    try:
        crop_path = f"s3://{SILVER_BUCKET}/silver/crops/"
        # 30-day window used here (vs 7 for weather) because crop growth cycles
        # are slower and records aren't updated daily.
        df = read_recent_partitions(crop_path, days=30)

        if df is None or len(df) == 0:
            results["checks"].append({
                "check": "data_exists",
                "status": "FAILED",
                "message": "No crop data found"
            })
            results["failed"] = True
            return results

        # Check 1: Required columns exist
        required_columns = [
            'crop_type', 'growth_stage', 'health_score', 'estimated_yield_tons_ha'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            results["checks"].append({
                "check": "required_columns",
                "status": "FAILED",
                "message": f"Missing columns: {missing_columns}"
            })
            results["failed"] = True
        else:
            results["checks"].append({
                "check": "required_columns",
                "status": "PASSED",
                "message": "All required columns present"
            })

        # Check 2: Health score range (0-100)
        if 'health_score' in df.columns:
            invalid_health = df[(df['health_score'] < 0) | (df['health_score'] > 100)]
            if len(invalid_health) > 0:
                results["checks"].append({
                    "check": "health_score_range",
                    "status": "FAILED",
                    "message": (
                        f"{len(invalid_health)} records have invalid health scores "
                        "(should be 0-100)"
                    )
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "health_score_range",
                    "status": "PASSED",
                    "message": "All health scores within valid range"
                })

        # Check 3: Valid crop types
        valid_crops = [
            'Maize', 'Beans', 'Coffee', 'Tea', 'Cassava',
            'Sweet_Potato', 'Banana', 'Sorghum', 'Millet'
        ]
        if 'crop_type' in df.columns:
            invalid_crops = df[~df['crop_type'].isin(valid_crops)]
            if len(invalid_crops) > 0:
                results["checks"].append({
                    "check": "valid_crop_types",
                    "status": "FAILED",
                    "message": (
                        f"Found {len(invalid_crops)} records with invalid crop types: "
                        f"{invalid_crops['crop_type'].unique().tolist()}"
                    )
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "valid_crop_types",
                    "status": "PASSED",
                    "message": "All crop types are valid"
                })

        # Check 4: Valid growth stages
        valid_stages = [
            'Germination/Seedling', 'Vegetative', 'Flowering',
            'Grain filling', 'Maturity/Harvest'
        ]
        if 'growth_stage' in df.columns:
            invalid_stages = df[~df['growth_stage'].isin(valid_stages)]
            if len(invalid_stages) > 0:
                results["checks"].append({
                    "check": "valid_growth_stages",
                    "status": "FAILED",
                    "message": f"Found {len(invalid_stages)} records with invalid growth stages"
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "valid_growth_stages",
                    "status": "PASSED",
                    "message": "All growth stages are valid"
                })

        # Check 5: Yield reasonableness (most crops don't yield >50 tons/ha)
        if 'estimated_yield_tons_ha' in df.columns:
            unreasonable_yield = df[df['estimated_yield_tons_ha'] > 50]
            if len(unreasonable_yield) > 0:
                results["checks"].append({
                    "check": "yield_reasonableness",
                    "status": "FAILED",
                    "message": (
                        f"{len(unreasonable_yield)} records have unreasonably "
                        "high yields (>50 tons/ha)"
                    )
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "yield_reasonableness",
                    "status": "PASSED",
                    "message": "All yield values are reasonable"
                })

    except Exception as e:
        results["checks"].append({
            "check": "validation_error",
            "status": "FAILED",
            "message": f"Error during validation: {str(e)}"
        })
        results["failed"] = True

    return results


# ---------------------------------------------------------------------------
# FARMER DATA VALIDATION
# ---------------------------------------------------------------------------

def validate_farmer_data():
    """Validate farmer data quality."""
    results = {
        "dataset": "farmer_data",
        "failed": False,
        "checks": []
    }

    try:
        farmer_path = f"s3://{SILVER_BUCKET}/silver/farmers/"
        df = read_s3_parquet(farmer_path)

        if df is None or len(df) == 0:
            results["checks"].append({
                "check": "data_exists",
                "status": "FAILED",
                "message": "No farmer data found"
            })
            results["failed"] = True
            return results

        # Check 1: Unique farmer IDs
        if 'farmer_id' in df.columns:
            duplicate_ids = df[df.duplicated('farmer_id', keep=False)]
            if len(duplicate_ids) > 0:
                results["checks"].append({
                    "check": "unique_farmer_ids",
                    "status": "FAILED",
                    "message": f"Found {len(duplicate_ids)} duplicate farmer IDs"
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "unique_farmer_ids",
                    "status": "PASSED",
                    "message": "All farmer IDs are unique"
                })

        # Check 2: Farm size reasonableness
        if 'farm_size_hectares' in df.columns:
            invalid_size = df[
                (df['farm_size_hectares'] <= 0) | (df['farm_size_hectares'] > 1000)
            ]
            if len(invalid_size) > 0:
                results["checks"].append({
                    "check": "farm_size_range",
                    "status": "FAILED",
                    "message": f"{len(invalid_size)} records have invalid farm sizes"
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "farm_size_range",
                    "status": "PASSED",
                    "message": "All farm sizes are within reasonable range"
                })

        # Check 3: Contact info completeness (WARNING only — not a hard failure)
        if 'mobile_phone_owner' in df.columns:
            missing_contact = df[df['mobile_phone_owner'].isna()]
            if len(missing_contact) > 0:
                results["checks"].append({
                    "check": "contact_info_completeness",
                    "status": "WARNING",
                    "message": (
                        f"{len(missing_contact)} records missing "
                        "mobile phone ownership info"
                    )
                })

    except Exception as e:
        results["checks"].append({
            "check": "validation_error",
            "status": "FAILED",
            "message": f"Error during validation: {str(e)}"
        })
        results["failed"] = True

    return results


# ---------------------------------------------------------------------------
# FIELD / SOIL DATA VALIDATION
# ---------------------------------------------------------------------------

def validate_field_data():
    """Validate field/soil data quality."""
    results = {
        "dataset": "field_data",
        "failed": False,
        "checks": []
    }

    try:
        field_path = f"s3://{SILVER_BUCKET}/silver/fields/"
        df = read_s3_parquet(field_path)

        if df is None or len(df) == 0:
            results["checks"].append({
                "check": "data_exists",
                "status": "FAILED",
                "message": "No field data found"
            })
            results["failed"] = True
            return results

        # Check 1: Soil pH range (valid agronomic range: 3–10)
        if 'soil_ph' in df.columns:
            invalid_ph = df[(df['soil_ph'] < 3) | (df['soil_ph'] > 10)]
            if len(invalid_ph) > 0:
                results["checks"].append({
                    "check": "soil_ph_range",
                    "status": "FAILED",
                    "message": (
                        f"{len(invalid_ph)} records have invalid soil pH "
                        "(should be 3-10)"
                    )
                })
                results["failed"] = True
            else:
                results["checks"].append({
                    "check": "soil_ph_range",
                    "status": "PASSED",
                    "message": "All soil pH values within valid range"
                })

        # Check 2: Organic matter percentage (WARNING — extreme but not hard failure)
        if 'organic_matter_percentage' in df.columns:
            invalid_om = df[
                (df['organic_matter_percentage'] < 0) |
                (df['organic_matter_percentage'] > 20)
            ]
            if len(invalid_om) > 0:
                results["checks"].append({
                    "check": "organic_matter_range",
                    "status": "WARNING",
                    "message": (
                        f"{len(invalid_om)} records have extreme "
                        "organic matter values"
                    )
                })

        # Check 3: Nitrogen levels (WARNING — extreme but not hard failure)
        nutrient_cols = ['nitrogen_ppm', 'phosphorus_ppm', 'potassium_ppm']
        if all(col in df.columns for col in nutrient_cols):
            invalid_nitrogen = df[
                (df['nitrogen_ppm'] < 10) | (df['nitrogen_ppm'] > 200)
            ]
            if len(invalid_nitrogen) > 0:
                results["checks"].append({
                    "check": "nitrogen_levels",
                    "status": "WARNING",
                    "message": (
                        f"{len(invalid_nitrogen)} records have extreme "
                        "nitrogen levels"
                    )
                })

    except Exception as e:
        results["checks"].append({
            "check": "validation_error",
            "status": "FAILED",
            "message": f"Error during validation: {str(e)}"
        })
        results["failed"] = True

    return results


# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def read_s3_parquet(path, max_files=10):
    """
    Read parquet files from an S3 path.
    Capped at max_files for Lambda memory safety — logs a warning if exceeded.
    """
    try:
        import s3fs
        fs = s3fs.S3FileSystem()

        files = fs.glob(f"{path}**/*.parquet")

        if not files:
            print(f"No parquet files found at {path}")
            return None

        if len(files) > max_files:
            print(
                f"WARNING: {len(files)} parquet files found at {path}; "
                f"only reading the first {max_files}."
            )

        dfs = []
        for file in files[:max_files]:
            df = pd.read_parquet(f"s3://{file}", filesystem=fs)
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True) if dfs else None

    except Exception as e:
        print(f"Error reading parquet from {path}: {str(e)}")
        return None


def read_recent_partitions(base_path, days=7, max_files=20):
    """
    Read parquet files written within the last `days` days.
    Looks for Hive-style date partitions (year=, month=, day=) and filters
    to the relevant date range. Falls back to reading all files if no date
    partitions are found.
    """
    try:
        import s3fs
        fs = s3fs.S3FileSystem()

        cutoff = datetime.now() - timedelta(days=days)
        all_files = fs.glob(f"{base_path}**/*.parquet")

        if not all_files:
            print(f"No parquet files found at {base_path}")
            return None

        # Attempt to filter by partition path date
        recent_files = []
        for f in all_files:
            try:
                year  = int(re.search(r'year=(\d{4})',  f).group(1)) if re.search(r'year=(\d{4})',  f) else None
                month = int(re.search(r'month=(\d{1,2})', f).group(1)) if re.search(r'month=(\d{1,2})', f) else None
                day   = int(re.search(r'day=(\d{1,2})',   f).group(1)) if re.search(r'day=(\d{1,2})',   f) else None

                if year and month and day:
                    partition_date = datetime(year, month, day)
                    if partition_date >= cutoff:
                        recent_files.append(f)
                else:
                    # No date partitions detected — include the file
                    recent_files.append(f)
            except Exception:
                recent_files.append(f)  # Include on parse failure

        if not recent_files:
            print(f"No files within last {days} days at {base_path}")
            return None

        if len(recent_files) > max_files:
            print(
                f"WARNING: {len(recent_files)} recent files found; "
                f"only reading the first {max_files}."
            )

        dfs = []
        for file in recent_files[:max_files]:
            df = pd.read_parquet(f"s3://{file}", filesystem=fs)
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True) if dfs else None

    except Exception as e:
        print(f"Error reading recent partitions from {base_path}: {str(e)}")
        return None


def list_s3_partitions(base_path):
    """List all partition folders under an S3 path."""
    try:
        import s3fs
        fs = s3fs.S3FileSystem()
        return fs.glob(f"{base_path}**/")
    except Exception as e:
        print(f"Error listing partitions at {base_path}: {str(e)}")
        return []


def check_null_values(df, columns):
    """Check for null values in specified columns."""
    results = []
    total_records = len(df)

    for col in columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            null_percentage = (null_count / total_records) * 100

            if null_percentage > QUALITY_THRESHOLDS["max_null_percentage"]:
                results.append({
                    "check": f"null_values_{col}",
                    "status": "FAILED",
                    "message": (
                        f"{null_percentage:.1f}% null values in {col} "
                        f"(threshold: {QUALITY_THRESHOLDS['max_null_percentage']}%)"
                    ),
                    "actual":   f"{null_percentage:.1f}%",
                    "expected": f"<{QUALITY_THRESHOLDS['max_null_percentage']}%"
                })
            elif null_percentage > 0:
                results.append({
                    "check": f"null_values_{col}",
                    "status": "WARNING",
                    "message": f"{null_percentage:.1f}% null values in {col}",
                    "actual":  f"{null_percentage:.1f}%"
                })
            else:
                results.append({
                    "check": f"null_values_{col}",
                    "status": "PASSED",
                    "message": f"No null values in {col}"
                })

    return results


def check_value_ranges(df, range_dict):
    """Check if values fall within expected ranges."""
    results = []

    for column, (min_val, max_val) in range_dict.items():
        if column in df.columns:
            invalid = df[(df[column] < min_val) | (df[column] > max_val)]
            invalid_percentage = (len(invalid) / len(df)) * 100

            if invalid_percentage > QUALITY_THRESHOLDS["max_outlier_percentage"]:
                results.append({
                    "check": f"range_check_{column}",
                    "status": "FAILED",
                    "message": (
                        f"{invalid_percentage:.1f}% values outside range "
                        f"{min_val}-{max_val} in {column}"
                    ),
                    "actual":   f"{invalid_percentage:.1f}%",
                    "expected": f"<{QUALITY_THRESHOLDS['max_outlier_percentage']}%"
                })
            elif invalid_percentage > 0:
                results.append({
                    "check": f"range_check_{column}",
                    "status": "WARNING",
                    "message": f"{invalid_percentage:.1f}% outliers in {column}",
                    "actual":  f"{invalid_percentage:.1f}%"
                })
            else:
                results.append({
                    "check": f"range_check_{column}",
                    "status": "PASSED",
                    "message": f"All {column} values within range {min_val}-{max_val}"
                })

    return results


def check_data_freshness(df, date_column):
    """
    Check if data is up to date.
    Handles both timezone-aware and naive timestamps to avoid TypeError.
    """
    if date_column not in df.columns:
        return {
            "check": "data_freshness",
            "status": "WARNING",
            "message": f"Date column '{date_column}' not found"
        }

    # Normalise to datetime
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df[date_column] = pd.to_datetime(df[date_column])

    latest_date = df[date_column].max()

    # Match timezone-awareness between latest_date and now
    if hasattr(latest_date, 'tzinfo') and latest_date.tzinfo is not None:
        now = datetime.now(timezone.utc)
    else:
        now = datetime.now()

    hours_ago = (now - latest_date).total_seconds() / 3600

    if hours_ago > QUALITY_THRESHOLDS["freshness_hours"]:
        return {
            "check": "data_freshness",
            "status": "FAILED",
            "message": (
                f"Latest data is {hours_ago:.1f} hours old "
                f"(threshold: {QUALITY_THRESHOLDS['freshness_hours']} hours)"
            ),
            "actual":   f"{hours_ago:.1f} hours",
            "expected": f"<{QUALITY_THRESHOLDS['freshness_hours']} hours"
        }

    return {
        "check": "data_freshness",
        "status": "PASSED",
        "message": f"Data is fresh (latest: {latest_date})",
        "actual":  f"{hours_ago:.1f} hours old"
    }


def check_partition_completeness(partitions, expected_partition_cols):
    """Check if partition paths follow the expected Hive-style structure."""
    if not partitions:
        return {
            "check": "partition_completeness",
            "status": "FAILED",
            "message": "No partitions found",
            "actual":   "0%",
            "expected": ">80%"
        }

    valid_partitions = sum(
        1 for p in partitions
        if all(f"{col}=" in p for col in expected_partition_cols)
    )
    completeness = (valid_partitions / len(partitions)) * 100

    if completeness < 80:
        return {
            "check": "partition_completeness",
            "status": "FAILED",
            "message": (
                f"Only {completeness:.1f}% of partitions follow "
                "expected structure"
            ),
            "actual":   f"{completeness:.1f}%",
            "expected": ">80%"
        }

    return {
        "check": "partition_completeness",
        "status": "PASSED",
        "message": (
            f"{completeness:.1f}% of partitions follow expected structure"
        ),
        "actual": f"{completeness:.1f}%"
    }


def check_logical_consistency(df, rules):
    """Check logical consistency rules."""
    results = []

    for rule_name, rule_info in rules.items():
        try:
            violations = ~rule_info['condition']
            violation_count = int(violations.sum()) if hasattr(violations, 'sum') else len(violations)
            violation_percentage = (violation_count / len(df)) * 100 if len(df) > 0 else 0

            if violation_count > 0:
                results.append({
                    "check": f"logical_consistency_{rule_name}",
                    "status": "FAILED" if violation_percentage > 5 else "WARNING",
                    "message": (
                        f"{violation_count} records ({violation_percentage:.1f}%) "
                        f"violate: {rule_info['message']}"
                    )
                })
            else:
                results.append({
                    "check": f"logical_consistency_{rule_name}",
                    "status": "PASSED",
                    "message": f"All records satisfy: {rule_info['message']}"
                })
        except Exception as e:
            results.append({
                "check": f"logical_consistency_{rule_name}",
                "status": "ERROR",
                "message": f"Error checking rule: {str(e)}"
            })

    return results


# ---------------------------------------------------------------------------
# SNS ALERTS
# ---------------------------------------------------------------------------

def send_quality_alert(results):
    """Send SNS alert for data quality failures."""

    failed_lines = []
    for failed in results["failed_checks"]:
        failed_lines.append(f"\n{failed['dataset'].upper()}:")
        for check in failed["checks"]:
            if check["status"] == "FAILED":
                failed_lines.append(f"  [FAILED]  {check['check']}: {check['message']}")
            elif check["status"] == "WARNING":
                failed_lines.append(f"  [WARNING] {check['check']}: {check['message']}")

    subject = f"DATA QUALITY ALERT - {results['overall_status']}"

    message = f"""
AWS Data Quality Alert
======================

Status: {results['overall_status']}
Time:   {results['execution_time']}

FAILED CHECKS:
{chr(10).join(failed_lines) if failed_lines else 'No failed checks'}

SUMMARY:
- Total checks performed : {len(results['checks_performed'])}
- Failed datasets        : {len(results['failed_checks'])}
- Datasets with warnings : {len(results.get('warning_checks', []))}

Recommendation:
1. Review the failed checks above
2. Check the full quality report in S3
3. Investigate data pipeline issues

S3 Quality Report: s3://{SILVER_BUCKET}/quality_reports/{results['execution_time'].split('T')[0]}/quality_report.json
"""

    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
            MessageAttributes={
                'AlertType': {
                    'DataType': 'String',
                    'StringValue': 'DataQuality'
                },
                'Severity': {
                    'DataType': 'String',
                    'StringValue': 'HIGH' if len(results['failed_checks']) > 2 else 'MEDIUM'
                }
            }
        )
        print(f"Alert sent to SNS: {response['MessageId']}")

    except Exception as e:
        print(f"Failed to send SNS alert: {str(e)}")


def send_success_notification(results):
    """Send success notification when all quality checks pass."""

    subject = "DATA QUALITY PASSED - Agriculture Silver Layer"

    message = f"""
Data Quality Check Passed
========================

Time:   {results['execution_time']}
Status: PASSED

All quality checks passed successfully:
- Weather data : Validated
- Crop data    : Validated
- Farmer data  : Validated
- Field data   : Validated

Total checks : {len(results['checks_performed'])}
All data is ready for analytics.
"""

    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
            MessageAttributes={
                'AlertType': {
                    'DataType': 'String',
                    'StringValue': 'Success'
                }
            }
        )
        print(f"Success notification sent: {response['MessageId']}")

    except Exception as e:
        print(f"Failed to send success notification: {str(e)}")


def send_error_alert(error_message):
    """Send alert for validation function errors."""

    subject = "DATA QUALITY FUNCTION ERROR"

    message = f"""
Data Quality Validation Function Error
======================================

Time:  {datetime.now().isoformat()}
Error: {error_message}

Check CloudWatch logs for details.
"""

    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"Error alert sent: {response['MessageId']}")

    except Exception as e:
        print(f"Failed to send error alert: {str(e)}")


# ---------------------------------------------------------------------------
# CLOUDWATCH METRICS
# ---------------------------------------------------------------------------

def push_metrics_to_cloudwatch(results):
    """
    Push quality metrics to CloudWatch for all datasets
    (both passed and failed) so dashboards have full coverage.
    """
    all_datasets = (
        results.get("failed_checks", []) +
        results.get("passed_checks", []) +
        results.get("warning_checks", [])
    )

    for dataset_result in all_datasets:
        metric_value = 1 if dataset_result['failed'] else 0
        status_label = 'FAILED' if dataset_result['failed'] else 'PASSED'

        try:
            cloudwatch_client.put_metric_data(
                Namespace='Agriculture/DataQuality',
                MetricData=[
                    {
                        'MetricName': f"DataQuality_{dataset_result['dataset']}",
                        'Value': metric_value,
                        'Unit': 'Count',
                        'Timestamp': datetime.now(timezone.utc),
                        'Dimensions': [
                            {
                                'Name': 'Dataset',
                                'Value': dataset_result['dataset']
                            },
                            {
                                'Name': 'Status',
                                'Value': status_label
                            }
                        ]
                    }
                ]
            )
        except Exception as e:
            print(f"Failed to push metric for {dataset_result['dataset']}: {str(e)}")

    print("Metrics pushed to CloudWatch")


# ---------------------------------------------------------------------------
# REPORT PERSISTENCE
# ---------------------------------------------------------------------------

def save_quality_report(results):
    """Save the full quality report as JSON to S3."""

    date_prefix = datetime.now().strftime('%Y/%m/%d')
    timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"quality_reports/{date_prefix}/quality_report_{timestamp}.json"

    try:
        s3_client.put_object(
            Bucket=SILVER_BUCKET,
            Key=key,
            Body=json.dumps(results, indent=2, default=str),
            ContentType='application/json'
        )
        print(f"Quality report saved to: s3://{SILVER_BUCKET}/{key}")

    except Exception as e:
        print(f"Failed to save quality report: {str(e)}")