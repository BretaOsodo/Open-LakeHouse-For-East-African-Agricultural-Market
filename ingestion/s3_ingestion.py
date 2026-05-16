import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()


bronze_bucket_name=os.getenv("BRONZE_BUCKET")
aws_access_key_id=os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
json_file=r'C:\Users\ADMIN\Documents\Projects\Open LakeHouse for EastAfrica Agricultural market Data\ingestion\east_africa_agriculture_dict.json'

def upload_all_partitioned_data():
    """
    Upload all partitioned data to an S3 bucket with partitioning
    :return:
    """

    #load the JSON file
    print(f'Loading data from {json_file}')
    with open(json_file) as json_data:
        agriculture_data = json.load(json_data)


    #connect to the bronze bucket
    s3_client = boto3.client('s3')
    stats={'weather':0,'crops':0,'farmers':0,'fields':0}

    #Helper function to parse location
    def parse_location(location_string):

        #find the last underscore to separate city and country
        last_underscore = location_string.rfind('_')
        if last_underscore == -1:
            return location_string,'Unknown'

        else:
            city = location_string[:last_underscore]
            country = location_string[last_underscore+1:]
            return city,country


    #1. Upload weather data
    if 'weather_data' in agriculture_data:
        print('Uploading Weather data...')

        for location, weather_records in agriculture_data['weather_data'].items():
            city,country=parse_location(location)
            for date, daily_data in weather_records.items():
                year,month,day = date.split('-')
                s3_key=f'weather/country={country}/city={city}/year={year}/month={month}/day={day}/weather.json'

                try:
                    s3_client.put_object(
                        Bucket=bronze_bucket_name,
                        Key=s3_key,
                        Body=json.dumps(daily_data,indent=2),
                        ContentType='application/json'
                    )
                    stats['weather']+=1
                except Exception as e:
                    stats['error']+=1
                    print(f'Error:{s3_key} - {e}')

        print(f'Uploaded {stats['weather']} weather files ')

    #2. Upload crop growth data
    if "crop_growth_data" in  agriculture_data:
        print('Uploading Crop growth data...')

        for location, crop_data in agriculture_data['crop_growth_data'].items():

            city,country=parse_location(location)

            for crop_name, planting in crop_data.items():
                for planting_date, growth_records in planting.items():
                    for growth_date, growth_data in growth_records.items():
                        year, month, day= growth_date.split('-')
                        s3_key=f"crops/country={country}/city={city}/year={year}/month={month}/day={day}/growth.json"

                        try:
                            s3_client.put_object(
                                Bucket=bronze_bucket_name,
                                Key=s3_key,
                                Body=json.dumps(growth_data,indent=2),
                                ContentType='application/json'
                            )

                            stats['crops']+=1

                        except Exception as e:
                            stats['error']+=1

        print(f'uploaded {stats["crops"]} crop growth files')

    #3. Upload Farmer Data

    if "farmer_data" in agriculture_data:
        print('Uploading Farmer data...')

        for location, farmers in agriculture_data['farmer_data'].items():
            city, country=parse_location(location)
            now = datetime.now()
            for farmer_id, farmer_info in farmers.items():
                s3_key=f'farmers/county={country}/city={city}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{farmer_id}.json/'
                try:
                    s3_client.put_object(
                        Bucket=bronze_bucket_name,
                        Key=s3_key,
                        Body=json.dumps(farmer_info,indent=2),
                        ContentType='application/json'
                    )

                    stats['farmers']+=1

                except Exception as e:
                    stats['error']+=1
        print(f'Uploaded {stats["farmers"]} farmer files')

    #4. Upload field data
    if 'field_data' in agriculture_data:
        print('Uploading Field data...')

        for location, fields in agriculture_data['field_data'].items():
            city,country=parse_location(location)
            now = datetime.now()

            for field_id , field_Info in fields.items():
                s3_key=f'fields/country={country}/city={city}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{field_id}.json'

                try:
                    s3_client.put_object(
                        Bucket=bronze_bucket_name,
                        Key=s3_key,
                        Body=json.dumps(field_Info,indent=2),
                        ContentType='application/json'
                    )

                    stats['fields']+=1

                except Exception as e:

                    stats['error']+=1

            print(f'Uploaded {stats["fields"]} field data')


#Run the upload
upload_all_partitioned_data()