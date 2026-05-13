from pydantic import BaseModel, PositiveInt, PositiveFloat, NegativeInt, field_validator, Field, model_validator
from ingestion import EastAfricaAgricultureDataGenerator
from typing import Optional,Annotated,Literal
from datetime import datetime,date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

generator=EastAfricaAgricultureDataGenerator()
daily_weather_data= generator.generate_daily_weather('Nairobi_Kenya')


class Daily_Weather(BaseModel):
    date: Annotated[
        date,
        Field(
            title='date',
            description= 'Date of the weather observation',
            examples= ['2025-04-01']
        )
    ]

    location: Annotated[
        str,
        Field(
            title='location',
            description= 'Location of the weather observation',
            examples= ['Nairobi, Kenya'])
    ]

    longitude: Annotated[
        float,
        Field(
            title='longitude',
            description = 'Longitude coordinate',
            ge=-180,
            le=180,
            examples= [36.8172]
        )
    ]

    latitude: Annotated[
        float,
        Field(
            title='latitude',
            description= 'Latitude coordinate',
            ge=-90,
            le=90,
            examples= [-1.2864]
        )
    ]

    elevation_m: Annotated[
        float,
        Field(
            title='elevation_m',
            description = 'Elevation above sea level in meters',
            ge=-1000,
            le=9000,
        )
    ]

    agro_zone: Annotated[
        str,
        Field(
            title='agro_zone',
            description='Agricultural ecological zone',
            examples= ['Highland','Lowland','Semi-Arid','Coastal']
        )
    ]

    temp_min_c: Annotated[
        float,
        Field(
            title='Minimum temeperature',
            description= 'Minimum temperature in celsius',
            ge=-20,
            le=60
        )
    ]

    temp_max_c: Annotated[
        float,
        Field(
            title='Maximum temperature',
            description= 'Maximum temperature in celsius',
            ge=-20,
            le=60
        )
    ]

    temp_avg_c: Annotated[
        float,
        Field(
            title='Average temperature',
            description= 'Average temperature in celsius',
            ge=-20,
            le=60
        )
    ]

    humidity_pct: Annotated[
        int,
        Field(
            title='humidity percentage',
            description = "Relative humidity percentage",
            ge=0,
            le=100
        )
    ]

    precipitation_mm: Annotated[
        float,
        Field(
            title='precipitation mm',
            description = "Rainfall in millimeters",
            ge=0
        )
    ]

    evapotranspiration_mm: Annotated[
        float,
        Field(
            title='evapotranspiration mm',
            description = "Evapotranspiration in millimeters",
            ge=0
        )
    ]

    solar_radiation_mj_m2: Annotated[
        float,
        Field(
            title='solar radiation mj2',
            description = "solar radiation in MJ/m2",
            ge=0,
            le= 40
        )
    ]

    wind_speed_kmh: Annotated[
        float,
        Field(
            title='wind speed km/h',
            description = "Wind speed in km/h",
            ge=0,
            le=250
        )
    ]

    soil_moisture_vol_pct: Annotated[
        float,
        Field(
            title='soil moisture vol %',
            description = "Volumetric soil moisture percentage",
            ge=0,
            le=100
        )
    ]

    soil_temp_c: Annotated[
        float,
        Field(
            title='soil temperature',
            description = "Soil temperature in celsius",
            ge=-20,
            le=60
        )
    ]

    drought_risk: Annotated[
        str,
        Field(
            title='drought risk',
            description = "Drought risk",
            examples= ['low','medium','high']
        )
    ]

    flood_risk: Annotated[
        str,
        Field(
            title='flood risk',
            description = "Flood risk",
            examples=['low','medium','high']
        )
    ]

    frost_risk: Annotated[
        str,
        Field(
            title='frost risk',
            description = "Frost risk",
            examples=['Yes','No']
        )
    ]

    @field_validator('date')
    @classmethod

    def validate_date(cls,value):
        if value > date.today():
            raise ValueError('Weather date cannot be in the future')
        return value

    @model_validator(mode='after')
    def validate_temperature(self):
        if self.temp_min_c > self.temp_max_c:
            raise ValueError('Minimum temperature cannot be greater than maximum temperature')

        if not (
            self.temp_min_c <= self.temp_avg_c <= self.temp_max_c
        ):
            raise ValueError("Average temperature must be between min and max temperature")


        return self


def validate_data(daily_weather_data):
    validated=[]

    for date, record in daily_weather_data.items():

        try:
            object=Daily_Weather(**record)
            validated.append(object.model_dump())
            logger.info('Successfully validated daily weather data')

        except Exception as e:
            logger.error(f'Validation failed for the record:{date} : error{e}')
    return validated

if __name__ == '__main__':
    validate_data(daily_weather_data)