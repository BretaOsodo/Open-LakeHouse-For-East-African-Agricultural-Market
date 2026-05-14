from pydantic import BaseModel, PositiveInt, PositiveFloat, NegativeInt, field_validator, Field, model_validator
from ingestion import EastAfricaAgricultureDataGenerator
from typing import Optional,Annotated,Literal
from datetime import datetime,date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

generator=EastAfricaAgricultureDataGenerator()
farmers_data = generator.generate_farmer_data('Nairobi_Kenya')

class FarmerData(BaseModel):
    location: Annotated[
        str,
        Field(
            title = 'Location',
            description = 'Location of the farmer',

        )
    ]

    farm_size_hectares: Annotated[
        float,
        Field(
            title = 'Farm Size Hectares',
            description = 'The size of the farm in hectares',
            ge=0

        )
    ]

    crop_grown: Annotated[
        list[str],
        Field(
            title = 'Crop Grown',
            description = 'Crop grown by the farmer',
        )
    ]

    farming_practice: Annotated[
        str,
        Field(
            title = 'Farming Practice',
            description = 'Farming Practice the farmer uses'
        )
    ]

    access_to_extension: Annotated[
        bool,
        Field(
            title = 'Access to Extensions',
            description = 'Access to extensions by the farmer'
        )
    ]

    mobile_phone_owner: Annotated[
        bool,
        Field(
            title = 'Mobile Phone Owner',
            description = 'Does the farmer owns a mobile phone?'
        )
    ]

    uses_weather_app: Annotated[
        bool,
        Field(
            title = 'Use Weather App',
            description = 'Does the farmer use weather app?'
        )
    ]

    cooperative_member: Annotated[
        bool,
        Field(
            title = 'Cooperative Member',
            description = 'Is the farmer a cooperative member?'
        )
    ]

    registered_date: Annotated[
        date,
        Field(
            title = 'Registered Date',
            description = 'The date the farmer registered'
        )
    ]


    @field_validator(
        'access_to_extension',
        'mobile_phone_owner',
        'uses_weather_app',
        'cooperative_member'
    )

    @classmethod
    def validate_farmers_activity(cls,value):
        allowed = [True, False]

        if value not in allowed:
            raise ValueError('Farmer activity must be either True or False')
        return value

def validate_farmer_data(farmers_data):
    validated =[]

    for farmers_id, records in farmers_data.items():
        try:
            object = FarmerData(**records)
            validated.append(object.model_dump())
            logger.info('Successfully validated the farmers records')

        except Exception as e:
            logger.info(f'Validation failed for {farmers_id}: erorr:{e}')

if __name__=='__main__':
    validate_farmer_data(farmers_data)
