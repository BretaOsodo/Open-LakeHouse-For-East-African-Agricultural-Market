from pydantic import BaseModel, PositiveInt, PositiveFloat, NegativeInt, field_validator, Field, model_validator
from ingestion import EastAfricaAgricultureDataGenerator
from typing import Optional,Annotated,Literal
from datetime import datetime,date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

generator = EastAfricaAgricultureDataGenerator()
field_data = generator.generate_field_data('Nairobi_Kenya')

class FieldData(BaseModel):
    location: Annotated[
        str,
        Field(
            title = 'Location',
            description='The location of the field data',
            examples=['Nairobi_Kenya']
        )
    ]

    soil_type: Annotated[
        str,
        Field(
            title = 'Soil Type',
            description='The soil type of the field data',
            examples=['Andosol']
        )
    ]

    soil_ph: Annotated[
        float,
        Field(
            title = 'Soil Ph',
            description='The soil ph of the field data',
            ge=0
        )
    ]

    organic_matter_pct: Annotated[
        PositiveFloat,
        Field(
            title = 'Organic Matter Pct',
            description='The percentage of the organic matter in the field data',
            ge=0,
            le=100
        )
    ]

    nitrogen_ppm: Annotated[
        float,
        Field(
            title = 'Nitrogen Ppm',
            ge=0
        )
    ]

    phosphorous_ppm: Annotated[
        float,
        Field(
            title = 'Phosphorus Ppm',
            description='The amount of the phosphorus in the field data',
            ge=0
        )
    ]

    potassium_ppm: Annotated[
        float,
        Field(
            title = 'Potassium Ppm',
            description='The amount of the potassium in the field data',
            ge=0
        )
    ]

    cation_exchange_capacity: Annotated[
        float,
        Field(
            title = 'Cation Exchange Capacity',
            ge=0
        )
    ]

    drainage_class : Annotated[
        str,
        Field(
            title = 'Drainage Class',
            description =' The state of the drainage in the field',
            examples=[
                'Well drained',
                'Moderate',
                'Poor'
            ]
        )
    ]

    erosion_risk: Annotated[
        str,
        Field(
            title = 'Erosion Risk',
            description= ' The risk of erosion in the field',
            examples=[
                'Low',
                'Moderate',
                'High'
            ]
        )
    ]

    last_soil_test_date: Annotated[
        date,
        Field(
            title = 'Last Soil Test Date',
            description=' The date of the last soil test',
            examples=['2025-05-20']
        )
    ]

    @field_validator('soil_ph')
    @classmethod
    def validate_soil_ph(cls, value):
        if not 0 <= value <= 14:
            raise ValueError('Soil ph must be between 0 and 14')
        return value

    @field_validator('organic_matter_pct')
    @classmethod
    def validate_organic_matter_pct(cls, value):

        if not 0 <= value <= 100:
            raise ValueError('Organic matter pct must be between 0 and 100')
        return value

    @field_validator(
        'nitrogen_ppm',
        'phosphorous_ppm',
        'potassium_ppm',
    )

    @classmethod
    def validate_nutrients(cls, value):
        if value < 0:
            raise ValueError('Nutrients must not be negative')
        return value

    @field_validator('cation_exchange_capacity')
    @classmethod
    def validate_cation_exchange_capacity(cls, value):
        if value < 0:
            raise ValueError('Cation exchange capacity must not be negative')
        return value

    @field_validator('drainage_class')
    @classmethod
    def validate_drainage_class(cls, value):
        allowed =[
            'Well drained',
            'Moderate',
            'Poor'
        ]

        if value not in allowed:
            raise ValueError(f'Drainage class must be one of {allowed}')
        return value

    @field_validator('erosion_risk')
    @classmethod
    def validate_erosion_risk(cls, value):
        allowed =['Low', 'Moderate', 'High']

        if value not in allowed:
            raise ValueError(f'Erosion risk must be one of {allowed}')
        return value

def validate_field_data(field_data):

    validated=[]

    for field_id, records in field_data.items():
        try:
            object = FieldData(**records)
            validated.append(object.model_dump())
            logger.info('Successfully validated the field data')
        except Exception as e:
            logger.error(f'Validation failed for {field_id}: error: {e}')

if __name__=='__main__':
    validate_field_data(field_data)