from pydantic import BaseModel,PositiveInt,PositiveFloat,NegativeInt,field_validator,Field
from ingestion import EastAfricaAgricultureDataGenerator
from typing import Optional,Annotated
from datetime import datetime,date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

generator=EastAfricaAgricultureDataGenerator()
crop_growth_data=generator.generate_crop_growth_data(location='Nairobi_Kenya',crop_name='Maize',planting_date='2025-03-31')

class Crop_growth(BaseModel):
    location: Annotated[
        str,
        Field(
            title = 'Location',
            description =
            "Standardized location identifier used in East African agricultural datasets. "
            "Format: City_Country (e.g. Nairobi_Kenya). Used for partitioning in data lake.",
            examples=['Nairobi_Kenya','Nakuru_Kenya','Kampala_uganda']

        )
    ]

    crop_name: Annotated[
        str,
        Field(
            title = 'Crop Name',
            description = "name of the cultivated crop used for agronomic modeling and yield prediction",
            examples=['Maize',"Beans","Cassava"]
        )
    ]

    planting_date: Annotated[
        date,
        Field(
            title = 'Planting Date',
            description = 'Date when the crop was planted (YYYY-MM-DD)',
            examples=['2025-03-31','2025-03-31']

        )
    ]

    date: Annotated[
        date,
        Field(
            title = 'Observation Date',
            description = 'daily timestamp of crop growth observation',
            examples=['2025-03-31','2025-03-31']

        )
    ]

    day_after_planting: Annotated[
        int,
        Field(
            title = 'Day After Planting',
            description = ' Number of days since planting date used for growth stage modelling ',
            ge=1
        )
    ]
    growth_stage: Annotated[
        str,
        Field(
            title = 'Growth Stage',
            description =' Biological stage of crop development',
            examples=[
                'Germination/seedling',
                'Vegetative',
                'Flowering',
                'Grain filling',
                'Maturity/Harvest'
            ]
        )]

    canopy_cover_pct: Annotated[
        PositiveFloat,
        Field(
            title = 'Canopy Cover Percentage',
            description = 'Estimated percentage of ground covered by crop canopy',
            ge=0,
            le=100
        )
    ]

    planting_height_cm: Annotated[
        PositiveFloat,
        Field(
            title = 'Planting Height (m)',
            description='Siumulated crop measured from soil surface',
            ge=0
        )
    ]

    health_score: Annotated[
        float,
        Field(
            title = 'Crop Health Score',
            description= 'Overall crop health index derived from environmental and growth factors',
            ge=0,
            le=100
        )
    ]

    stress_level: Annotated[
        str,
        Field(
            title = 'Crop Stress Level',
            description= 'Indicates physiological stress level of crop',
            examples=['Normal',"Severe","Mild"]
        )
    ]

    estimated_yield_tons_ha: Annotated[
        PositiveFloat,
        Field(
            title = 'Estimated Yield (tons/ha)',
            description= 'Predicted agricultural yield perr hectare based on health score and crop model',
            ge=0,
        )
    ]

    pest_risk: Annotated[
        str,
        Field(
            title= "Pest Risk Indicator",
            description='Risk level and type of pest or diseases affecting the crop',
            examples=[
                "Low risk",
                'Moderate risk of Aphids',
                'High risk of Fall Armyworm'
            ]
        )
    ]

    irrigation_needed: Annotated[
        bool,
        Field(
            title= "Irrigation Requirement",
            description = 'Indicates whether irrigation is required for the crop on that day'
        )
    ]

    @field_validator('date')
    @classmethod

    def planting_date_validator(cls, value):
        if value > date.today():
            raise ValueError('Date cannot be in the future')
        return value

def validate_data(crop_growth_data):

    validated=[]

    for date,record in crop_growth_data.items():
        try:
            object=Crop_growth(**record)
            validated.append(object.model_dump())
            logger.info('Successfully validated crop growth data')
        except Exception as e:
            logger.error(f'Validation failed for the record : {date}: error:{e}')
    return validated

if __name__ == '__main__':
    validate_data(crop_growth_data)
