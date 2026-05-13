from ingestion import EastAfricaAgricultureDataGenerator
import pytest

generator = EastAfricaAgricultureDataGenerator()

@pytest.mark.parametrize("location",[
    "Nairobi_Kenya",
    "Kampala_Uganda",
    "Nakuru_Kenya",
    "Dar_es_Salaam_Tanzania",
    "Arusha_Tanzania",
    "Kigali_Rwanda",
    "Addis_Ababa_Ethiopia",
    "Juba_South_Sudan",
    "Mbale_Uganda",
    "Mwanza_Tanzania"
])


def test_daily_weather_generation(location):
    """
    Does it return a list or dictionary
    is it empty or filled
    :return:
    """

    data= generator.generate_daily_weather(location)

    assert isinstance(data,dict)
    assert data is not None
    assert len(data) > 0

@pytest.mark.parametrize("location,crop_name",[
    ("Nairobi_Kenya","Maize"),
    ("Kampala_Uganda","Beans"),
    ("Nakuru_Kenya","Coffee"),
    ("Dar_es_Salaam_Tanzania","Cassava"),
    ("Arusha_Tanzania","Cassava"),
    ("Kigali_Rwanda","Banana"),
    ("Addis_Ababa_Ethiopia","Sorghum"),
    ("Juba_South_Sudan","Millet"),
    ("Mbale_Uganda","Irish_Potato"),
    ("Mwanza_Tanzania","Sweet_Potato"),
])
def test_crop_growth_data(location,crop_name):
    data=generator.generate_crop_growth_data(
        location=location,
        crop_name=crop_name,
        planting_date='2025-03-31'
    )

    assert isinstance(data,dict)
    assert data is not None
    assert len(data) > 0

if __name__ == '__main__':
    test_daily_weather_generation()
    test_crop_growth_data()
