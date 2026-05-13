from ingestion import EastAfricaAgricultureDataGenerator
import pytest

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

    generator= EastAfricaAgricultureDataGenerator()

    data= generator.generate_daily_weather(location)

    assert isinstance(data,dict)
    assert data is not None
    assert len(data) > 0



if __name__ == '__main__':
    test_daily_weather_generation()
