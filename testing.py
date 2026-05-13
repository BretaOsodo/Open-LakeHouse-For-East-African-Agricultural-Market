from ingestion.generate_data import EastAfricaAgricultureDataGenerator

generator= EastAfricaAgricultureDataGenerator()
weather_nairobi= generator.generate_daily_weather(location="Nairobi_Kenya")
weather_nakuru= generator.generate_daily_weather(location="Nakuru_Kenya")
weather_kampala= generator.generate_daily_weather(location="Kampala_Uganda")
print(weather_nairobi)
print(weather_nakuru)
print(weather_kampala)