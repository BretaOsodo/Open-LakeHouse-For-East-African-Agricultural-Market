from ingestion.generate_data import EastAfricaAgricultureDataGenerator
import json
generator= EastAfricaAgricultureDataGenerator()

weather_data = generator.generate_daily_weather('Nairobi_Kenya')
print(json.dumps(weather_data, indent=4))