from ingestion.generate_data import EastAfricaAgricultureDataGenerator
import json
generator= EastAfricaAgricultureDataGenerator()
crop_growth=generator.generate_crop_growth_data(location='Nairobi_Kenya',crop_name='Maize',planting_date='2025-03-31')
print(json.dumps(crop_growth, indent=4))