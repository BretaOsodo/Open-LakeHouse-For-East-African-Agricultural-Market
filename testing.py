from ingestion.generate_data import EastAfricaAgricultureDataGenerator

generator= EastAfricaAgricultureDataGenerator()
crop_growth=generator.generate_crop_growth_data(location='Nairobi_Kenya',crop_name='Maize',planting_date='2025-03-31')
print(crop_growth)