from ingestion import EastAfricaAgricultureDataGenerator
import json
generator = EastAfricaAgricultureDataGenerator()

farmers_data = generator.generate_farmer_data('Nairobi_Kenya')
print(json.dumps(farmers_data, indent=4))