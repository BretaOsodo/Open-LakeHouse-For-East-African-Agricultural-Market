
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

class EastAfricaAgricultureDataGenerator:

    """Generate agricultural weather and crop data for East African farmers"""

    #East African locations with coeerdinates and agricultural zones
    LOCATIONS={
        "Nairobi_Kenya": {"lat": -1.2864, "lon": 36.8172, "elevation_m": 1795, "agro_zone": "Highland",
                          "rainfall_pattern": "Bimodal"},
        "Nakuru_Kenya": {"lat": -0.3031, "lon": 36.0800, "elevation_m": 1850, "agro_zone": "Highland",
                         "rainfall_pattern": "Bimodal"},
        "Kampala_Uganda": {"lat": 0.3136, "lon": 32.5811, "elevation_m": 1190, "agro_zone": "Lake Victoria Basin",
                           "rainfall_pattern": "Bimodal"},
        "Dar_es_Salaam_Tanzania": {"lat": -6.7924, "lon": 39.2083, "elevation_m": 12, "agro_zone": "Coastal",
                                   "rainfall_pattern": "Unimodal"},
        "Arusha_Tanzania": {"lat": -3.3869, "lon": 36.6820, "elevation_m": 1400, "agro_zone": "Highland",
                            "rainfall_pattern": "Bimodal"},
        "Kigali_Rwanda": {"lat": -1.9441, "lon": 30.0619, "elevation_m": 1567, "agro_zone": "Highland",
                          "rainfall_pattern": "Bimodal"},
        "Addis_Ababa_Ethiopia": {"lat": 9.0320, "lon": 38.7469, "elevation_m": 2355, "agro_zone": "Highland",
                                 "rainfall_pattern": "Unimodal"},
        "Juba_South_Sudan": {"lat": 4.8594, "lon": 31.5713, "elevation_m": 382, "agro_zone": "Lowland",
                             "rainfall_pattern": "Unimodal"},
        "Mbale_Uganda": {"lat": 1.0759, "lon": 34.1755, "elevation_m": 1200, "agro_zone": "Eastern Highlands",
                         "rainfall_pattern": "Bimodal"},
        "Mwanza_Tanzania": {"lat": -2.5164, "lon": 32.9172, "elevation_m": 1140, "agro_zone": "Lake Zone",
                            "rainfall_pattern": "Bimodal"}
    }

    #East African crops with their growing parameters
    CROPS={
        "Maize": {"growing_days": 120, "water_need_mm": 500, "temp_min_c": 18, "temp_max_c": 32,
                  "rainfall_opt_mm": 450},
        "Beans": {"growing_days": 90, "water_need_mm": 300, "temp_min_c": 15, "temp_max_c": 28, "rainfall_opt_mm": 350},
        "Coffee": {"growing_days": 365, "water_need_mm": 1500, "temp_min_c": 17, "temp_max_c": 24,
                   "rainfall_opt_mm": 1200},
        "Tea": {"growing_days": 365, "water_need_mm": 1400, "temp_min_c": 15, "temp_max_c": 25,
                "rainfall_opt_mm": 1300},
        "Cassava": {"growing_days": 240, "water_need_mm": 500, "temp_min_c": 20, "temp_max_c": 35,
                    "rainfall_opt_mm": 600},
        "Sweet_Potato": {"growing_days": 120, "water_need_mm": 400, "temp_min_c": 18, "temp_max_c": 30,
                         "rainfall_opt_mm": 450},
        "Banana": {"growing_days": 365, "water_need_mm": 1200, "temp_min_c": 20, "temp_max_c": 30,
                   "rainfall_opt_mm": 1000},
        "Sorghum": {"growing_days": 110, "water_need_mm": 350, "temp_min_c": 22, "temp_max_c": 35,
                    "rainfall_opt_mm": 400},
        "Millet": {"growing_days": 90, "water_need_mm": 300, "temp_min_c": 25, "temp_max_c": 40,
                   "rainfall_opt_mm": 350},
        "Irish_Potato": {"growing_days": 100, "water_need_mm": 450, "temp_min_c": 10, "temp_max_c": 22,
                         "rainfall_opt_mm": 500}
    }

    #soil type common in East Africa
    SOIL_TYPES=["Luvisol", "Ferralsol", "Acrisol", "Nitisol", "Andosol", "Vertisol", "CAMBISOL"]

    def __init__(self,start_date: str="2025-01-01",end_date: str="2025-12-31"):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")

    def _get_growth_stage(self,day: int,total_days: int) ->str:
        """Determine crop growth stage based on day after planting"""
        stage_pct = day/total_days

        if stage_pct < 0.2:
            return "Germination/ Seedling"
        elif stage_pct < 0.4:
            return "Vegetative"
        elif stage_pct < 0.6:
            return "Flowering"
        elif stage_pct < 0.8:
            return "Grain filling"
        else:
            return "Maturity/harvest"

    def _calculate_height(self,day: int,total_days: int,crop: str) -> float:
        """Calculate plant height in cm based on crop type and growth stage"""

        base_height={
            "Maize":200,
            "Beans":50,
            "Coffee":300,
            "Tea":150,
            "Cassava":200,
            "Sweet_Potato":30,
            "Banana":400,
            "Sorghum":180,
            "Millet":150,
        }
        max_height = base_height.get(crop,100)

        #growth progress (0 -> 1)
        progress = day/total_days

        #Logistic growth curve for realistic height progression
        growth_curve=  1 / (1 + np.exp(-10 * (progress - 0.5)))
        height = max_height * growth_curve

        #small natural randomness
        height += random.uniform(-3,3)

        return max(0, round(height,1))

    def _calculate_yield(self,health_score: float, crop:str)->float:
        """calculate estimated yield in tons per hectare"""
        base_yield={
            "Maize":3.5,
            "Beans":1.5,
            "Coffee":2.5,
            "Tea":2.5,
            "Cassava":15,
            "Sweet_Potato":10,
            "Banana":20,
            "Sorghum":2.0,
            "Millet":2.0,
        }

        base_yield = base_yield.get(crop,5.0)

        #Health score affects yield( 80% health = 80% of potential yield)
        return round(base_yield * (health_score/100),2)

    def _calculate_pest_risk(self,growth_stage:str,crop:str)->str:
        """Calculate pest/diseases risk based on growth stage and crop type"""
        pests = {
            "Maize": ["Fall Armyworm", "Maize Stem Borer", "Maize Streak Virus"],
            "Beans": ["Aphids", "Bean Fly", "Angular Leaf Spot"],
            "Coffee": ["Coffee Berry Borer", "Coffee Leaf Rust", "Antestia Bug"],
            "Tea": ["Tea Mosquito Bug", "Red Spider Mite", "Blister Blight"],
            "Cassava": ["Cassava Mosaic Virus", "Cassava Brown Streak", "Green Mite"],
            "Sweet_Potato": ["Weevil", "Alternaria Blight", "Sweet Potato Virus"],
            "Banana": ["Banana Weevil", "Sigatoka", "Panama Disease"],
            "Sorghum": ["Shoot Fly", "Stem Borer", "Grain Mold"],
            "Millet": ["Downy Mildew", "Head Miner", "Ergot"]
        }

        #Determine risk level based on growth stage
        if growth_stage in ['Flowering','Grain filling']:
            risk_level="High"
            if crop in pests:
                pest = random.choice(pests[crop])
                return f"{risk_level} risk of {pest}"
            return f"{risk_level} risk of pest infestation"

        elif growth_stage in ['Vegetative']:
            return "Moderate risk of pests"

        else:
            return "Low risk"
    def generate_daily_weather(self,location: str) -> Dict:

        """Generate daily weather data for a specific location"""
        weather_data = {}
        current_date=self.start_date

        location_data=self.LOCATIONS[location]

        while current_date <= self.end_date:
            day_of_year = current_date.timetuple().tm_yday

            #Seasonal patterns for East Africa
            if location_data['rainfall_pattern'] == "Bimodal":

                #long rains: March-May, Short rains: October-December
                if 60 <= day_of_year <= 150:
                    rain_factor=1.5
                elif 274 <= day_of_year <= 365:
                    rain_factor=1.3
                else:
                    rain_factor=0.4
            else:  #Unimodal
                if 120 <= day_of_year <= 270:
                    rain_factor=1.6
                else:
                    rain_factor=0.3

            #Temperature based on elevation
            base_temp=25-(location_data['elevation_m']/200)
            seasonal_variation = 3*(1-abs((day_of_year-180)/180))

            daily = {
                "date": current_date.strftime("%Y-%m-%d"),
                "location": location,
                "latitude": location_data['lat'],
                "longitude": location_data['lon'],
                "elevation_m": location_data['elevation_m'],
                "agro_zone": location_data['agro_zone'],

                #Weather parameters
                "temp_min_c":round(base_temp -5 + random.uniform(-2,2),1),
                "temp_max_c":round(base_temp +5 + random.uniform(-2,2),1),
                "temp_avg_c":round(base_temp + random.uniform(-3,3),1),
                "humidity_pct":random.randint(40,100),
                "precipitation_mm":round(max(0,random.gauss(4*rain_factor,3.0)),1),
                "evapotranspiration_mm":round(random.uniform(2,6),1),
                "solar_radiation_mj_m2":round(random.uniform(15,25),1),
                "wind_speed_kmh":round(random.uniform(5,20),1),

                #Soil moisture and conditions
                "soil_moisture_vol_pct":round(random.uniform(15,45),1),
                "soil_temp_c":round(base_temp - 2 + random.uniform(-3,3),1),

                #Agricultural Indicators
                'drought_risk':"low" if rain_factor > 0.8 else "Moderate" if rain_factor > 0.5 else "high",
                "flood_risk":"high" if rain_factor > 1.5 else "low",
                "frost_risk":"Yes" if base_temp < 5 else "No"
            }

            weather_data[current_date.strftime("%Y-%m-%d")]=daily
            current_date += timedelta(days=1)

        return weather_data



    def generate_crop_growth_data(self,location:str,crop_name: str,planting_date: str) -> Dict:

        """Generate daily crop growth monitoring data """

        start=datetime.strptime(planting_date, "%Y-%m-%d")
        crop= self.CROPS[crop_name]

        growth_data = {}

        for day in range(crop['growing_days']):
            current_date= start+ timedelta(days=day)
            growth_stage = self._get_growth_stage(day, crop['growing_days'])

            #Simulate growth parameters
            health_score = max(0, min(100, random.gauss(85.0,10.0)))

            daily_growth = {
                "location": location,
                'crop_name': crop_name,
                "planting_date": planting_date,
                "date": current_date.strftime("%Y-%m-%d"),
                "day_after_planting": day + 1,
                "growth_stage": growth_stage,
                "canopy_cover_pct": min(95, int(day/ crop['growing_days'] * 95 + random.uniform(-10,10))),
                "planting_height_cm": round(self._calculate_height(day ,crop['growing_days'],crop_name),2),
                "health_score": health_score,
                "stress_level":"Normal" if health_score > 70 else "Mild" if health_score > 40 else "Severe",
                "estimated_yield_tons_ha":round(self._calculate_yield(health_score,crop_name),2),
                "pest_risk":self._calculate_pest_risk(growth_stage,crop_name),
                "irrigation_needed":day % 7 == 0 # Every 7 Days


            }
            growth_data[current_date.strftime("%Y-%m-%d")] = daily_growth


        return growth_data

