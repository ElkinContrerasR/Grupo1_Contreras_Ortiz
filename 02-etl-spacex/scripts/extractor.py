#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class SpaceX:
    def __init__(self):
        self.base_url=os.getenv("SPACEX_BASE_URL")
        self.endpoints = os.getenv("SPACEX_ENDPOINTS").split(",")

        if not self.base_url:
            logging.critical("SPACEX_BASE_URL no está definida")
            raise ValueError("SPACEX_BASE_URL no está definida")

    def extract_info(self, endpoint):
        try:
            logging.info(f"Realizando petición a {self.base_url}")
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            logging.info("Petición exitosa")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error en la petición: {e}")
            raise

    def transform_launches(self, raw_data):
        transformed = []

        for item in raw_data:
            try:
                rocket = item.get("rocket", {})
                first_stage = rocket.get("first_stage", {})
                cores = first_stage.get("cores", [])

                second_stage = rocket.get("second_stage", {})
                payloads = second_stage.get("payloads", [])

                launch_site = item.get("launch_site", {})

                # Fecha
                launch_date = item.get("launch_date_utc")
                if launch_date:
                    launch_date = datetime.fromisoformat(
                        launch_date.replace("Z", "+00:00")
                    )

                # Primer payload (si existe)
                if payloads:
                    payload = payloads[0]
                else:
                    payload = {}

                transformed.append({
                    "flight_number": item.get("flight_number"),
                    "mission_name": item.get("mission_name"),
                    "launch_year": int(item.get("launch_year")) if item.get("launch_year") else None,
                    "launch_date": launch_date,
                    "launch_success": item.get("launch_success"),
                    "upcoming": item.get("upcoming"),

                    # Rocket
                    "rocket_id": rocket.get("rocket_id"),
                    "rocket_name": rocket.get("rocket_name"),
                    "rocket_type": rocket.get("rocket_type"),

                    # Payload
                    "payload_id": payload.get("payload_id"),
                    "payload_type": payload.get("payload_type"),
                    "payload_mass_kg": payload.get("payload_mass_kg"),
                    "orbit": payload.get("orbit"),
                    "num_payloads": len(payloads),

                    # Core
                    "cores_reused": any(c.get("reused") for c in cores) if cores else False,

                    # Launch site
                    "site_id": launch_site.get("site_id"),
                    "site_name": launch_site.get("site_name"),

                    "details": item.get("details")
                })

            except Exception as e:
                logging.error(f"Error transformando launch: {e}")
                continue

        return transformed

    def transform_rockets(self, raw_data):
        transformed = []

        for item in raw_data:
            try:
                height = item.get("height", {})
                diameter = item.get("diameter", {})
                mass = item.get("mass", {})
                engines = item.get("engines", {})
                payload_weights = item.get("payload_weights", [])

                # Payload LEO
                leo_payload = next(
                    (p for p in payload_weights if p.get("id") == "leo"),
                    {}
                )

                transformed.append({
                    "rocket_id": item.get("rocket_id"),
                    "rocket_name": item.get("rocket_name"),
                    "rocket_type": item.get("rocket_type"),
                    "active": item.get("active"),

                    "stages": item.get("stages"),
                    "boosters": item.get("boosters"),
                    "cost_per_launch": item.get("cost_per_launch"),
                    "success_rate_pct": item.get("success_rate_pct"),
                    "country": item.get("country"),
                    "company": item.get("company"),

                    # Dimensiones
                    "height_m": height.get("meters"),
                    "diameter_m": diameter.get("meters"),
                    "mass_kg": mass.get("kg"),

                    # Motores
                    "engine_number": engines.get("number"),
                    "engine_type": engines.get("type"),
                    "engine_version": engines.get("version"),
                    "propellant_1": engines.get("propellant_1"),
                    "propellant_2": engines.get("propellant_2"),
                    "thrust_to_weight": engines.get("thrust_to_weight"),

                    # Performance
                    "first_flight": item.get("first_flight"),
                    "payload_leo_kg": leo_payload.get("kg")
                })

            except Exception as e:
                logging.error(f"Error transformando rocket: {e}")
                continue

        return transformed

    def execute_extract(self):
        data = {}

        for endpoint in self.endpoints:
            logger.info(f"Extrayendo {endpoint}")

            raw = self.extract_info(endpoint)

            if endpoint == "launches":
                clean = self.transform_launches(raw)

            elif endpoint == "rockets":
                clean = self.transform_rockets(raw)

            else:
                clean = raw  # fallback

            data[endpoint] = clean

        return data

if __name__ == "__main__":
    try:
        extractor = SpaceX()
        datos = extractor.execute_extract()
        
        for endpoint, records in datos.items():
            # JSON
            with open(f"data/{endpoint}.json", "w") as f:
                json.dump(records, f, indent=2, default=str)

            # CSV
            df = pd.DataFrame(records)
            df.to_csv(f"data/{endpoint}.csv", index=False)

        print("\n" + "="*50)
        print("RESUMEN DE EXTRACCIÓN")
        print("="*50)
        for endpoint, records in datos.items():
            df = pd.DataFrame(records)
            print(f"\nResumen de {endpoint}")
            print(df.head())
        print("="*50)
        
    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}")
