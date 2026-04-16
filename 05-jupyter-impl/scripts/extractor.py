#!/usr/bin/env python3
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import logging
from database import SessionLocal
from models import *
from sqlalchemy.exc import IntegrityError

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
        self.db = SessionLocal()
        self.registros_extraidos = 0
        self.registros_guardados = 0
        self.registros_fallidos = 0

        if not self.base_url:
            logging.critical("SPACEX_BASE_URL no está definida")
            raise ValueError("SPACEX_BASE_URL no está definida")

    def guardar_tabla(self, modelo, datos):

        try:

            for row in datos:
                obj = modelo(**row)
                self.db.add(obj)

            self.db.commit()

            self.registros_guardados += len(datos)

            logger.info(f"{len(datos)} registros guardados en {modelo.__tablename__}")

        except IntegrityError as e:

            self.db.rollback()
            logger.error(f"Error de integridad en {modelo.__tablename__}: {e}")
            self.registros_fallidos += len(datos)

        except Exception as e:

            self.db.rollback()
            logger.error(f"Error guardando {modelo.__tablename__}: {e}")
            self.registros_fallidos += len(datos)

        
    def cargar_en_bd(self, data):
        logger.info("Iniciando limpieza de tablas")
    
        # Borrar en orden inverso para respetar las FK
        self.db.query(LaunchesCentral).delete()
        self.db.query(RocketPayloadWeight).delete()
        self.db.query(LaunchCapsule).delete()
        self.db.query(LaunchShip).delete()
        self.db.query(LaunchCore).delete()
        self.db.query(LaunchPayload).delete()
        self.db.query(Launch).delete()
        self.db.query(Capsule).delete()
        self.db.query(Ship).delete()
        self.db.query(Landpad).delete()
        self.db.query(Launchpad).delete()
        self.db.query(Core).delete()
        self.db.query(Payload).delete()
        self.db.query(Rocket).delete()
        self.db.commit()

        logger.info("Iniciando carga en PostgreSQL")

        self.guardar_tabla(Rocket, data.get("rockets", []))

        self.guardar_tabla(Payload, data.get("payloads", []))

        self.guardar_tabla(Core, data.get("cores", []))

        self.guardar_tabla(Launchpad, data.get("launchpads", []))

        self.guardar_tabla(Landpad, data.get("landpads", []))

        self.guardar_tabla(Ship, data.get("ships", []))

        self.guardar_tabla(Capsule, data.get("capsules", []))

        self.guardar_tabla(Launch, data.get("launches", []))

        self.guardar_tabla(LaunchPayload, data.get("launch_payloads", []))

        self.guardar_tabla(LaunchCore, data.get("launch_cores", []))

        self.guardar_tabla(LaunchShip, data.get("launch_ships", []))

        self.guardar_tabla(LaunchCapsule, data.get("launch_capsules", []))

        self.guardar_tabla(RocketPayloadWeight, data.get("rocket_payload_weights", []))

        self.guardar_tabla(LaunchesCentral, data.get("launches_central", []))
    
    def guardar_metricas(self, estado):

        try:

            metricas = MetricasETL(
                registros_extraidos=self.registros_extraidos,
                registros_guardados=self.registros_guardados,
                registros_fallidos=self.registros_fallidos,
                estado=estado
            )

            self.db.add(metricas)
            self.db.commit()

        except Exception as e:

            logger.error(f"Error guardando métricas: {e}")

    def extract_info(self, endpoint):
        try:
            
            url = f"{self.base_url}/{endpoint}"
            logging.info(f"Realizando petición a {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            logging.info("Petición exitosa")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error en la petición: {e}")
            raise
    def build_launches_central(self, data):
        launches = {l["launch_id"]: l for l in data.get("launches", [])}
        rockets = {r["rocket_id"]: r for r in data.get("rockets", [])}

        # Agrupar payloads por launch
        payload_map = {}  # launch_id -> lista de payload dicts
        payload_lookup = {p["payload_id"]: p for p in data.get("payloads", [])}
        for lp in data.get("launch_payloads", []):
            lid = lp["launch_id"]
            pid = lp["payload_id"]
            payload_map.setdefault(lid, []).append(payload_lookup.get(pid, {}))

        # Agrupar cores por launch
        core_map = {}  # launch_id -> lista de launch_core dicts
        for lc in data.get("launch_cores", []):
            core_map.setdefault(lc["launch_id"], []).append(lc)

        launches_central = []
        for launch_id, launch in launches.items():
            rocket = rockets.get(launch.get("rocket_id"), {})

            payloads = payload_map.get(launch_id, [])
            payload_count = len(payloads)
            total_payload_mass = sum(
                p.get("mass_kg") or 0 for p in payloads
            )

            cores = core_map.get(launch_id, [])
            core_count = len(cores)
            reused_cores = sum(1 for c in cores if c.get("reused"))

            launches_central.append({
                "launch_id": launch_id,
                "flight_number": launch.get("flight_number"),
                "rocket_name": rocket.get("name"),
                "success": launch.get("success"),
                "payload_count": payload_count,
                "total_payload_mass": total_payload_mass,
                "core_count": core_count,
                "reused_cores": reused_cores,
            })

        return launches_central

    def transform_launches(self, raw_data):
        launches = []
        launch_payload_rel = []
        launch_core_rel = []
        launch_ship_rel = []    # Nueva relación
        launch_capsule_rel = [] # Nueva relación

        for item in raw_data:
            try:
                launch_id = item.get("id") 

                # Transformación básica del lanzamiento
                launches.append({
                    "launch_id": launch_id,
                    "flight_number": item.get("flight_number"), 
                    "name": item.get("name"), 
                    "date_utc": datetime.fromisoformat(
                        item.get("date_utc").replace("Z", "+00:00")
                        ) if item.get("date_utc") else None, 
                    "success": item.get("success"), 
                    "upcoming": item.get("upcoming"), 
                    "rocket_id": item.get("rocket"), 
                    "launchpad_id": item.get("launchpad"), 
                    "details": item.get("details") 
                })

                # Relación Launch-Payload (N:M)
                for payload_id in item.get("payloads", []): 
                    launch_payload_rel.append({
                        "launch_id": launch_id,
                        "payload_id": payload_id
                    })

                # Relación Launch-Core (N:M) + Landpad
                for core in item.get("cores", []):
                    core_id = core.get("core")
                    if core_id is None:  # Ignorar cores sin ID
                        continue
                    launch_core_rel.append({
                        "launch_id": launch_id,
                        "core_id": core.get("core"), 
                        "landing_success": core.get("landing_success"), 
                        "landing_type": core.get("landing_type"), 
                        "landpad_id": core.get("landpad"), # Corregido: captura el ID del landpad 
                        "reused": core.get("reused") 
                    })

                # Relación Launch-Ship (N:M) - NUEVO
                for ship_id in item.get("ships", []): 
                    launch_ship_rel.append({
                        "launch_id": launch_id,
                        "ship_id": ship_id
                    })

                # Relación Launch-Capsule (N:M) - NUEVO
                for capsule_id in item.get("capsules", []): 
                    launch_capsule_rel.append({
                        "launch_id": launch_id,
                        "capsule_id": capsule_id
                    })

            except Exception as e:
                logging.error(f"Error transformando launch: {e}")
                continue

        return launches, launch_payload_rel, launch_core_rel, launch_ship_rel, launch_capsule_rel

    def transform_rockets(self, raw_data):
        rockets = []
        rocket_payload_rel = []

        for item in raw_data:
            try:
                rocket_id = item.get("id")
                
                # Datos técnicos principales (Dimensiones y Motores)
                rockets.append({
                    "rocket_id": rocket_id,
                    "name": item.get("name"),
                    "type": item.get("type"),
                    "active": item.get("active"),
                    "stages": item.get("stages"),
                    "boosters": item.get("boosters"),
                    "cost_per_launch": item.get("cost_per_launch"),
                    "success_rate_pct": item.get("success_rate_pct"),
                    "first_flight": item.get("first_flight"),
                    "country": item.get("country"),
                    "company": item.get("company"),
                    # Dimensiones [cite: 5, 6]
                    "height_m": item.get("height", {}).get("meters"),
                    "diameter_m": item.get("diameter", {}).get("meters"),
                    "mass_kg": item.get("mass", {}).get("kg"),
                    # Motores y Propulsión [cite: 10, 11]
                    "engine_type": item.get("engines", {}).get("type"),
                    "engine_version": item.get("engines", {}).get("version"),
                    "engine_loss_max": item.get("engines", {}).get("engine_loss_max"),
                    "propellant_1": item.get("engines", {}).get("propellant_1"),
                    "propellant_2": item.get("engines", {}).get("propellant_2"),
                    "thrust_to_weight": item.get("engines", {}).get("thrust_to_weight"),
                    "isp_sea_level": item.get("engines", {}).get("isp", {}).get("sea_level"),
                    "isp_vacuum": item.get("engines", {}).get("isp", {}).get("vacuum")
                })

                # Tabla puente para capacidades de carga por órbita 
                for weight in item.get("payload_weights", []):
                    rocket_payload_rel.append({
                        "rocket_id": rocket_id,
                        "orbit_id": weight.get("id"),
                        "orbit_name": weight.get("name"),
                        "kg": weight.get("kg"),
                        "lb": weight.get("lb")
                    })

            except Exception as e:
                logging.error(f"Error transformando rocket: {e}")

        return rockets, rocket_payload_rel
    
    def transform_payloads(self, raw_data):
        payloads = []

        for item in raw_data:
            payloads.append({
                "payload_id": item.get("id"),
                "name": item.get("name"),
                "type": item.get("type"),
                "mass_kg": item.get("mass_kg"),
                "orbit": item.get("orbit"),
                "customers": ",".join(item.get("customers", [])),
                "nationalities": ",".join(item.get("nationalities", []))
            })

        return payloads

    def transform_cores(self, raw_data):
        cores = []

        for item in raw_data:
            cores.append({
                "core_id": item.get("id"),
                "serial": item.get("serial"),
                "reuse_count": item.get("reuse_count"),
                "status": item.get("status"),
                "rtls_landings": item.get("rtls_landings"),
                "asds_landings": item.get("asds_landings")
            })

        return cores
    def transform_launchpads(self, raw_data):
        launchpads = []

        for item in raw_data:
            launchpads.append({
                "launchpad_id": item.get("id"),
                "name": item.get("name"),
                "full_name": item.get("full_name"),
                "locality": item.get("locality"),
                "region": item.get("region"),
                "latitude": item.get("latitude"),
                "longitude": item.get("longitude"),
                "launch_attempts": item.get("launch_attempts"),
                "launch_successes": item.get("launch_successes"),
                "status": item.get("status")
            })

        return launchpads
    
    def transform_landpads(self, raw_data):
        landpads = []

        for item in raw_data:
            landpads.append({
                "landpad_id": item.get("id"),
                "name": item.get("name"),
                "full_name": item.get("full_name"),
                "type": item.get("type"),
                "locality": item.get("locality"),
                "region": item.get("region"),
                "latitude": item.get("latitude"),
                "longitude": item.get("longitude"),
                "landing_attempts": item.get("landing_attempts"),
                "landing_successes": item.get("landing_successes"),
                "status": item.get("status")
            })

        return landpads
    
    def transform_ships(self, raw_data):
        ships = []

        for item in raw_data:
            ships.append({
                "ship_id": item.get("id"),
                "name": item.get("name"),
                "type": item.get("type"),
                "model": item.get("model"),
                "year_built": item.get("year_built"),
                "home_port": item.get("home_port"),
                "mass_kg": item.get("mass_kg"),
                "active": item.get("active")
            })

        return ships
    
    def transform_capsules(self, raw_data):
        capsules = []

        for item in raw_data:
            capsules.append({
                "capsule_id": item.get("id"),
                "serial": item.get("serial"),
                "type": item.get("type"),
                "status": item.get("status"),
                "reuse_count": item.get("reuse_count"),
                "water_landings": item.get("water_landings"),
                "land_landings": item.get("land_landings")
            })

        return capsules

    def execute_extract(self):
        data = {}

        for endpoint in self.endpoints:
            logger.info(f"Extrayendo {endpoint}")
            raw = self.extract_info(endpoint)

            if endpoint == "launches":
                launches, lp_rel, lc_rel, ls_rel, lcap_rel = self.transform_launches(raw)
                data["launches"] = launches
                data["launch_payloads"] = lp_rel
                data["launch_cores"] = lc_rel
                data["launch_ships"] = ls_rel    # Guardar nueva tabla
                data["launch_capsules"] = lcap_rel # Guardar nueva tabla

            elif endpoint == "rockets":
                rockets, rp_weights = self.transform_rockets(raw)
                data["rockets"] = rockets
                data["rocket_payload_weights"] = rp_weights

            elif endpoint == "payloads":
                data["payloads"] = self.transform_payloads(raw)

            elif endpoint == "cores":
                data["cores"] = self.transform_cores(raw)
            
            elif endpoint == "launchpads":
                data["launchpads"] = self.transform_launchpads(raw)

            elif endpoint == "landpads":
                data["landpads"] = self.transform_landpads(raw)

            elif endpoint == "ships":
                data["ships"] = self.transform_ships(raw)

            elif endpoint == "capsules":
                data["capsules"] = self.transform_capsules(raw)

            else:
                data[endpoint] = raw

        return data

    def ejecutar(self):

        try:

            logger.info("Iniciando ETL SpaceX")

            data = self.execute_extract()
            
            data["launches_central"] = self.build_launches_central(data)

            self.registros_extraidos = sum(len(v) for v in data.values())

            self.cargar_en_bd(data)

            estado = "SUCCESS" if self.registros_fallidos == 0 else "PARTIAL"

            self.guardar_metricas(estado)

            return True

        except Exception as e:

            logger.error(f"Error en ETL: {e}")

            self.guardar_metricas("FAILED")

            return False

        finally:

            self.db.close()

if __name__ == "__main__":

    try:

        etl = SpaceX()

        exito = etl.ejecutar()

        if exito:
            print("ETL ejecutado correctamente")
        else:
            print("ETL terminó con errores")

        exit(0 if exito else 1)

    except Exception as e:

        logger.error(f"Error en pipeline: {e}")