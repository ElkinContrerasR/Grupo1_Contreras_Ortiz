import numpy as np
import random
from datetime import datetime, timedelta

np.random.seed(42)

def generate_ids(prefix, n):
    return [f"{prefix}_{i}" for i in range(n)]

def generate_synthetic_data(n_launches=300):

    # =========================
    # ROCKETS (BASE CONTROLADA)
    # =========================
    rockets = [
        {
            "rocket_id": "falcon1",
            "name": "Falcon 1",
            "type": "rocket",
            "active": False,
            "stages": 2,
            "boosters": 0,
            "cost_per_launch": 7000000,
            "success_rate_pct": 40,
            "first_flight": "2006-03-24",
            "country": "USA",
            "company": "SpaceX",
            "height_m": 22,
            "diameter_m": 1.7,
            "mass_kg": 30000,
            "engine_type": "merlin",
            "engine_version": "1A",
            "engine_loss_max": None,
            "propellant_1": "LOX",
            "propellant_2": "RP-1",
            "thrust_to_weight": 90,
            "isp_sea_level": 267,
            "isp_vacuum": 304
        },
        {
            "rocket_id": "falcon9",
            "name": "Falcon 9",
            "type": "rocket",
            "active": True,
            "stages": 2,
            "boosters": 0,
            "cost_per_launch": 50000000,
            "success_rate_pct": 98,
            "first_flight": "2010-06-04",
            "country": "USA",
            "company": "SpaceX",
            "height_m": 70,
            "diameter_m": 3.7,
            "mass_kg": 550000,
            "engine_type": "merlin",
            "engine_version": "1D",
            "engine_loss_max": None,
            "propellant_1": "LOX",
            "propellant_2": "RP-1",
            "thrust_to_weight": 180,
            "isp_sea_level": 282,
            "isp_vacuum": 311
        },
        {
            "rocket_id": "falconheavy",
            "name": "Falcon Heavy",
            "type": "rocket",
            "active": True,
            "stages": 2,
            "boosters": 2,
            "cost_per_launch": 90000000,
            "success_rate_pct": 95,
            "first_flight": "2018-02-06",
            "country": "USA",
            "company": "SpaceX",
            "height_m": 70,
            "diameter_m": 3.7,
            "mass_kg": 1400000,
            "engine_type": "merlin",
            "engine_version": "1D",
            "engine_loss_max": None,
            "propellant_1": "LOX",
            "propellant_2": "RP-1",
            "thrust_to_weight": 220,
            "isp_sea_level": 282,
            "isp_vacuum": 311
        }
    ]

    # =========================
    # OTRAS ENTIDADES
    # =========================
    launchpads = [{
        "launchpad_id": "lp1",
        "name": "KSC",
        "full_name": "Kennedy Space Center",
        "locality": "Florida",
        "region": "USA",
        "latitude": 28.5,
        "longitude": -80.6,
        "launch_attempts": 100,
        "launch_successes": 95,
        "status": "active"
    }]

    landpads = [{
        "landpad_id": "land1",
        "name": "Drone Ship",
        "full_name": "Autonomous Spaceport",
        "type": "ASDS",
        "locality": "Ocean",
        "region": "USA",
        "latitude": 0.0,
        "longitude": 0.0,
        "landing_attempts": 100,
        "landing_successes": 85,
        "status": "active"
    }]

    cores = []
    payloads = []
    launches = []
    launch_payloads = []
    launch_cores = []

    # =========================
    # GENERACIÓN PRINCIPAL
    # =========================
    for i in range(n_launches):

        rocket = random.choice(rockets)
        launch_id = f"launch_{i}"
        payload_id = f"payload_{i}"
        core_id = f"core_{i}"

        reused = np.random.choice([0, 1], p=[0.4, 0.6])

        # 🔥 VARIABLE OBJETIVO BIEN DEFINIDA
        payload_mass = (
            0.00008 * rocket["mass_kg"] +
            80 * rocket["thrust_to_weight"] +
            1200 * reused -
            0.000001 * rocket["cost_per_launch"] +
            np.random.normal(0, 300)
        )

        payload_mass = max(0, payload_mass)

        payloads.append({
            "payload_id": payload_id,
            "name": f"Payload {i}",
            "type": "satellite",
            "mass_kg": int(payload_mass),
            "orbit": "LEO",
            "customers": "NASA",
            "nationalities": "USA"
        })

        cores.append({
            "core_id": core_id,
            "serial": f"B{i}",
            "reuse_count": reused,
            "status": "active",
            "rtls_landings": reused,
            "asds_landings": reused
        })

        launches.append({
            "launch_id": launch_id,
            "flight_number": i,
            "name": f"Mission {i}",
            "date_utc": datetime.utcnow() - timedelta(days=i),
            "success": True,
            "upcoming": False,
            "rocket_id": rocket["rocket_id"],
            "launchpad_id": "lp1",
            "details": "Synthetic mission"
        })

        launch_payloads.append({
            "launch_id": launch_id,
            "payload_id": payload_id
        })

        launch_cores.append({
            "launch_id": launch_id,
            "core_id": core_id,
            "landing_success": True,
            "landing_type": "ASDS",
            "landpad_id": "land1",
            "reused": bool(reused)
        })

    # =========================
    # OUTPUT FINAL (CLAVE)
    # =========================
    return {
        "rockets": rockets,
        "payloads": payloads,
        "cores": cores,
        "launchpads": launchpads,
        "landpads": landpads,
        "ships": [],
        "capsules": [],
        "launches": launches,
        "launch_payloads": launch_payloads,
        "launch_cores": launch_cores,
        "launch_ships": [],
        "launch_capsules": [],
        "rocket_payload_weights": []
    }