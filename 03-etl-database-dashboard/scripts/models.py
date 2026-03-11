#!/usr/bin/env python3
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean,Text, ForeignKey
from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import relationship
from scripts.database import Base
from sqlalchemy.orm import Mapped, mapped_column
 

class Launch(Base):
    __tablename__ = "launches"

    launch_id: Mapped[str] = mapped_column(String, primary_key=True)

    flight_number: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)

    date_utc: Mapped[datetime] = mapped_column(DateTime)

    success: Mapped[bool] = mapped_column(Boolean, nullable=True)
    upcoming: Mapped[bool] = mapped_column(Boolean)

    rocket_id: Mapped[str] = mapped_column(
        ForeignKey("rockets.rocket_id")
    )

    launchpad_id: Mapped[str] = mapped_column(
        ForeignKey("launchpads.launchpad_id")
    )

    details: Mapped[str] = mapped_column(Text, nullable=True)



class Rocket(Base):
    __tablename__ = "rockets"

    rocket_id: Mapped[str] = mapped_column(String, primary_key=True)

    name: Mapped[str] = mapped_column(String)
    type: Mapped[str] = mapped_column(String)

    active: Mapped[bool] = mapped_column(Boolean)

    stages: Mapped[int] = mapped_column(Integer)
    boosters: Mapped[int] = mapped_column(Integer)

    cost_per_launch: Mapped[int] = mapped_column(Integer)
    success_rate_pct: Mapped[int] = mapped_column(Integer)

    first_flight: Mapped[str] = mapped_column(String)

    country: Mapped[str] = mapped_column(String)
    company: Mapped[str] = mapped_column(String)

    height_m: Mapped[float] = mapped_column(Float)
    diameter_m: Mapped[float] = mapped_column(Float)
    mass_kg: Mapped[int] = mapped_column(Integer)

    engine_type: Mapped[str] = mapped_column(String)
    engine_version: Mapped[str] = mapped_column(String)

    engine_loss_max: Mapped[int] = mapped_column(Integer, nullable = True)

    propellant_1: Mapped[str] = mapped_column(String)
    propellant_2: Mapped[str] = mapped_column(String)

    thrust_to_weight: Mapped[float] = mapped_column(Float)

    isp_sea_level: Mapped[int] = mapped_column(Integer)
    isp_vacuum: Mapped[int] = mapped_column(Integer)

class Payload(Base):
    __tablename__ = "payloads"

    payload_id: Mapped[str] = mapped_column(String, primary_key=True)

    name: Mapped[str] = mapped_column(String)

    type: Mapped[str] = mapped_column(String)

    mass_kg: Mapped[int] = mapped_column(Integer, nullable=True)

    orbit: Mapped[str] = mapped_column(String, nullable=True)

    customers: Mapped[str] = mapped_column(String)

    nationalities: Mapped[str] = mapped_column(String)
class Core(Base):
    __tablename__ = "cores"

    core_id: Mapped[str] = mapped_column(String, primary_key=True)

    serial: Mapped[str] = mapped_column(String)

    reuse_count: Mapped[int] = mapped_column(Integer)

    status: Mapped[str] = mapped_column(String)

    rtls_landings: Mapped[int] = mapped_column(Integer)

    asds_landings: Mapped[int] = mapped_column(Integer)

class Launchpad(Base):
    __tablename__ = "launchpads"

    launchpad_id: Mapped[str] = mapped_column(String, primary_key=True)

    name: Mapped[str] = mapped_column(String)

    full_name: Mapped[str] = mapped_column(String)

    locality: Mapped[str] = mapped_column(String)

    region: Mapped[str] = mapped_column(String)

    latitude: Mapped[float] = mapped_column(Float)

    longitude: Mapped[float] = mapped_column(Float)

    launch_attempts: Mapped[int] = mapped_column(Integer)

    launch_successes: Mapped[int] = mapped_column(Integer)

    status: Mapped[str] = mapped_column(String)

class Landpad(Base):
    __tablename__ = "landpads"

    landpad_id: Mapped[str] = mapped_column(String, primary_key=True)

    name: Mapped[str] = mapped_column(String)

    full_name: Mapped[str] = mapped_column(String)

    type: Mapped[str] = mapped_column(String)

    locality: Mapped[str] = mapped_column(String)

    region: Mapped[str] = mapped_column(String)

    latitude: Mapped[float] = mapped_column(Float)

    longitude: Mapped[float] = mapped_column(Float)

    landing_attempts: Mapped[int] = mapped_column(Integer)

    landing_successes: Mapped[int] = mapped_column(Integer)

    status: Mapped[str] = mapped_column(String)

class Ship(Base):
    __tablename__ = "ships"

    ship_id: Mapped[str] = mapped_column(String, primary_key=True)

    name: Mapped[str] = mapped_column(String)

    type: Mapped[str] = mapped_column(String)

    model: Mapped[str] = mapped_column(String, nullable=True)

    year_built: Mapped[int] = mapped_column(Integer, nullable=True)

    home_port: Mapped[str] = mapped_column(String)

    mass_kg: Mapped[int] = mapped_column(Integer, nullable=True)

    active: Mapped[bool] = mapped_column(Boolean)

class Capsule(Base):
    __tablename__ = "capsules"

    capsule_id: Mapped[str] = mapped_column(String, primary_key=True)

    serial: Mapped[str] = mapped_column(String)

    type: Mapped[str] = mapped_column(String)

    status: Mapped[str] = mapped_column(String)

    reuse_count: Mapped[int] = mapped_column(Integer)

    water_landings: Mapped[int] = mapped_column(Integer)

    land_landings: Mapped[int] = mapped_column(Integer)

class LaunchPayload(Base):
    __tablename__ = "launch_payloads"

    launch_id: Mapped[str] = mapped_column(
        ForeignKey("launches.launch_id"),
        primary_key=True
    )

    payload_id: Mapped[str] = mapped_column(
        ForeignKey("payloads.payload_id"),
        primary_key=True
    )

class LaunchCore(Base):
    __tablename__ = "launch_cores"

    launch_id: Mapped[str] = mapped_column(
        ForeignKey("launches.launch_id"),
        primary_key=True
    )

    core_id: Mapped[str] = mapped_column(
        ForeignKey("cores.core_id"),
        primary_key=True
    )

    landing_success: Mapped[bool] = mapped_column(Boolean, nullable=True)

    landing_type: Mapped[str] = mapped_column(String, nullable=True)

    landpad_id: Mapped[str] = mapped_column(
        ForeignKey("landpads.landpad_id"),
        nullable=True
    )

    reused: Mapped[bool] = mapped_column(Boolean)

class LaunchShip(Base):
    __tablename__ = "launch_ships"

    launch_id: Mapped[str] = mapped_column(
        ForeignKey("launches.launch_id"),
        primary_key=True
    )

    ship_id: Mapped[str] = mapped_column(
        ForeignKey("ships.ship_id"),
        primary_key=True
    )

class LaunchCapsule(Base):
    __tablename__ = "launch_capsules"

    launch_id: Mapped[str] = mapped_column(
        ForeignKey("launches.launch_id"),
        primary_key=True
    )

    capsule_id: Mapped[str] = mapped_column(
        ForeignKey("capsules.capsule_id"),
        primary_key=True
    )

class RocketPayloadWeight(Base):
    __tablename__ = "rocket_payload_weights"

    rocket_id: Mapped[str] = mapped_column(
        ForeignKey("rockets.rocket_id"),
        primary_key=True
    )

    orbit_id: Mapped[str] = mapped_column(String, primary_key=True)

    orbit_name: Mapped[str] = mapped_column(String)

    kg: Mapped[int] = mapped_column(Integer)

    lb: Mapped[int] = mapped_column(Integer)




class LaunchesCentral(Base):
    __tablename__ = "launches_central"

    launch_id: Mapped[str] = mapped_column(
        ForeignKey("launches.launch_id"),
        primary_key=True
    )

    flight_number: Mapped[int] = mapped_column(Integer)

    rocket_name: Mapped[str] = mapped_column(String)

    success: Mapped[bool] = mapped_column(Boolean, nullable=True)

    payload_count: Mapped[int] = mapped_column(Integer)

    total_payload_mass: Mapped[float] = mapped_column(Float)

    core_count: Mapped[int] = mapped_column(Integer)

    reused_cores: Mapped[int] = mapped_column(Integer)

class MetricasETL(Base):
    __tablename__ = "metricas_etl"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fecha: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    registros_extraidos: Mapped[int] = mapped_column(Integer)
    registros_guardados: Mapped[int] = mapped_column(Integer)
    registros_fallidos: Mapped[int] = mapped_column(Integer)
    estado: Mapped[str] = mapped_column(String)