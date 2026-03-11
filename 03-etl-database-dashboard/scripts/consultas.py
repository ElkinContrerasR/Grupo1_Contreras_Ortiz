#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import (
    Launch, Rocket, Payload, LaunchPayload,
    LaunchCore, Core, Launchpad, MetricasETL,
    LaunchesCentral
)

from sqlalchemy import func, Integer
import pandas as pd

db = SessionLocal()


def lanzamientos_por_cohete():
    """Cantidad de lanzamientos por cohete"""
    registros = db.query(
        Rocket.name,
        func.count(Launch.launch_id).label("total_launches")
    ).join(Launch, Launch.rocket_id == Rocket.rocket_id)\
     .group_by(Rocket.name)\
     .order_by(func.count(Launch.launch_id).desc())\
     .all()

    df = pd.DataFrame(registros, columns=["Cohete", "Total Lanzamientos"])

    print("\n🚀 LANZAMIENTOS POR COHETE:")
    print(df.to_string(index=False))


def tasa_exito_por_cohete():
    """Tasa de éxito de lanzamientos por cohete"""
    registros = db.query(
        Rocket.name,
        func.avg(func.cast(Launch.success, Integer)).label("success_rate")
    ).join(Launch)\
     .group_by(Rocket.name)\
     .all()

    df = pd.DataFrame(registros, columns=["Cohete", "Tasa Éxito"])

    print("\n📊 TASA DE ÉXITO POR COHETE:")
    print(df.to_string(index=False))


def payload_total_por_orbita():
    """Masa total de payload enviada por órbita"""
    registros = db.query(
        Payload.orbit,
        func.sum(Payload.mass_kg).label("masa_total")
    ).group_by(Payload.orbit)\
     .order_by(func.sum(Payload.mass_kg).desc())\
     .all()

    df = pd.DataFrame(registros, columns=["Órbita", "Masa Total (kg)"])

    print("\n🛰️ MASA TOTAL ENVIADA POR ÓRBITA:")
    print(df.to_string(index=False))


def launchpads_mas_usados():
    """Launchpads con más lanzamientos"""
    registros = db.query(
        Launchpad.name,
        func.count(Launch.launch_id).label("total_launches")
    ).join(Launch)\
     .group_by(Launchpad.name)\
     .order_by(func.count(Launch.launch_id).desc())\
     .limit(5)\
     .all()

    df = pd.DataFrame(registros, columns=["Launchpad", "Lanzamientos"])

    print("\n📍 TOP 5 LAUNCHPADS MÁS USADOS:")
    print(df.to_string(index=False))


def nucleos_mas_reutilizados():
    """Cores con mayor reutilización"""
    registros = db.query(
        Core.serial,
        Core.reuse_count
    ).order_by(Core.reuse_count.desc())\
     .limit(5)\
     .all()

    df = pd.DataFrame(registros, columns=["Core", "Reutilizaciones"])

    print("\n♻️ CORES MÁS REUTILIZADOS:")
    print(df.to_string(index=False))


def resumen_launches_central():
    """Resumen usando tabla analítica"""
    registros = db.query(
        LaunchesCentral.rocket_name,
        func.count(LaunchesCentral.launch_id).label("total"),
        func.sum(LaunchesCentral.payload_count).label("payloads"),
        func.sum(LaunchesCentral.total_payload_mass).label("masa_total")
    ).group_by(LaunchesCentral.rocket_name)\
     .order_by(func.count(LaunchesCentral.launch_id).desc())\
     .all()

    df = pd.DataFrame(
        registros,
        columns=["Cohete", "Lanzamientos", "Payloads", "Masa Total"]
    )

    print("\n📊 RESUMEN ANALÍTICO DE LANZAMIENTOS:")
    print(df.to_string(index=False))


def metricas_etl():
    """Últimas ejecuciones del ETL"""
    metricas = db.query(MetricasETL)\
        .order_by(MetricasETL.fecha.desc())\
        .limit(5)\
        .all()

    print("\n📈 ÚLTIMAS EJECUCIONES DEL ETL:")

    for m in metricas:
        print(
            f"- {m.fecha}: {m.estado} "
            f"({m.registros_guardados} registros guardados)"
        )


if __name__ == "__main__":

    try:
        print("\n" + "="*50)
        print("ANÁLISIS DE DATOS SPACEX - POSTGRESQL")
        print("="*50)

        lanzamientos_por_cohete()
        tasa_exito_por_cohete()
        payload_total_por_orbita()
        launchpads_mas_usados()
        nucleos_mas_reutilizados()
        resumen_launches_central()
        metricas_etl()

        print("\n" + "="*50 + "\n")

    finally:
        db.close()