#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import func
from datetime import datetime

from scripts.database import SessionLocal
from scripts.models import Launch, LaunchesCentral, MetricasETL

st.set_page_config(
    page_title="SpaceX Dashboard",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 SpaceX Launch Analytics Dashboard")

db = SessionLocal()

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Vista General",
    "📈 Histórico",
    "🔍 Análisis por Rocket",
    "⚙️ ETL"
])

# =============================
# TAB 1 — VISTA GENERAL
# =============================

with tab1:

    st.subheader("Indicadores generales")

    col1, col2, col3, col4 = st.columns(4)

    total_launches = db.query(
        func.count(LaunchesCentral.launch_id)
    ).scalar()

    total_rockets = db.query(
        func.count(func.distinct(LaunchesCentral.rocket_name))
    ).scalar()

    total_payloads = db.query(
        func.sum(LaunchesCentral.payload_count)
    ).scalar()

    ultima_mision = db.query(
        func.max(LaunchesCentral.flight_number)
    ).scalar()

    col1.metric("🚀 Lanzamientos", total_launches)
    col2.metric("🛰 Rockets", total_rockets)
    col3.metric("📦 Payloads", total_payloads)
    col4.metric("🔢 Última misión", ultima_mision)

    st.markdown("---")

    # lanzamientos por rocket

    data = db.query(
        LaunchesCentral.rocket_name,
        func.count(LaunchesCentral.launch_id)
    ).group_by(
        LaunchesCentral.rocket_name
    ).all()

    df = pd.DataFrame(data, columns=["Rocket", "Launches"])

    fig = px.bar(
        df,
        x="Rocket",
        y="Launches",
        title="Lanzamientos por Rocket"
    )

    st.plotly_chart(fig, use_container_width=True)

    # masa total por rocket

    data_mass = db.query(
        LaunchesCentral.rocket_name,
        func.sum(LaunchesCentral.total_payload_mass)
    ).group_by(
        LaunchesCentral.rocket_name
    ).all()

    df_mass = pd.DataFrame(data_mass, columns=["Rocket", "Mass"])

    fig2 = px.bar(
        df_mass,
        x="Rocket",
        y="Mass",
        title="Masa total transportada por Rocket"
    )

    st.plotly_chart(fig2, use_container_width=True)


# =============================
# TAB 2 — HISTÓRICO
# =============================

with tab2:

    st.subheader("Análisis temporal")

    data = db.query(
        Launch.date_utc,
        LaunchesCentral.total_payload_mass
    ).join(
        LaunchesCentral,
        Launch.launch_id == LaunchesCentral.launch_id
    ).all()

    df = pd.DataFrame(
        data,
        columns=["Fecha", "PayloadMass"]
    )

    df = df.sort_values("Fecha")

    # lanzamientos por fecha

    df_count = df.groupby("Fecha").size().reset_index(name="Launches")

    fig = px.line(
        df_count,
        x="Fecha",
        y="Launches",
        title="Lanzamientos en el tiempo"
    )

    st.plotly_chart(fig, use_container_width=True)

    # masa transportada

    df_mass = df.groupby("Fecha")["PayloadMass"].sum().reset_index()

    fig2 = px.line(
        df_mass,
        x="Fecha",
        y="PayloadMass",
        title="Masa transportada en el tiempo"
    )

    st.plotly_chart(fig2, use_container_width=True)


# =============================
# TAB 3 — ANÁLISIS ROCKET
# =============================

with tab3:

    st.subheader("Análisis por Rocket")

    rockets = db.query(
        LaunchesCentral.rocket_name
    ).distinct().all()

    for rocket in rockets:

        rocket_name = rocket[0]

        with st.expander(f"🚀 {rocket_name}"):

            data = db.query(
                LaunchesCentral
            ).filter(
                LaunchesCentral.rocket_name == rocket_name
            ).all()

            launches = len(data)

            success_rate = sum(
                1 for r in data if r.success
            ) / launches * 100

            payload_avg = sum(
                r.payload_count for r in data
            ) / launches

            reused = sum(
                r.reused_cores for r in data
            )

            col1, col2, col3, col4 = st.columns(4)

            col1.metric("Launches", launches)
            col2.metric("Success Rate", f"{success_rate:.1f}%")
            col3.metric("Payload Promedio", f"{payload_avg:.2f}")
            col4.metric("Cores reutilizados", reused)


# =============================
# TAB 4 — ETL
# =============================

with tab4:

    st.subheader("Historial de ejecuciones ETL")

    data = db.query(
        MetricasETL
    ).order_by(
        MetricasETL.fecha.desc()
    ).limit(20).all()

    rows = []

    for m in data:

        rows.append({
            "Fecha": m.fecha,
            "Extraídos": m.registros_extraidos,
            "Guardados": m.registros_guardados,
            "Fallidos": m.registros_fallidos,
            "Estado": m.estado
        })

    df = pd.DataFrame(rows)

    st.dataframe(df, use_container_width=True)

    fig = px.bar(
        df,
        x="Fecha",
        y="Guardados",
        color="Estado",
        title="Registros guardados por ejecución"
    )

    st.plotly_chart(fig, use_container_width=True)


db.close()