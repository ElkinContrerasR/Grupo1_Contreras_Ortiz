#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import LaunchesCentral, Rocket, MetricasETL

# Configuración página
st.set_page_config(
    page_title="SpaceX ETL Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🚀 Dashboard SpaceX - ETL")
st.markdown("---")

db = SessionLocal()

try:

    # =========================
    # Cargar datos principales
    # =========================
    launches = db.query(LaunchesCentral).all()

    data = []
    for l in launches:
        data.append({
            "Rocket": l.rocket_name,
            "Flight Number": l.flight_number,
            "Success": l.success,
            "Payload Count": l.payload_count,
            "Payload Mass": l.total_payload_mass,
            "Core Count": l.core_count,
            "Reused Cores": l.reused_cores
        })

    df = pd.DataFrame(data)

    # =========================
    # Sidebar filtros
    # =========================
    st.sidebar.title("Filtros")

    rockets = st.sidebar.multiselect(
        "Selecciona Rocket:",
        options=df["Rocket"].unique(),
        default=df["Rocket"].unique()
    )

    df_filtrado = df[df["Rocket"].isin(rockets)]

    # =========================
    # Métricas principales
    # =========================
    st.subheader("Métricas Principales")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_launches = len(df_filtrado)
        st.metric("Total Lanzamientos", total_launches)

    with col2:
        success_rate = df_filtrado["Success"].mean() * 100
        st.metric("Tasa de Éxito", f"{success_rate:.1f}%")

    with col3:
        avg_payload = df_filtrado["Payload Mass"].mean()
        st.metric("Carga Promedio (kg)", f"{avg_payload:.0f}")

    with col4:
        reused = df_filtrado["Reused Cores"].sum()
        st.metric("Cores Reutilizados", reused)

    st.markdown("---")

    # =========================
    # Gráficas
    # =========================

    st.subheader("Visualizaciones")

    col1, col2 = st.columns(2)

    # Lanzamientos por rocket
    with col1:
        rocket_counts = df_filtrado["Rocket"].value_counts().reset_index()
        rocket_counts.columns = ["Rocket", "Launches"]

        fig1 = px.bar(
            rocket_counts,
            x="Rocket",
            y="Launches",
            title="Lanzamientos por Rocket",
            color="Launches"
        )

        st.plotly_chart(fig1, use_container_width=True)

    # Payload total por rocket
    with col2:
        payload_mass = df_filtrado.groupby("Rocket")["Payload Mass"].sum().reset_index()

        fig2 = px.bar(
            payload_mass,
            x="Rocket",
            y="Payload Mass",
            title="Carga Total Transportada (kg)",
            color="Payload Mass"
        )

        st.plotly_chart(fig2, use_container_width=True)

    col1, col2 = st.columns(2)

    # Payload vs cores
    with col1:
        fig3 = px.scatter(
            df_filtrado,
            x="Payload Mass",
            y="Core Count",
            size="Payload Count",
            color="Rocket",
            title="Payload vs Cores"
        )

        st.plotly_chart(fig3, use_container_width=True)

    # Reutilización de cores
    with col2:
        reused_data = df_filtrado.groupby("Rocket")["Reused Cores"].sum().reset_index()

        fig4 = px.bar(
            reused_data,
            x="Rocket",
            y="Reused Cores",
            title="Reutilización de Cores",
            color="Reused Cores"
        )

        st.plotly_chart(fig4, use_container_width=True)

    st.markdown("---")

    # =========================
    # Tabla detallada
    # =========================
    st.subheader("Datos Detallados")

    st.dataframe(
        df_filtrado.sort_values("Flight Number", ascending=False),
        use_container_width=True,
        height=400
    )

    # =========================
    # Métricas ETL
    # =========================
    st.markdown("---")
    st.subheader("Estado del ETL")

    metricas = db.query(MetricasETL).order_by(MetricasETL.fecha.desc()).first()

    if metricas:

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Registros Extraídos", metricas.registros_extraidos)

        with col2:
            st.metric("Registros Guardados", metricas.registros_guardados)

        with col3:
            st.metric("Registros Fallidos", metricas.registros_fallidos)

        with col4:
            st.metric("Estado", metricas.estado)

finally:
    db.close()