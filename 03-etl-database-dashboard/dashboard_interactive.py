#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import func, and_
from datetime import datetime, timedelta

from scripts.database import SessionLocal
from scripts.models import Launch, LaunchesCentral, MetricasETL

st.set_page_config(
    page_title="SpaceX Dashboard",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 SpaceX Launch Analytics Dashboard")

db = SessionLocal()

# =============================
# SIDEBAR — CONTROLES
# =============================

st.sidebar.markdown("## 🎛️ Controles")

# Rockets disponibles
rockets_db = db.query(
    LaunchesCentral.rocket_name
).distinct().all()

rockets = [r[0] for r in rockets_db]

selected_rockets = st.sidebar.multiselect(
    "🚀 Rockets",
    options=rockets,
    default=rockets
)

# rango de fechas

min_date = db.query(func.min(Launch.date_utc)).scalar()
max_date = db.query(func.max(Launch.date_utc)).scalar()
min_date = min_date.date()
max_date = max_date.date()

fecha_inicio = st.sidebar.date_input(
    "📅 Desde",
    value=min_date
)

fecha_fin = st.sidebar.date_input(
    "📅 Hasta",
    value=max_date
)

# filtro payload mass

payload_min = st.sidebar.slider(
    "Payload mínimo",
    0,
    100000,
    0
)

payload_max = st.sidebar.slider(
    "Payload máximo",
    0,
    100000,
    100000
)

# =============================
# NUEVO — OPCIÓN ANÁLISIS AVANZADO
# =============================

ver_analisis_avanzado = st.sidebar.checkbox(
    "Mostrar análisis avanzado"
)

# =============================
# QUERY BASE FILTRADA
# =============================

# =============================
# QUERY BASE FILTRADA (DINÁMICA)
# =============================

query = db.query(
    Launch.date_utc,
    LaunchesCentral.rocket_name,
    LaunchesCentral.success,
    LaunchesCentral.payload_count,
    LaunchesCentral.total_payload_mass,
    LaunchesCentral.reused_cores
).join(
    Launch,
    Launch.launch_id == LaunchesCentral.launch_id
)

# lista de filtros dinámicos
filtros = []

if selected_rockets:
    filtros.append(
        LaunchesCentral.rocket_name.in_(selected_rockets)
    )

if fecha_inicio:
    filtros.append(
        Launch.date_utc >= fecha_inicio
    )

if fecha_fin:
    filtros.append(
        Launch.date_utc <= fecha_fin
    )

if payload_min is not None:
    filtros.append(
        LaunchesCentral.total_payload_mass >= payload_min
    )

if payload_max is not None:
    filtros.append(
        LaunchesCentral.total_payload_mass <= payload_max
    )

# aplicar filtros solo si existen
if filtros:
    query = query.filter(*filtros)

data = query.all()

# =============================
# TABS
# =============================

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

    if not df.empty:

        st.subheader("Indicadores generales")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("🚀 Lanzamientos", len(df))
        col2.metric("🛰 Rockets", df["rocket"].nunique())
        col3.metric("📦 Payloads", df["payload_count"].sum())
        col4.metric("📊 Payload total", f"{df['payload_mass'].sum():,.0f} kg")

        st.markdown("---")

        col1, col2 = st.columns(2)

        # Lanzamientos por rocket

        with col1:

            launches_rocket = df.groupby("rocket").size().reset_index(name="launches")

            fig = px.bar(
                launches_rocket,
                x="rocket",
                y="launches",
                color="rocket",
                title="Lanzamientos por Rocket"
            )

            st.plotly_chart(fig, use_container_width=True)

        # Payload por rocket

        with col2:

            payload_rocket = df.groupby("rocket")["payload_mass"].sum().reset_index()

            fig = px.bar(
                payload_rocket,
                x="rocket",
                y="payload_mass",
                color="rocket",
                title="Masa transportada por Rocket"
            )

            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        col1, col2 = st.columns(2)

        # NUEVO — Success Rate por rocket

        with col1:

            success_rate = df.groupby("rocket")["success"].mean().reset_index()
            success_rate["success"] = success_rate["success"] * 100

            fig = px.bar(
                success_rate,
                x="rocket",
                y="success",
                title="Success Rate por Rocket"
            )

            st.plotly_chart(fig, use_container_width=True)

        # NUEVO — distribución payload mass

        with col2:

            fig = px.histogram(
                df,
                x="payload_mass",
                nbins=30,
                title="Distribución de Payload Mass"
            )

            st.plotly_chart(fig, use_container_width=True)

        # =============================
        # NUEVO — ANÁLISIS AVANZADO
        # =============================

        if ver_analisis_avanzado:

            st.markdown("---")
            st.subheader("Análisis avanzado")

            fig = px.box(
                df,
                x="rocket",
                y="payload_mass",
                title="Distribución de payload por rocket"
            )

            st.plotly_chart(fig, use_container_width=True)

        # =============================
        # DESCARGA DE DATASET
        # =============================

        st.markdown("---")

        col1, col2 = st.columns([3,1])

        with col2:

            csv = df.to_csv(index=False).encode("utf-8")

            st.download_button(
                label="⬇️ Descargar dataset filtrado",
                data=csv,
                file_name="spacex_launches_filtrados.csv",
                mime="text/csv"
            )

# =============================
# TAB 2 — HISTÓRICO
# =============================

with tab2:

    if not df.empty:

        st.subheader("Análisis temporal")

        df["date"] = pd.to_datetime(df["date"])

        launches_time = df.groupby("date").size().reset_index(name="launches")

        fig = px.line(
            launches_time,
            x="date",
            y="launches",
            title="Lanzamientos en el tiempo",
            markers=True
        )

        st.plotly_chart(fig, use_container_width=True)

        payload_time = df.groupby("date")["payload_mass"].sum().reset_index()

        fig = px.line(
            payload_time,
            x="date",
            y="payload_mass",
            title="Masa transportada en el tiempo",
            markers=True
        )

        st.plotly_chart(fig, use_container_width=True)

# =============================
# TAB 3 — ANÁLISIS ROCKET
# =============================

with tab3:

    if not df.empty:

        rockets = df["rocket"].unique()

        for rocket in rockets:

            with st.expander(f"🚀 {rocket}"):

                r = df[df["rocket"] == rocket]

                launches = len(r)
                success_rate = r["success"].mean() * 100
                payload_avg = r["payload_count"].mean()
                reused = r["reused_cores"].sum()

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

    df_etl = pd.DataFrame(rows)

    st.dataframe(df_etl, use_container_width=True)

    fig = px.bar(
        df_etl,
        x="Fecha",
        y="Guardados",
        color="Estado",
        title="Registros guardados por ejecución"
    )

    st.plotly_chart(fig, use_container_width=True)

db.close()