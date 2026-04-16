#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

def generar_reporte_visual():
    data_dir = 'data'
    output_file = 'reporte_spacex_v2.png'
    
    if not os.path.exists(data_dir):
        print("Error: Carpeta 'data' no encontrada.")
        return

    try:
        launches_df = pd.read_csv(f'{data_dir}/launches.csv')
        payloads_df = pd.read_csv(f'{data_dir}/payloads.csv')
    except Exception as e:
        print(f"Error al cargar archivos: {e}")
        return

    fig, axs = plt.subplots(2, 2, figsize=(16, 14))
    plt.subplots_adjust(hspace=0.4, wspace=0.3)
    fig.suptitle('Análisis Operativo y Comercial - SpaceX', fontsize=22, fontweight='bold', y=0.96)

    # 1. LÍNEAS: Lanzamientos por Año
    launches_df['year'] = pd.to_datetime(launches_df['date_utc']).dt.year
    l_per_year = launches_df.groupby('year').size()
    axs[0, 0].plot(l_per_year.index, l_per_year.values, marker='o', color='#1f77b4', linewidth=2.5)
    axs[0, 0].set_title('Crecimiento Anual de Lanzamientos', fontsize=14, pad=10)
    axs[0, 0].grid(True, alpha=0.3)

    # 2. BARRAS: Éxito vs Fracaso
    success_counts = launches_df['success'].value_counts().rename({True: 'Éxito', False: 'Fracaso'})
    axs[0, 1].bar(success_counts.index.astype(str), success_counts.values, color=['#2ca02c', '#d62728'], width=0.6)
    axs[0, 1].set_title('Ratio de Éxito de Misiones', fontsize=14, pad=10)
    for i, v in enumerate(success_counts.values):
        axs[0, 1].text(i, v + 2, str(v), ha='center', fontweight='bold')

    # 3. TORTA: Distribución de Órbitas (Top 5 + Otros)
    orbit_counts = payloads_df['orbit'].value_counts()
    top_orbits = orbit_counts.head(5)
    others_count = orbit_counts.iloc[5:].sum()
    if others_count > 0:
        top_orbits['Otros'] = others_count

    axs[1, 0].pie(top_orbits.values, labels=top_orbits.index, autopct='%1.1f%%', 
                 startangle=140, colors=plt.cm.Set3.colors, pctdistance=0.85)
    # Círculo blanco en el centro para estilo "Donut" (ayuda a la lectura de etiquetas)
    centre_circle = plt.Circle((0,0), 0.70, fc='white')
    axs[1, 0].add_artist(centre_circle)
    axs[1, 0].set_title('Destinos de Carga (Órbitas)', fontsize=14, pad=10)

    # 4. BARRAS HORIZONTALES: Top 10 Clientes
    # Como customers es un string separado por comas, lo expandimos
    customers_series = payloads_df['customers'].dropna().str.split(',').explode().str.strip()
    top_customers = (
        customers_series
        .value_counts()
        .head(10)
        .sort_values(ascending=True)
    )
    # Colores distintos para cada barra
    colors = plt.cm.tab10(np.linspace(0, 1, len(top_customers)))

    axs[1, 1].barh(
        top_customers.index,
        top_customers.values,
        color=colors
    )
    axs[1, 1].set_title('Top 10 Clientes (Volumen de Cargas)', fontsize=14, pad=10)
    axs[1, 1].set_xlabel('Número de Payloads')
    axs[1, 1].grid(axis='x', linestyle='--', alpha=0.5)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Reporte actualizado generado: {output_file}")

if __name__ == "__main__":
    generar_reporte_visual()