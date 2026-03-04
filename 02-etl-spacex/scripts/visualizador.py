#!/usr/bin/env python3
import os
import pandas as pd
import matplotlib.pyplot as plt
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_PATH = "data/launches.csv"
OUTPUT_DIR = "data"

def main():
    try:
        logging.info("Leyendo archivo launches.csv")
        df = pd.read_csv(DATA_PATH)

        # Convertir a numérico (True/False -> 1/0)
        df["launch_success"] = df["launch_success"].astype(float)

        # ===============================
        # Cálculos
        # ===============================
        success_rate = (
            df.groupby("rocket_name")["launch_success"]
            .mean()
            .sort_values(ascending=False)
        )

        launch_count = (
            df.groupby("rocket_name")
            .size()
            .sort_values(ascending=False)
        )

        # ===============================
        # Figura única con 2 subplots
        # ===============================
        fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 10))

        # --- Gráfico 1: Tasa de éxito ---
        success_rate.plot(
            kind="bar",
            ax=axes[0]
        )
        axes[0].set_title("Tasa de Éxito por Rocket")
        axes[0].set_xlabel("Rocket")
        axes[0].set_ylabel("Tasa de éxito (0-1)")
        axes[0].tick_params(axis="x", rotation=45)

        # --- Gráfico 2: Número de lanzamientos ---
        launch_count.plot(
            kind="bar",
            ax=axes[1]
        )
        axes[1].set_title("Número de Lanzamientos por Rocket")
        axes[1].set_xlabel("Rocket")
        axes[1].set_ylabel("Cantidad de Lanzamientos")
        axes[1].tick_params(axis="x", rotation=45)

        plt.tight_layout()

        output_path = os.path.join(OUTPUT_DIR, "rockets_analysis.png")
        plt.savefig(output_path)
        plt.close()

        logging.info(f"Gráfico combinado guardado en: {output_path}")
        print("\nGráfico combinado generado correctamente.")

    except Exception as e:
        logging.error(f"Error generando gráfico: {e}")


if __name__ == "__main__":
    main()