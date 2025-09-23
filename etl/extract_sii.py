#!/usr/bin/env python3
"""
Script de extracción de datos desde el Servicio de Impuestos Internos (SII).

Descarga la tabla de escalas de remuneraciones publicada en la página de transparencia
del SII y la guarda en `data/raw/sii/<YYYY-MM>/escala.csv`. Este script usa pandas para
leer tablas directamente desde HTML.
"""

import time
from pathlib import Path
import pandas as pd


# URL de la página de transparencia activa del SII con la escala de remuneraciones. Actualiza si cambia.
URL = "https://www.sii.cl/transparencia/2025/plantilla_escala_ene.html"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'


def main():
    # Directorio para el mes actual
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'sii' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        # pandas.read_html devuelve una lista de dataframes; se asume que la primera tabla es la relevante
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        tables = pd.read_html(URL, headers=headers)
        if not tables:
            print("No se encontraron tablas en la página del SII.")
            return
        df = tables[0]
        dest = dest_dir / 'escala.csv'
        df.to_csv(dest, index=False)
        print(f"Tabla de escalas del SII guardada en {dest}")
    except Exception as e:
        print(f"Error al procesar la página del SII: {e}")


if __name__ == '__main__':
    main()