#!/usr/bin/env python3
"""
Script de extracción de datos desde la Contraloría General de la República.

Extrae tablas de la página de escalas de remuneraciones y las guarda en
`data/raw/contraloria/<YYYY-MM>/`. Este script es una aproximación simplificada
y puede requerir ajustes dependiendo del formato exacto de la página.
"""

import time
from pathlib import Path
import pandas as pd
import requests


# URL de la página de la Contraloría con escalas de remuneraciones de municipalidades
URL = "https://www.contraloria.cl/web/cgr/escalas-de-remuneraciones"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'


def main():
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'contraloria' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"No se pudo acceder a {URL}: {e}")
        return
    # Extrae tablas con pandas.read_html
    try:
        tables = pd.read_html(resp.text)
        if tables:
            for i, df in enumerate(tables):
                dest = dest_dir / f'tabla_{i}.csv'
                df.to_csv(dest, index=False)
                print(f"Tabla {i} guardada en {dest}")
        else:
            print("No se encontraron tablas en la página de la Contraloría.")
    except Exception as e:
        print(f"Error al procesar tablas de la Contraloría: {e}")


if __name__ == '__main__':
    main()