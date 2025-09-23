#!/usr/bin/env python3
"""
Procesa los datos consolidados de todas las fuentes.
"""

import pandas as pd
import logging
from pathlib import Path
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

def process_consolidated_data():
    """Procesa los datos consolidados."""
    
    # Cargar datos consolidados
    consolidated_file = BASE_DIR / 'data' / 'raw' / 'consolidado' / '2025-09' / 'todos_los_datos.csv'
    
    if not consolidated_file.exists():
        logger.error(f"No se encontró el archivo consolidado: {consolidated_file}")
        return
    
    logger.info(f"Cargando datos consolidados desde: {consolidated_file}")
    df = pd.read_csv(consolidated_file)
    
    logger.info(f"Datos cargados: {len(df)} registros")
    
    # Limpiar datos
    df_clean = df.copy()
    
    # Convertir sueldo_bruto a numérico
    df_clean['sueldo_bruto'] = pd.to_numeric(df_clean['sueldo_bruto'], errors='coerce')
    
    # Mapear columnas a nombres estándar
    if 'Estamento' in df_clean.columns:
        df_clean['estamento'] = df_clean['Estamento'].fillna('Sin especificar')
        df_clean['estamento'] = df_clean['estamento'].str.strip()
    else:
        df_clean['estamento'] = 'Sin especificar'
    
    if 'Grado' in df_clean.columns:
        df_clean['grado'] = df_clean['Grado'].fillna('Sin especificar')
        df_clean['grado'] = df_clean['grado'].astype(str).str.strip()
    else:
        df_clean['grado'] = 'Sin especificar'
    
    # Limpiar organismos
    df_clean['organismo'] = df_clean['organismo'].fillna('Sin especificar')
    df_clean['organismo'] = df_clean['organismo'].str.strip()
    
    # Limpiar nombres
    df_clean['nombre'] = df_clean['nombre'].fillna('Sin especificar')
    df_clean['nombre'] = df_clean['nombre'].str.strip()
    
    # Limpiar cargos
    df_clean['cargo'] = df_clean['cargo'].fillna('Sin especificar')
    df_clean['cargo'] = df_clean['cargo'].str.strip()
    
    # Agregar metadatos
    df_clean['fuente'] = df_clean['fuente'].fillna('consolidado')
    df_clean['archivo_origen'] = df_clean['url_origen'].fillna('consolidado')
    df_clean['fecha_procesamiento'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Filtrar registros válidos
    df_valid = df_clean[
        (df_clean['sueldo_bruto'].notna()) & 
        (df_clean['sueldo_bruto'] > 0) &
        (df_clean['sueldo_bruto'] < 10000000)  # Filtro de valores extremos
    ].copy()
    
    logger.info(f"Registros válidos después de limpieza: {len(df_valid)}")
    
    # Guardar datos procesados
    output_file = BASE_DIR / 'data' / 'processed' / 'sueldos_reales_consolidado.csv'
    df_valid.to_csv(output_file, index=False, encoding='utf-8')
    
    logger.info(f"Datos procesados guardados en: {output_file}")
    
    # Generar estadísticas
    stats = {
        'total_registros': len(df_valid),
        'organismos_unicos': df_valid['organismo'].nunique(),
        'estamentos_unicos': df_valid['estamento'].nunique(),
        'grados_unicos': df_valid['grado'].nunique(),
        'fuentes_unicas': df_valid['fuente'].nunique(),
        'promedio_sueldo': float(df_valid['sueldo_bruto'].mean()),
        'mediana_sueldo': float(df_valid['sueldo_bruto'].median()),
        'min_sueldo': float(df_valid['sueldo_bruto'].min()),
        'max_sueldo': float(df_valid['sueldo_bruto'].max()),
        'desv_std': float(df_valid['sueldo_bruto'].std())
    }
    
    # Guardar estadísticas
    stats_file = BASE_DIR / 'data' / 'processed' / 'estadisticas_reales.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    logger.info("Estadísticas generadas:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")
    
    return df_valid

if __name__ == '__main__':
    process_consolidated_data()
