#!/usr/bin/env python3
"""
Filtra datos problemáticos (sueldos menores al mínimo legal).
"""

import pandas as pd
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

# Sueldo mínimo más conservador para no eliminar datos legítimos
SUELDO_MINIMO_CHILE = 200000  # pesos chilenos (más conservador)

def filtrar_datos_problematicos(df):
    """Filtra datos con sueldos menores al mínimo legal."""
    if df.empty:
        return df
    
    logger.info("🔍 Filtrando datos problemáticos...")
    
    # Datos originales
    total_original = len(df)
    
    # Identificar datos problemáticos
    datos_problematicos = df[df['sueldo_bruto'] < SUELDO_MINIMO_CHILE].copy()
    datos_validos = df[df['sueldo_bruto'] >= SUELDO_MINIMO_CHILE].copy()
    
    logger.info(f"📊 Análisis de datos problemáticos:")
    logger.info(f"  Total original: {total_original:,} registros")
    logger.info(f"  Datos problemáticos: {len(datos_problematicos):,} registros")
    logger.info(f"  Datos válidos: {len(datos_validos):,} registros")
    
    if len(datos_problematicos) > 0:
        logger.info(f"  Porcentaje problemático: {len(datos_problematicos)/total_original*100:.1f}%")
        
        # Análisis por organismo
        logger.info("\n📋 Datos problemáticos por organismo:")
        org_problematicos = datos_problematicos['organismo'].value_counts()
        for org, count in org_problematicos.items():
            logger.info(f"  {org}: {count} registros")
        
        # Análisis por fuente
        logger.info("\n📋 Datos problemáticos por fuente:")
        fuente_problematicos = datos_problematicos['fuente'].value_counts()
        for fuente, count in fuente_problematicos.items():
            logger.info(f"  {fuente}: {count} registros")
        
        # Guardar datos problemáticos para revisión
        output_problematicos = BASE_DIR / 'data' / 'processed' / 'datos_problematicos.csv'
        datos_problematicos.to_csv(output_problematicos, index=False, encoding='utf-8')
        logger.info(f"📁 Datos problemáticos guardados en: {output_problematicos}")
    
    return datos_validos

def main():
    """Función principal."""
    logger.info("🚀 Iniciando filtrado de datos problemáticos")
    
    # Cargar datos categorizados
    input_file = BASE_DIR / 'data' / 'processed' / 'sueldos_categorizados.parquet'
    
    if not input_file.exists():
        # Fallback a CSV
        input_file = BASE_DIR / 'data' / 'processed' / 'sueldos_categorizados.csv'
    
    if not input_file.exists():
        logger.error(f"No se encontró el archivo: {input_file}")
        return
    
    logger.info(f"Cargando datos desde: {input_file}")
    
    if input_file.suffix == '.parquet':
        df = pd.read_parquet(input_file)
    else:
        df = pd.read_csv(input_file)
    
    logger.info(f"Datos cargados: {len(df)} registros")
    
    # Filtrar datos problemáticos
    df_filtrado = filtrar_datos_problematicos(df)
    
    # Guardar datos filtrados
    output_file_parquet = BASE_DIR / 'data' / 'processed' / 'sueldos_filtrados.parquet'
    output_file_csv = BASE_DIR / 'data' / 'processed' / 'sueldos_filtrados.csv'
    
    df_filtrado.to_parquet(output_file_parquet, index=False, compression='snappy')
    df_filtrado.to_csv(output_file_csv, index=False, encoding='utf-8')
    
    # Crear archivo pequeño para Streamlit Cloud
    df_small = df_filtrado.sample(n=min(5000, len(df_filtrado)), random_state=42)
    output_file_small_parquet = BASE_DIR / 'data' / 'processed' / 'sueldos_filtrados_small.parquet'
    output_file_small_csv = BASE_DIR / 'data' / 'processed' / 'sueldos_filtrados_small.csv'
    
    df_small.to_parquet(output_file_small_parquet, index=False, compression='snappy')
    df_small.to_csv(output_file_small_csv, index=False, encoding='utf-8')
    
    logger.info(f"✅ Datos filtrados guardados en:")
    logger.info(f"  Parquet: {output_file_parquet}")
    logger.info(f"  CSV: {output_file_csv}")
    logger.info(f"  Parquet pequeño: {output_file_small_parquet}")
    logger.info(f"  CSV pequeño: {output_file_small_csv}")
    
    # Estadísticas finales
    logger.info(f"\n📊 Estadísticas finales:")
    logger.info(f"  Registros válidos: {len(df_filtrado):,}")
    logger.info(f"  Organismos únicos: {df_filtrado['organismo'].nunique()}")
    logger.info(f"  Categorías únicas: {df_filtrado['categoria_organismo'].nunique()}")
    logger.info(f"  Sueldo mínimo: ${df_filtrado['sueldo_bruto'].min():,.0f}")
    logger.info(f"  Sueldo máximo: ${df_filtrado['sueldo_bruto'].max():,.0f}")
    logger.info(f"  Sueldo promedio: ${df_filtrado['sueldo_bruto'].mean():,.0f}")
    
    # Distribución por categoría
    logger.info(f"\n📋 Distribución por categoría:")
    for categoria, count in df_filtrado['categoria_organismo'].value_counts().items():
        logger.info(f"  {categoria}: {count} funcionarios")

if __name__ == '__main__':
    main()
