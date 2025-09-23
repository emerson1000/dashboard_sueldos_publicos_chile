#!/usr/bin/env python3
"""
Consolida todos los datos: municipios enriquecidos + datos del SII.
"""

import pandas as pd
import logging
from pathlib import Path
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

def consolidar_todos_los_datos():
    """Consolida todos los datos disponibles."""
    
    logger.info("🚀 Iniciando consolidación final de todos los datos")
    
    # Cargar datos municipales enriquecidos
    municipios_file = BASE_DIR / 'data' / 'processed' / 'sueldos_reales_consolidado.csv'
    logger.info(f"Cargando datos municipales desde: {municipios_file}")
    df_municipios = pd.read_csv(municipios_file)
    logger.info(f"Municipios cargados: {len(df_municipios)} registros")
    
    # Cargar datos del SII
    sii_file = BASE_DIR / 'data' / 'raw' / 'sii_tablas' / '2025-09' / 'sii_combinado.csv'
    logger.info(f"Cargando datos del SII desde: {sii_file}")
    df_sii = pd.read_csv(sii_file)
    logger.info(f"SII cargado: {len(df_sii)} registros")
    
    # Preparar datos del SII para consolidación
    df_sii_proc = df_sii.copy()
    
    # Mapear columnas del SII a formato estándar
    if 'Nombres' in df_sii_proc.columns and 'Apellido Paterno' in df_sii_proc.columns:
        df_sii_proc['nombre'] = df_sii_proc['Nombres'].fillna('') + ' ' + df_sii_proc['Apellido Paterno'].fillna('')
        if 'Apellido Materno' in df_sii_proc.columns:
            df_sii_proc['nombre'] += ' ' + df_sii_proc['Apellido Materno'].fillna('')
        df_sii_proc['nombre'] = df_sii_proc['nombre'].str.strip()
    
    # Mapear sueldo (limpiar formato de números chilenos)
    if 'Honorario bruto mensual' in df_sii_proc.columns:
        # Limpiar formato: "2.290.200" -> 2290200
        df_sii_proc['sueldo_bruto'] = df_sii_proc['Honorario bruto mensual'].astype(str)
        df_sii_proc['sueldo_bruto'] = df_sii_proc['sueldo_bruto'].str.replace('.', '').str.replace(',', '.')
        df_sii_proc['sueldo_bruto'] = pd.to_numeric(df_sii_proc['sueldo_bruto'], errors='coerce')
    elif 'Remuneración bruta mensualizada' in df_sii_proc.columns:
        df_sii_proc['sueldo_bruto'] = df_sii_proc['Remuneración bruta mensualizada']
    elif 'Pago Mensual' in df_sii_proc.columns:
        df_sii_proc['sueldo_bruto'] = df_sii_proc['Pago Mensual']
    
    # Mapear cargo
    if 'Descripción de la función' in df_sii_proc.columns:
        df_sii_proc['cargo'] = df_sii_proc['Descripción de la función']
    elif 'Cargo o función' in df_sii_proc.columns:
        df_sii_proc['cargo'] = df_sii_proc['Cargo o función']
    
    # Mapear grado
    if 'Grado EUS' in df_sii_proc.columns:
        df_sii_proc['grado'] = df_sii_proc['Grado EUS']
    
    # Limpiar y estandarizar datos del SII
    df_sii_proc['organismo'] = 'Servicio de Impuestos Internos'
    df_sii_proc['estamento'] = df_sii_proc['tipo'].fillna('Sin especificar')
    df_sii_proc['cargo'] = df_sii_proc['cargo'].fillna('Sin especificar')
    df_sii_proc['grado'] = df_sii_proc['grado'].fillna('Sin especificar')
    df_sii_proc['nombre'] = df_sii_proc['nombre'].fillna('Sin especificar')
    df_sii_proc['fuente'] = 'sii_tablas_html'
    df_sii_proc['url_origen'] = f'http://www.sii.cl/transparencia/{df_sii_proc["anio"]}/per_{df_sii_proc["tipo"]}_{df_sii_proc["mes"]}.html'
    
    # Seleccionar columnas comunes
    columnas_comunes = ['fuente', 'url_origen', 'sueldo_bruto', 'organismo', 'estamento', 'grado', 'cargo', 'nombre']
    
    df_municipios_clean = df_municipios[columnas_comunes].copy()
    df_sii_clean = df_sii_proc[columnas_comunes].copy()
    
    # Combinar datasets
    logger.info("🔄 Combinando datasets...")
    df_final = pd.concat([df_municipios_clean, df_sii_clean], ignore_index=True)
    
    # Limpiar datos finales
    df_final['sueldo_bruto'] = pd.to_numeric(df_final['sueldo_bruto'], errors='coerce')
    
    # Filtrar registros válidos
    df_valid = df_final[
        (df_final['sueldo_bruto'].notna()) & 
        (df_final['sueldo_bruto'] > 0) &
        (df_final['sueldo_bruto'] < 10000000)  # Filtro de valores extremos
    ].copy()
    
    # Agregar metadatos
    df_valid['fecha_procesamiento'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"✅ Datos consolidados: {len(df_valid)} registros válidos")
    
    # Guardar datos finales en CSV y Parquet
    output_file_csv = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado_final.csv'
    output_file_parquet = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado_final.parquet'
    
    df_valid.to_csv(output_file_csv, index=False, encoding='utf-8')
    df_valid.to_parquet(output_file_parquet, index=False, compression='snappy')
    
    logger.info(f"✅ Datos finales guardados en:")
    logger.info(f"  CSV: {output_file_csv}")
    logger.info(f"  Parquet: {output_file_parquet}")
    
    # Mostrar comparación de tamaños
    csv_size = output_file_csv.stat().st_size / (1024*1024)  # MB
    parquet_size = output_file_parquet.stat().st_size / (1024*1024)  # MB
    compression_ratio = (1 - parquet_size/csv_size) * 100
    
    logger.info(f"📊 Comparación de tamaños:")
    logger.info(f"  CSV: {csv_size:.2f} MB")
    logger.info(f"  Parquet: {parquet_size:.2f} MB")
    logger.info(f"  Compresión: {compression_ratio:.1f}% más pequeño")
    
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
    stats_file = BASE_DIR / 'data' / 'processed' / 'estadisticas_finales.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    logger.info("📊 Estadísticas finales:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")
    
    # Estadísticas por organismo (top 10)
    stats_organismos = df_valid['organismo'].value_counts().head(10)
    logger.info("🏛️ Top 10 organismos:")
    for organismo, count in stats_organismos.items():
        logger.info(f"  {organismo}: {count} funcionarios")
    
    # Estadísticas por estamento
    stats_estamentos = df_valid['estamento'].value_counts()
    logger.info("📋 Distribución por estamento:")
    for estamento, count in stats_estamentos.items():
        logger.info(f"  {estamento}: {count} funcionarios")
    
    # Estadísticas por fuente
    stats_fuentes = df_valid['fuente'].value_counts()
    logger.info("📊 Distribución por fuente:")
    for fuente, count in stats_fuentes.items():
        logger.info(f"  {fuente}: {count} funcionarios")
    
    return df_valid

if __name__ == '__main__':
    consolidar_todos_los_datos()
