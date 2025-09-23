#!/usr/bin/env python3
"""
Categoriza los organismos en Municipalidades, Ministerios y Otros.
"""

import pandas as pd
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

# Lista de ministerios chilenos
MINISTERIOS = [
    'Ministerio de Educación',
    'Ministerio de Salud', 
    'Ministerio del Trabajo y Previsión Social',
    'Ministerio de Hacienda',
    'Ministerio del Interior y Seguridad Pública',
    'Ministerio de Justicia y Derechos Humanos',
    'Ministerio de Defensa Nacional',
    'Ministerio de Relaciones Exteriores',
    'Ministerio de Obras Públicas',
    'Ministerio de Transportes y Telecomunicaciones',
    'Ministerio de Vivienda y Urbanismo',
    'Ministerio del Medio Ambiente',
    'Ministerio de Energía',
    'Ministerio de Minería',
    'Ministerio de Agricultura',
    'Ministerio de Desarrollo Social y Familia',
    'Ministerio de las Culturas, las Artes y el Patrimonio',
    'Ministerio del Deporte',
    'Ministerio de la Mujer y la Equidad de Género',
    'Ministerio de Ciencia, Tecnología, Conocimiento e Innovación'
]

def categorizar_organismos(df):
    """Categoriza los organismos en Municipalidades, Ministerios y Otros."""
    if df.empty:
        return df
    
    logger.info("🏛️ Categorizando organismos...")
    
    # Crear copia
    df_categorized = df.copy()
    
    # Función para categorizar
    def categorizar_organismo(organismo):
        if pd.isna(organismo):
            return 'Sin especificar'
        
        organismo_str = str(organismo).strip()
        
        # Servicios públicos
        if organismo_str == 'Servicio de Impuestos Internos':
            return 'Servicios Públicos'
        
        # Ministerios
        if any(ministerio.lower() in organismo_str.lower() for ministerio in MINISTERIOS):
            return 'Ministerios'
        
        # Municipalidades
        if 'municipalidad' in organismo_str.lower():
            return 'Municipalidades'
        
        # Otros organismos del estado
        if any(palabra in organismo_str.lower() for palabra in ['ministerio', 'servicio', 'dirección', 'comisión', 'fiscalía']):
            return 'Otros Organismos del Estado'
        
        return 'Otros'
    
    # Aplicar categorización
    df_categorized['categoria_organismo'] = df_categorized['organismo'].apply(categorizar_organismo)
    
    # Estadísticas
    stats_categoria = df_categorized['categoria_organismo'].value_counts()
    logger.info("📊 Distribución por categoría:")
    for categoria, count in stats_categoria.items():
        logger.info(f"  {categoria}: {count} funcionarios")
    
    # Estadísticas detalladas por categoría
    logger.info("\n🏛️ Detalle por categoría:")
    for categoria in stats_categoria.index:
        df_cat = df_categorized[df_categorized['categoria_organismo'] == categoria]
        organismos_unicos = df_cat['organismo'].nunique()
        logger.info(f"  {categoria}: {len(df_cat)} funcionarios en {organismos_unicos} organismos")
        
        # Top 5 organismos de cada categoría
        top_organismos = df_cat['organismo'].value_counts().head(5)
        logger.info(f"    Top organismos:")
        for org, count in top_organismos.items():
            logger.info(f"      {org}: {count} funcionarios")
    
    return df_categorized

def main():
    """Función principal."""
    logger.info("🚀 Iniciando categorización de organismos")
    
    # Cargar datos finales
    input_file = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado_final.parquet'
    
    if not input_file.exists():
        # Fallback a CSV
        input_file = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado_final.csv'
    
    if not input_file.exists():
        logger.error(f"No se encontró el archivo: {input_file}")
        return
    
    logger.info(f"Cargando datos desde: {input_file}")
    
    if input_file.suffix == '.parquet':
        df = pd.read_parquet(input_file)
    else:
        df = pd.read_csv(input_file)
    
    logger.info(f"Datos cargados: {len(df)} registros")
    logger.info(f"Organismos únicos: {df['organismo'].nunique()}")
    
    # Categorizar organismos
    df_categorized = categorizar_organismos(df)
    
    # Guardar datos categorizados
    output_file_parquet = BASE_DIR / 'data' / 'processed' / 'sueldos_categorizados.parquet'
    output_file_csv = BASE_DIR / 'data' / 'processed' / 'sueldos_categorizados.csv'
    
    df_categorized.to_parquet(output_file_parquet, index=False, compression='snappy')
    df_categorized.to_csv(output_file_csv, index=False, encoding='utf-8')
    
    # Crear archivo pequeño para Streamlit Cloud
    df_small = df_categorized.sample(n=min(5000, len(df_categorized)), random_state=42)
    output_file_small_parquet = BASE_DIR / 'data' / 'processed' / 'sueldos_categorizados_small.parquet'
    output_file_small_csv = BASE_DIR / 'data' / 'processed' / 'sueldos_categorizados_small.csv'
    
    df_small.to_parquet(output_file_small_parquet, index=False, compression='snappy')
    df_small.to_csv(output_file_small_csv, index=False, encoding='utf-8')
    
    logger.info(f"✅ Datos categorizados guardados en:")
    logger.info(f"  Parquet: {output_file_parquet}")
    logger.info(f"  CSV: {output_file_csv}")
    logger.info(f"  Parquet pequeño: {output_file_small_parquet}")
    logger.info(f"  CSV pequeño: {output_file_small_csv}")
    
    # Mostrar comparación de tamaños
    csv_size = output_file_csv.stat().st_size / (1024*1024)  # MB
    parquet_size = output_file_parquet.stat().st_size / (1024*1024)  # MB
    compression_ratio = (1 - parquet_size/csv_size) * 100
    
    logger.info(f"📊 Comparación de tamaños:")
    logger.info(f"  CSV: {csv_size:.2f} MB")
    logger.info(f"  Parquet: {parquet_size:.2f} MB")
    logger.info(f"  Compresión: {compression_ratio:.1f}% más pequeño")
    
    logger.info(f"📊 Total registros: {len(df_categorized)}")
    logger.info(f"🏛️ Categorías únicas: {df_categorized['categoria_organismo'].nunique()}")

if __name__ == '__main__':
    main()
