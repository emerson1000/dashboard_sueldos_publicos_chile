#!/usr/bin/env python3
"""
Transforma y normaliza los datos descargados desde las fuentes públicas.

Lee los archivos en `data/raw/<fuente>/<YYYY-MM>/`, intenta mapear columnas relevantes a un esquema
estándar y guarda los resultados en `data/processed/sueldos_<YYYY-MM>.csv` y
`data/processed/sueldos_consolidado.csv`.

Esta normalización es heurística; dependiendo del origen y formato de los datos
puedes necesitar ajustar las reglas de mapeo. El objetivo es obtener las columnas:
- organismo
- nombre
- cargo
- grado
- estamento
- sueldo_bruto
- fuente
- fecha_procesamiento
"""

import time
import logging
from pathlib import Path
import pandas as pd
import re
from datetime import datetime
import numpy as np

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / 'data' / 'raw'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'

# Mapeos específicos por fuente
SOURCE_MAPPINGS = {
    'dipres': {
        'organismo': ['institucion', 'organismo', 'servicio', 'ministerio'],
        'nombre': ['nombre', 'funcionario', 'empleado', 'apellido'],
        'cargo': ['cargo', 'puesto', 'denominacion', 'funcion'],
        'grado': ['grado', 'categoria', 'nivel', 'escala'],
        'estamento': ['estamento', 'tipo', 'clasificacion'],
        'sueldo': ['sueldo', 'remuneracion', 'bruto', 'liquido', 'total', 'monto']
    },
    'sii': {
        'organismo': ['organismo', 'institucion', 'servicio'],
        'nombre': ['nombre', 'funcionario'],
        'cargo': ['cargo', 'puesto'],
        'grado': ['grado', 'categoria', 'nivel'],
        'estamento': ['estamento', 'tipo'],
        'sueldo': ['sueldo', 'remuneracion', 'bruto', 'total']
    },
    'contraloria': {
        'organismo': ['organismo', 'institucion', 'servicio'],
        'nombre': ['nombre', 'funcionario'],
        'cargo': ['cargo', 'puesto'],
        'grado': ['grado', 'categoria'],
        'estamento': ['estamento', 'tipo'],
        'sueldo': ['sueldo', 'remuneracion', 'bruto']
    }
}

def clean_text(text):
    """Limpia texto eliminando caracteres especiales y normalizando."""
    if pd.isna(text) or text == '':
        return None
    
    text = str(text).strip()
    if text.lower() in ['nan', 'none', 'null', '']:
        return None
    
    # Normalizar espacios múltiples
    text = re.sub(r'\s+', ' ', text)
    
    return text

def clean_numeric(value):
    """Limpia valores numéricos eliminando caracteres no numéricos."""
    if pd.isna(value):
        return None
    
    # Convertir a string si no lo es
    value_str = str(value).strip()
    
    # Eliminar caracteres no numéricos excepto punto y coma
    value_str = re.sub(r'[^\d.,]', '', value_str)
    
    if value_str == '' or value_str == '.':
        return None
    
    # Manejar diferentes formatos de separadores decimales
    if ',' in value_str and '.' in value_str:
        # Verificar si es formato europeo (1.234.567,89) o chileno (1.234.567,0)
        parts_comma = value_str.split(',')
        if len(parts_comma) == 2 and len(parts_comma[1]) <= 2:
            # Formato europeo: 1.234.567,89 - eliminar puntos de miles
            value_str = value_str.replace('.', '').replace(',', '.')
        else:
            # Formato chileno: 1.234.567,0 - eliminar solo comas
            value_str = value_str.replace(',', '')
    elif ',' in value_str:
        # Verificar si es separador decimal o de miles
        parts = value_str.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Separador decimal
            value_str = value_str.replace(',', '.')
        else:
            # Separador de miles
            value_str = value_str.replace(',', '')
    elif '.' in value_str:
        # Solo puntos - verificar si es separador de miles o decimal
        parts = value_str.split('.')
        if len(parts) > 2:
            # Múltiples puntos = separadores de miles (ej: 1.234.567)
            value_str = value_str.replace('.', '')
        elif len(parts) == 2 and len(parts[1]) <= 2:
            # Un punto con 1-2 dígitos después = decimal (ej: 123.45)
            pass  # Mantener como está
        else:
            # Un punto con más de 2 dígitos después = separador de miles (ej: 1234.567)
            value_str = value_str.replace('.', '')
    
    try:
        return float(value_str)
    except ValueError:
        return None

def find_best_column(df, possible_names, source_name):
    """Encuentra la mejor columna basada en nombres posibles."""
    cols = {c.lower(): c for c in df.columns}
    
    # Usar mapeos específicos de la fuente si están disponibles
    if source_name in SOURCE_MAPPINGS:
        possible_names = SOURCE_MAPPINGS[source_name].get('organismo', possible_names)
    
    for name in possible_names:
        for col_name, original_col in cols.items():
            if name in col_name:
                return original_col
    
    return None

def guess_and_normalize(df: pd.DataFrame, source_name: str, file_path: str) -> pd.DataFrame:
    """Intenta normalizar un DataFrame a las columnas estándar."""
    logger.info(f"Procesando archivo: {file_path}")
    
    cols = {c.lower(): c for c in df.columns}
    out = pd.DataFrame()
    
    def pick(poss):
        return find_best_column(df, poss, source_name)
    
    # Mapear organismo
    org_col = pick(['instit', 'organismo', 'servicio', 'ministerio'])
    if org_col:
        out['organismo'] = df[org_col].apply(clean_text)
    else:
        out['organismo'] = None
    
    # Mapear nombre
    name_col = pick(['nombre', 'funcionario', 'empleado', 'apellido'])
    if name_col:
        out['nombre'] = df[name_col].apply(clean_text)
    else:
        out['nombre'] = None
    
    # Mapear cargo
    cargo_col = pick(['cargo', 'puesto', 'denominacion', 'funcion'])
    if cargo_col:
        out['cargo'] = df[cargo_col].apply(clean_text)
    else:
        out['cargo'] = None
    
    # Mapear grado
    grado_col = pick(['grado', 'categoria', 'nivel', 'escala'])
    if grado_col:
        out['grado'] = df[grado_col].apply(clean_text)
    else:
        out['grado'] = None
    
    # Mapear estamento
    estamento_col = pick(['estamento', 'tipo', 'clasificacion'])
    if estamento_col:
        out['estamento'] = df[estamento_col].apply(clean_text)
    else:
        out['estamento'] = None
    
    # Mapear sueldo - específico para SII
    if source_name == 'sii':
        # Para SII, usar la última columna que contiene "Remuneracion Bruta Mensualizada"
        sal_col = None
        for col in df.columns:
            if 'remuneracion bruta mensualizada' in col.lower():
                sal_col = col
                break
        if not sal_col:
            # Si no encuentra la columna específica, usar la última columna
            sal_col = df.columns[-1]
    else:
        sal_col = pick(['sueldo', 'remuner', 'bruto', 'liquido', 'total', 'monto'])
    
    if sal_col:
        out['sueldo_bruto'] = df[sal_col].apply(clean_numeric)
    else:
        out['sueldo_bruto'] = None
    
    # Agregar metadatos
    out['fuente'] = source_name
    out['archivo_origen'] = Path(file_path).name
    out['fecha_procesamiento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Filtrar filas completamente vacías
    out = out.dropna(how='all')
    
    logger.info(f"Procesadas {len(out)} filas del archivo {file_path}")
    
    return out

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Valida y limpia los datos procesados."""
    logger.info("Validando datos procesados...")
    
    # Eliminar filas sin sueldo
    df = df.dropna(subset=['sueldo_bruto'])
    
    # Eliminar sueldos negativos o cero
    df = df[df['sueldo_bruto'] > 0]
    
    # Eliminar sueldos extremos (outliers)
    Q1 = df['sueldo_bruto'].quantile(0.01)
    Q99 = df['sueldo_bruto'].quantile(0.99)
    df = df[(df['sueldo_bruto'] >= Q1) & (df['sueldo_bruto'] <= Q99)]
    
    # Limpiar organismos
    df['organismo'] = df['organismo'].fillna('Sin especificar')
    df['organismo'] = df['organismo'].str.strip()
    
    # Limpiar estamentos
    df['estamento'] = df['estamento'].fillna('Sin especificar')
    df['estamento'] = df['estamento'].str.strip()
    
    # Limpiar grados
    df['grado'] = df['grado'].fillna('Sin especificar')
    df['grado'] = df['grado'].astype(str).str.strip()
    
    logger.info(f"Datos validados: {len(df)} registros finales")
    
    return df

def generate_summary_stats(df: pd.DataFrame):
    """Genera estadísticas resumen de los datos procesados."""
    stats = {
        'total_registros': len(df),
        'organismos_unicos': df['organismo'].nunique(),
        'estamentos_unicos': df['estamento'].nunique(),
        'grados_unicos': df['grado'].nunique(),
        'fuentes_unicas': df['fuente'].nunique(),
        'promedio_sueldo': df['sueldo_bruto'].mean(),
        'mediana_sueldo': df['sueldo_bruto'].median(),
        'min_sueldo': df['sueldo_bruto'].min(),
        'max_sueldo': df['sueldo_bruto'].max(),
        'desv_std': df['sueldo_bruto'].std()
    }
    
    logger.info("Estadísticas resumen:")
    for key, value in stats.items():
        if isinstance(value, float):
            logger.info(f"  {key}: {value:,.2f}")
        else:
            logger.info(f"  {key}: {value:,}")
    
    return stats

def main():
    y_m = time.strftime("%Y-%m")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Iniciando transformación de datos para {y_m}")
    
    dfs = []
    total_files_processed = 0
    
    for source_name in ['dipres', 'sii', 'contraloria']:
        source_dir = RAW_DIR / source_name / y_m
        if not source_dir.exists():
            logger.warning(f"Directorio no encontrado: {source_dir}")
            continue
        
        logger.info(f"Procesando fuente: {source_name}")
        
        for file in source_dir.iterdir():
            if file.is_file():
                try:
                    # Considera sólo CSV y Excel para la transformación básica
                    if file.suffix.lower() == '.csv':
                        df = pd.read_csv(file, encoding='utf-8')
                    elif file.suffix.lower() in ('.xlsx', '.xls'):
                        df = pd.read_excel(file)
                    else:
                        logger.info(f"Saltando archivo no soportado: {file}")
                        continue
                    
                    if df.empty:
                        logger.warning(f"Archivo vacío: {file}")
                        continue
                    
                    df_norm = guess_and_normalize(df, source_name, str(file))
                    
                    if not df_norm.empty:
                        dfs.append(df_norm)
                        total_files_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error al procesar {file}: {e}")
                    continue
    
    if not dfs:
        logger.error("No hay datos para transformar.")
        return
    
    logger.info(f"Procesados {total_files_processed} archivos")
    
    # Concatenar todos los DataFrames
    big = pd.concat(dfs, ignore_index=True, sort=False)
    logger.info(f"Total de registros antes de validación: {len(big)}")
    
    # Validar datos
    big = validate_data(big)
    
    # Generar estadísticas
    stats = generate_summary_stats(big)
    
    # Guardar archivos
    out_file = PROCESSED_DIR / f'sueldos_{y_m}.csv'
    big.to_csv(out_file, index=False, encoding='utf-8')
    
    # Copia el CSV como último consolidado
    big.to_csv(PROCESSED_DIR / 'sueldos_consolidado.csv', index=False, encoding='utf-8')
    
    logger.info(f"Datos transformados y guardados en {out_file}")
    logger.info(f"Archivo consolidado actualizado: {PROCESSED_DIR / 'sueldos_consolidado.csv'}")
    
    # Guardar estadísticas
    stats_file = PROCESSED_DIR / f'estadisticas_{y_m}.json'
    import json
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Estadísticas guardadas en {stats_file}")

if __name__ == '__main__':
    main()