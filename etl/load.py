#!/usr/bin/env python3
"""
Carga el CSV consolidado de sueldos en una base SQLite con mejoras de robustez.

Lee `data/processed/sueldos_consolidado.csv` y carga los datos en la tabla `sueldos`
de `data/sueldos.db`. Incluye validaciones, índices y metadatos.
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
DB_PATH = BASE_DIR / 'data' / 'sueldos.db'

def create_database_schema(conn):
    """Crea el esquema de la base de datos con índices."""
    logger.info("Creando esquema de base de datos...")
    
    # Crear tabla principal
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sueldos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organismo TEXT,
            nombre TEXT,
            cargo TEXT,
            grado TEXT,
            estamento TEXT,
            sueldo_bruto REAL,
            fuente TEXT,
            archivo_origen TEXT,
            fecha_procesamiento TEXT,
            fecha_carga TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Crear índices para mejorar el rendimiento
    indices = [
        'CREATE INDEX IF NOT EXISTS idx_organismo ON sueldos(organismo)',
        'CREATE INDEX IF NOT EXISTS idx_estamento ON sueldos(estamento)',
        'CREATE INDEX IF NOT EXISTS idx_grado ON sueldos(grado)',
        'CREATE INDEX IF NOT EXISTS idx_sueldo_bruto ON sueldos(sueldo_bruto)',
        'CREATE INDEX IF NOT EXISTS idx_fuente ON sueldos(fuente)',
        'CREATE INDEX IF NOT EXISTS idx_fecha_carga ON sueldos(fecha_carga)'
    ]
    
    for index_sql in indices:
        conn.execute(index_sql)
    
    # Crear tabla de metadatos
    conn.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_carga TEXT,
            total_registros INTEGER,
            organismos_unicos INTEGER,
            estamentos_unicos INTEGER,
            grados_unicos INTEGER,
            fuentes_unicas INTEGER,
            promedio_sueldo REAL,
            mediana_sueldo REAL,
            min_sueldo REAL,
            max_sueldo REAL,
            desv_std REAL,
            archivo_origen TEXT
        )
    ''')
    
    conn.commit()
    logger.info("Esquema de base de datos creado exitosamente")

def validate_dataframe(df):
    """Valida el DataFrame antes de cargarlo."""
    logger.info("Validando DataFrame...")
    
    required_columns = ['organismo', 'sueldo_bruto']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Columnas requeridas faltantes: {missing_columns}")
    
    # Verificar que hay datos
    if len(df) == 0:
        raise ValueError("El DataFrame está vacío")
    
    # Verificar que hay sueldos válidos
    valid_salaries = df['sueldo_bruto'].dropna()
    if len(valid_salaries) == 0:
        raise ValueError("No hay sueldos válidos en el DataFrame")
    
    logger.info(f"DataFrame validado: {len(df)} registros, {len(valid_salaries)} con sueldos válidos")
    
    return True

def load_data_to_db(df, conn):
    """Carga los datos en la base de datos."""
    logger.info("Cargando datos en la base de datos...")
    
    # Agregar fecha de carga
    df['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Cargar datos
    df.to_sql('sueldos', conn, if_exists='replace', index=False)
    
    logger.info(f"Datos cargados exitosamente: {len(df)} registros")

def save_metadata(df, conn, csv_file):
    """Guarda metadatos de la carga."""
    logger.info("Guardando metadatos...")
    
    metadata = {
        'fecha_carga': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_registros': len(df),
        'organismos_unicos': df['organismo'].nunique(),
        'estamentos_unicos': df['estamento'].nunique() if 'estamento' in df.columns else 0,
        'grados_unicos': df['grado'].nunique() if 'grado' in df.columns else 0,
        'fuentes_unicas': df['fuente'].nunique() if 'fuente' in df.columns else 0,
        'promedio_sueldo': df['sueldo_bruto'].mean(),
        'mediana_sueldo': df['sueldo_bruto'].median(),
        'min_sueldo': df['sueldo_bruto'].min(),
        'max_sueldo': df['sueldo_bruto'].max(),
        'desv_std': df['sueldo_bruto'].std(),
        'archivo_origen': csv_file.name
    }
    
    # Insertar metadatos
    conn.execute('''
        INSERT INTO metadata (
            fecha_carga, total_registros, organismos_unicos, estamentos_unicos,
            grados_unicos, fuentes_unicas, promedio_sueldo, mediana_sueldo,
            min_sueldo, max_sueldo, desv_std, archivo_origen
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        metadata['fecha_carga'], metadata['total_registros'], metadata['organismos_unicos'],
        metadata['estamentos_unicos'], metadata['grados_unicos'], metadata['fuentes_unicas'],
        metadata['promedio_sueldo'], metadata['mediana_sueldo'], metadata['min_sueldo'],
        metadata['max_sueldo'], metadata['desv_std'], metadata['archivo_origen']
    ))
    
    conn.commit()
    
    # Guardar metadatos en archivo JSON también
    metadata_file = PROCESSED_DIR / 'metadata_carga.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Metadatos guardados en {metadata_file}")
    
    return metadata

def get_database_stats(conn):
    """Obtiene estadísticas de la base de datos."""
    stats = {}
    
    # Contar registros por fuente
    cursor = conn.execute('SELECT fuente, COUNT(*) as count FROM sueldos GROUP BY fuente')
    stats['por_fuente'] = dict(cursor.fetchall())
    
    # Contar registros por estamento
    cursor = conn.execute('SELECT estamento, COUNT(*) as count FROM sueldos GROUP BY estamento ORDER BY count DESC')
    stats['por_estamento'] = dict(cursor.fetchall())
    
    # Estadísticas generales
    cursor = conn.execute('SELECT COUNT(*) FROM sueldos')
    stats['total_registros'] = cursor.fetchone()[0]
    
    cursor = conn.execute('SELECT COUNT(DISTINCT organismo) FROM sueldos')
    stats['organismos_unicos'] = cursor.fetchone()[0]
    
    return stats

def main():
    csv_file = PROCESSED_DIR / 'sueldos_consolidado.csv'
    
    if not csv_file.exists():
        logger.error("No se encontró el archivo de sueldos consolidado. Ejecuta transform.py primero.")
        return
    
    logger.info(f"Iniciando carga de datos desde {csv_file}")
    
    try:
        # Leer CSV
        df = pd.read_csv(csv_file, encoding='utf-8')
        logger.info(f"CSV leído exitosamente: {len(df)} registros")
        
        # Validar datos
        validate_dataframe(df)
        
        # Conectar a base de datos
        conn = sqlite3.connect(DB_PATH)
        
        # Crear esquema
        create_database_schema(conn)
        
        # Cargar datos
        load_data_to_db(df, conn)
        
        # Guardar metadatos
        metadata = save_metadata(df, conn, csv_file)
        
        # Obtener estadísticas finales
        db_stats = get_database_stats(conn)
        
        # Cerrar conexión
        conn.close()
        
        # Mostrar resumen
        logger.info("=== RESUMEN DE CARGA ===")
        logger.info(f"Archivo procesado: {csv_file}")
        logger.info(f"Registros cargados: {metadata['total_registros']:,}")
        logger.info(f"Organismos únicos: {metadata['organismos_unicos']:,}")
        logger.info(f"Estamentos únicos: {metadata['estamentos_unicos']:,}")
        logger.info(f"Fuentes únicas: {metadata['fuentes_unicas']:,}")
        logger.info(f"Promedio sueldo: ${metadata['promedio_sueldo']:,.0f}")
        logger.info(f"Mediana sueldo: ${metadata['mediana_sueldo']:,.0f}")
        logger.info(f"Base de datos: {DB_PATH}")
        
        logger.info("Carga completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error durante la carga: {e}")
        raise

if __name__ == '__main__':
    main()