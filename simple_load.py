#!/usr/bin/env python3
"""
Script simple para cargar datos en la base de datos.
"""

import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'data' / 'sueldos.db'
CSV_FILE = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado.csv'

def main():
    print(f"Cargando datos desde {CSV_FILE}")
    
    # Leer CSV
    df = pd.read_csv(CSV_FILE, encoding='utf-8')
    print(f"CSV leído: {len(df)} registros")
    
    # Conectar a base de datos
    conn = sqlite3.connect(DB_PATH)
    
    # Crear tabla simple
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
    
    # Cargar datos
    df.to_sql('sueldos', conn, if_exists='replace', index=False)
    
    # Verificar datos
    cursor = conn.execute('SELECT COUNT(*) FROM sueldos')
    count = cursor.fetchone()[0]
    print(f"Datos cargados: {count} registros")
    
    # Mostrar algunos datos
    cursor = conn.execute('SELECT estamento, sueldo_bruto FROM sueldos LIMIT 5')
    for row in cursor.fetchall():
        print(f"Estamento: {row[0]}, Sueldo: ${row[1]:,.0f}")
    
    conn.close()
    print("Carga completada exitosamente")

if __name__ == '__main__':
    main()
