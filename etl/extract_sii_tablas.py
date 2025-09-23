#!/usr/bin/env python3
"""
Extrae datos del SII usando tablas HTML directamente.
Basado en el script proporcionado por el usuario.
"""

import requests
import pandas as pd
from tqdm import tqdm
import time
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

YEARS = range(2020, 2025)  # Solo últimos años para prueba
MONTHS = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic']

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def fetch_table(url):
    """Descarga el HTML con headers para evitar 403."""
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        return tables[0] if tables else pd.DataFrame()
    except Exception as e:
        logger.warning(f"Error accediendo a {url}: {e}")
        return pd.DataFrame()

def dump_sii(tipo, outfile):
    """Extrae datos del SII por tipo (honorarios o planta)."""
    logger.info(f"🚀 Extrayendo datos de {tipo} del SII")
    
    all_rows = []
    total_combinations = len(YEARS) * len(MONTHS)
    
    with tqdm(total=total_combinations, desc=f"Extrayendo {tipo}") as pbar:
        for year in YEARS:
            for mon in MONTHS:
                page = f"http://www.sii.cl/transparencia/{year}/per_{tipo}_{mon}.html"
                try:
                    df = fetch_table(page)
                    if not df.empty:
                        df['anio'] = year
                        df['mes'] = mon
                        df['tipo'] = tipo
                        df['fuente'] = 'sii_tablas_html'
                        all_rows.append(df)
                        logger.info(f"✅ {year}-{mon}: {len(df)} registros")
                    else:
                        logger.debug(f"❌ {year}-{mon}: Sin datos")
                except Exception as e:
                    # algunas combinaciones no existen; se ignoran
                    logger.debug(f"❌ {year}-{mon}: Error - {e}")
                    continue
                
                pbar.update(1)
                time.sleep(0.5)  # Pausa para no sobrecargar el servidor
    
    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        combined.to_csv(outfile, index=False, encoding='utf-8')
        logger.info(f"✅ Datos de {tipo} guardados en {outfile}")
        logger.info(f"📊 Total registros: {len(combined)}")
        return combined
    else:
        logger.warning(f"⚠️ No se encontraron datos para {tipo}")
        return pd.DataFrame()

def procesar_datos_sii(df, tipo):
    """Procesa y normaliza los datos del SII."""
    if df.empty:
        return df
    
    logger.info(f"⚙️ Procesando datos de {tipo}")
    
    # Crear copia
    df_proc = df.copy()
    
    # Mapear columnas comunes
    column_mapping = {
        'RUT': 'rut',
        'Nombre': 'nombre',
        'Apellido Paterno': 'apellido_paterno',
        'Apellido Materno': 'apellido_materno',
        'Cargo': 'cargo',
        'Grado': 'grado',
        'Estamento': 'estamento',
        'Remuneración Bruta': 'sueldo_bruto',
        'Remuneración Bruta Mensualizada': 'sueldo_bruto',
        'Sueldo Base': 'sueldo_base',
        'Bonificaciones': 'bonificaciones'
    }
    
    # Renombrar columnas
    for old_col, new_col in column_mapping.items():
        if old_col in df_proc.columns:
            df_proc[new_col] = df_proc[old_col]
    
    # Crear nombre completo
    if 'nombre' in df_proc.columns and 'apellido_paterno' in df_proc.columns:
        df_proc['nombre_completo'] = df_proc['nombre'].fillna('') + ' ' + df_proc['apellido_paterno'].fillna('')
        if 'apellido_materno' in df_proc.columns:
            df_proc['nombre_completo'] += ' ' + df_proc['apellido_materno'].fillna('')
        df_proc['nombre_completo'] = df_proc['nombre_completo'].str.strip()
    
    # Limpiar sueldo
    if 'sueldo_bruto' in df_proc.columns:
        df_proc['sueldo_bruto'] = pd.to_numeric(df_proc['sueldo_bruto'], errors='coerce')
    
    # Agregar metadatos
    df_proc['organismo'] = 'Servicio de Impuestos Internos'
    df_proc['url_origen'] = f'http://www.sii.cl/transparencia/{{anio}}/per_{tipo}_{{mes}}.html'
    
    # Limpiar datos
    df_proc = df_proc.fillna('Sin especificar')
    
    logger.info(f"✅ Procesados {len(df_proc)} registros de {tipo}")
    
    return df_proc

def main():
    """Función principal."""
    logger.info("🚀 Iniciando extracción completa del SII")
    
    y_m = time.strftime("%Y-%m")
    dest_dir = BASE_DIR / 'data' / 'raw' / 'sii_tablas' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Extraer datos
    honorarios_file = dest_dir / 'honorarios_sii.csv'
    planta_file = dest_dir / 'planta_sii.csv'
    
    # Extraer honorarios
    df_honorarios = dump_sii('honorarios', honorarios_file)
    df_honorarios_proc = procesar_datos_sii(df_honorarios, 'honorarios')
    
    # Extraer planta
    df_planta = dump_sii('planta', planta_file)
    df_planta_proc = procesar_datos_sii(df_planta, 'planta')
    
    # Combinar datos
    if not df_honorarios_proc.empty and not df_planta_proc.empty:
        df_combined = pd.concat([df_honorarios_proc, df_planta_proc], ignore_index=True)
    elif not df_honorarios_proc.empty:
        df_combined = df_honorarios_proc
    elif not df_planta_proc.empty:
        df_combined = df_planta_proc
    else:
        logger.warning("⚠️ No se obtuvieron datos del SII")
        return
    
    # Guardar datos combinados
    combined_file = dest_dir / 'sii_combinado.csv'
    df_combined.to_csv(combined_file, index=False, encoding='utf-8')
    
    logger.info(f"✅ Datos combinados del SII guardados en {combined_file}")
    logger.info(f"📊 Total registros: {len(df_combined)}")
    logger.info(f"📅 Años: {df_combined['anio'].min()} - {df_combined['anio'].max()}")
    logger.info(f"🏛️ Organismo: {df_combined['organismo'].iloc[0]}")
    
    # Estadísticas por tipo
    if 'tipo' in df_combined.columns:
        stats_tipo = df_combined['tipo'].value_counts()
        logger.info("📈 Registros por tipo:")
        for tipo, count in stats_tipo.items():
            logger.info(f"  {tipo}: {count} registros")

if __name__ == '__main__':
    main()
