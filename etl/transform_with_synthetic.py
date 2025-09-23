#!/usr/bin/env python3
"""
Script de transformación que incluye datos sintéticos realistas.
Combina datos reales del SII con datos sintéticos generados.
"""

import pandas as pd
import time
import logging
from pathlib import Path
from datetime import datetime
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'

def cargar_datos_sii():
    """Carga datos reales del SII."""
    logger.info("Cargando datos del SII...")
    
    y_m = time.strftime("%Y-%m")
    sii_file = DATA_RAW / 'sii' / y_m / 'escala.csv'
    
    if not sii_file.exists():
        logger.warning("No se encontró archivo del SII")
        return pd.DataFrame()
    
    df = pd.read_csv(sii_file)
    
    # Procesar datos del SII
    datos_sii = []
    for _, row in df.iterrows():
        sueldo_str = str(row['Remuneracion Bruta Mensualizada'])
        sueldo_limpio = sueldo_str.replace('.', '').replace(',', '.')
        
        try:
            sueldo_num = float(sueldo_limpio)
            if sueldo_num > 100000:  # Sueldo mínimo razonable
                datos_sii.append({
                    'organismo': 'SII - Escala Oficial',
                    'nombre': None,
                    'cargo': f"{row['Estamento']} Grado {row['Grado']}",
                    'grado': row['Grado'],
                    'estamento': row['Estamento'],
                    'sueldo_bruto': sueldo_num,
                    'fuente': 'sii',
                    'archivo_origen': 'escala.csv',
                    'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        except:
            continue
    
    logger.info(f"Datos del SII procesados: {len(datos_sii)} registros")
    return pd.DataFrame(datos_sii)

def cargar_datos_sinteticos():
    """Carga datos sintéticos generados."""
    logger.info("Cargando datos sintéticos...")
    
    y_m = time.strftime("%Y-%m")
    sinteticos_file = DATA_RAW / 'sinteticos' / y_m / 'funcionarios_sinteticos.csv'
    
    if not sinteticos_file.exists():
        logger.warning("No se encontraron datos sintéticos")
        return pd.DataFrame()
    
    df = pd.read_csv(sinteticos_file)
    logger.info(f"Datos sintéticos cargados: {len(df)} registros")
    
    return df

def consolidar_datos():
    """Consolida todos los datos disponibles."""
    logger.info("Consolidando todos los datos...")
    
    # Cargar datos del SII
    df_sii = cargar_datos_sii()
    
    # Cargar datos sintéticos
    df_sinteticos = cargar_datos_sinteticos()
    
    # Combinar datos
    if not df_sii.empty and not df_sinteticos.empty:
        df_consolidado = pd.concat([df_sii, df_sinteticos], ignore_index=True)
    elif not df_sii.empty:
        df_consolidado = df_sii
    elif not df_sinteticos.empty:
        df_consolidado = df_sinteticos
    else:
        logger.error("No hay datos para consolidar")
        return pd.DataFrame()
    
    logger.info(f"Total de datos consolidados: {len(df_consolidado)} registros")
    
    return df_consolidado

def validar_datos(df):
    """Valida y limpia los datos consolidados."""
    logger.info("Validando datos consolidados...")
    
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

def generar_estadisticas(df):
    """Genera estadísticas de los datos procesados."""
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
    """Función principal."""
    y_m = time.strftime("%Y-%m")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Iniciando transformación de datos para {y_m}")
    
    try:
        # Consolidar datos
        df_consolidado = consolidar_datos()
        
        if df_consolidado.empty:
            logger.error("No hay datos para procesar")
            return
        
        # Validar datos
        df_validado = validar_datos(df_consolidado)
        
        # Generar estadísticas
        stats = generar_estadisticas(df_validado)
        
        # Guardar datos procesados
        output_file = PROCESSED_DIR / f'sueldos_{y_m}.csv'
        df_validado.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Datos transformados y guardados en {output_file}")
        
        # Actualizar archivo consolidado
        consolidado_file = PROCESSED_DIR / 'sueldos_consolidado.csv'
        df_validado.to_csv(consolidado_file, index=False, encoding='utf-8')
        logger.info(f"Archivo consolidado actualizado: {consolidado_file}")
        
        # Guardar estadísticas
        stats_file = PROCESSED_DIR / f'estadisticas_{y_m}.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logger.info(f"Estadísticas guardadas en {stats_file}")
        
        logger.info("Transformación completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error durante la transformación: {e}")
        raise

if __name__ == '__main__':
    main()
