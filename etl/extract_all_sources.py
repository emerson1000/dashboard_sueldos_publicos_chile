#!/usr/bin/env python3
"""
Script maestro para extraer datos de todas las fuentes disponibles.
Ejecuta todos los extractores y consolida los resultados.
"""

import subprocess
import pandas as pd
import logging
from pathlib import Path
import time
import requests

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'

def ejecutar_extractor(script_name, description):
    """Ejecuta un script de extracción."""
    logger.info(f"Ejecutando {description}...")
    
    try:
        result = subprocess.run(
            ['python', f'etl/{script_name}'],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completado exitosamente")
            return True
        else:
            logger.error(f"❌ {description} falló: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {description} excedió el tiempo límite")
        return False
    except Exception as e:
        logger.error(f"💥 Error ejecutando {description}: {e}")
        return False

def consolidar_datos():
    """Consolida todos los datos extraídos."""
    logger.info("Consolidando datos de todas las fuentes...")
    
    y_m = time.strftime("%Y-%m")
    todos_datos = []
    
    # Directorios a buscar
    directorios = [
        DATA_RAW / 'sii' / y_m,
        DATA_RAW / 'dipres' / y_m,
        DATA_RAW / 'contraloria' / y_m,
        DATA_RAW / 'funcionarios_reales' / y_m,
        DATA_RAW / 'transparencia_activa' / y_m,
        DATA_RAW / 'organismos_especificos' / y_m,
        DATA_RAW / 'fuentes_alternativas' / y_m
    ]
    
    for directorio in directorios:
        if directorio.exists():
            # Buscar archivos CSV
            for archivo in directorio.glob('*.csv'):
                try:
                    df = pd.read_csv(archivo, encoding='utf-8')
                    logger.info(f"Procesando {archivo}: {len(df)} registros")
                    
                    # Agregar información de fuente
                    df['archivo_origen'] = archivo.name
                    df['directorio_fuente'] = directorio.name
                    
                    todos_datos.append(df)
                    
                except Exception as e:
                    logger.warning(f"Error procesando {archivo}: {e}")
    
    if todos_datos:
        # Consolidar todos los datos
        df_consolidado = pd.concat(todos_datos, ignore_index=True)
        
        # Guardar datos consolidados
        output_file = DATA_RAW / 'consolidado' / y_m / 'todos_los_datos.csv'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_consolidado.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"📊 Datos consolidados guardados en {output_file}")
        logger.info(f"📈 Total de registros: {len(df_consolidado):,}")
        
        # Mostrar resumen por fuente
        if 'fuente' in df_consolidado.columns:
            logger.info("📋 Distribución por fuente:")
            for fuente, count in df_consolidado['fuente'].value_counts().items():
                logger.info(f"  {fuente}: {count:,} registros")
        
        # Mostrar resumen por organismo
        if 'organismo' in df_consolidado.columns:
            logger.info("🏛️ Top 10 organismos:")
            for org, count in df_consolidado['organismo'].value_counts().head(10).items():
                logger.info(f"  {org}: {count:,} registros")
        
        # Mostrar estadísticas de sueldos
        if 'sueldo_bruto' in df_consolidado.columns:
            sueldos_validos = df_consolidado['sueldo_bruto'].dropna()
            if len(sueldos_validos) > 0:
                logger.info("💰 Estadísticas de sueldos:")
                logger.info(f"  Promedio: ${sueldos_validos.mean():,.0f}")
                logger.info(f"  Mediana: ${sueldos_validos.median():,.0f}")
                logger.info(f"  Mínimo: ${sueldos_validos.min():,.0f}")
                logger.info(f"  Máximo: ${sueldos_validos.max():,.0f}")
                logger.info(f"  Registros con sueldo: {len(sueldos_validos):,}")
        
        return df_consolidado
    else:
        logger.warning("No se encontraron datos para consolidar")
        return None

def main():
    """Función principal para extraer datos de todas las fuentes."""
    logger.info("🚀 Iniciando extracción de datos de todas las fuentes")
    
    # Lista de extractores a ejecutar
    extractores = [
        ('extract_dipres.py', 'Extracción DIPRES'),
        ('extract_sii.py', 'Extracción SII'),
        ('extract_contraloria.py', 'Extracción Contraloría'),
        ('extract_real_data.py', 'Extracción datos reales'),
        ('extract_transparencia_activa.py', 'Extracción transparencia activa'),
        ('extract_organismos_especificos.py', 'Extracción organismos específicos'),
        ('extract_fuentes_alternativas.py', 'Extracción fuentes alternativas')
    ]
    
    # Ejecutar extractores
    exitosos = 0
    for script, description in extractores:
        if ejecutar_extractor(script, description):
            exitosos += 1
        
        # Pausa entre extractores
        time.sleep(2)
    
    logger.info(f"📊 Extractores ejecutados: {exitosos}/{len(extractores)}")
    
    # Consolidar datos
    df_consolidado = consolidar_datos()
    
    if df_consolidado is not None:
        logger.info("✅ Extracción y consolidación completada exitosamente")
        
        # Mostrar resumen final
        logger.info("🎯 RESUMEN FINAL:")
        logger.info(f"  📈 Total de registros: {len(df_consolidado):,}")
        logger.info(f"  🏛️ Organismos únicos: {df_consolidado['organismo'].nunique() if 'organismo' in df_consolidado.columns else 'N/A'}")
        logger.info(f"  📋 Fuentes únicas: {df_consolidado['fuente'].nunique() if 'fuente' in df_consolidado.columns else 'N/A'}")
        
        if 'sueldo_bruto' in df_consolidado.columns:
            sueldos_validos = df_consolidado['sueldo_bruto'].dropna()
            if len(sueldos_validos) > 0:
                logger.info(f"  💰 Registros con sueldo: {len(sueldos_validos):,}")
                logger.info(f"  💰 Promedio sueldo: ${sueldos_validos.mean():,.0f}")
    else:
        logger.error("❌ No se pudieron consolidar los datos")

if __name__ == '__main__':
    main()
