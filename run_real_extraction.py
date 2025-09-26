#!/usr/bin/env python3
"""
Script principal para extraer datos reales de transparencia activa.
"""

import sys
import argparse
import logging
from pathlib import Path

# Agregar directorio padre al path
sys.path.append(str(Path(__file__).resolve().parent))

from etl.get_real_transparencia_urls import RealTransparenciaURLs
from etl.extract_real_data import RealDataExtractor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('real_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Extracción de datos reales de transparencia activa')
    parser.add_argument('--step', choices=['urls', 'extract', 'full'], 
                       default='full', help='Paso a ejecutar')
    parser.add_argument('--max-urls', type=int, 
                       help='Número máximo de URLs a procesar')
    parser.add_argument('--max-workers', type=int, default=8,
                       help='Número de workers paralelos')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Timeout en segundos')
    
    args = parser.parse_args()
    
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data' / 'processed'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    if args.step in ['urls', 'full']:
        logger.info("Paso 1: Obteniendo URLs reales de transparencia activa")
        
        url_getter = RealTransparenciaURLs()
        df_urls = url_getter.process_all_organismos()
        
        if df_urls.empty:
            logger.error("No se encontraron URLs. Terminando.")
            return
        
        logger.info(f"URLs obtenidas: {len(df_urls)}")
    
    if args.step in ['extract', 'full']:
        logger.info("Paso 2: Extrayendo datos reales")
        
        # Buscar archivo de URLs
        urls_file = data_dir / 'urls_transparencia_completas.csv'
        if not urls_file.exists():
            urls_file = data_dir / 'urls_transparencia_real.csv'
        
        if not urls_file.exists():
            logger.error(f"Archivo de URLs no encontrado: {urls_file}")
            return
        
        # Crear extractor
        extractor = RealDataExtractor(
            max_workers=args.max_workers,
            timeout=args.timeout,
            max_retries=3
        )
        
        # Ejecutar extracción
        extractor.run_extraction(urls_file, args.max_urls)
    
    logger.info("Proceso completado")

if __name__ == '__main__':
    main()


