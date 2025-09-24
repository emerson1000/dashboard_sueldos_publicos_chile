#!/usr/bin/env python3
"""
Script de inicio rápido para extracción de transparencia activa.
"""

import sys
from pathlib import Path
import argparse
import logging

# Agregar directorio padre al path
sys.path.append(str(Path(__file__).resolve().parent))

from etl.run_extraction import ExtractionRunner
from etl.config_extractor import ExtractorConfig, ProgressMonitor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Inicio rápido de extracción')
    parser.add_argument('--test', action='store_true', 
                       help='Ejecutar prueba con pocos organismos')
    parser.add_argument('--monitor', action='store_true',
                       help='Solo mostrar estado actual')
    parser.add_argument('--validate', action='store_true',
                       help='Validar datos existentes')
    
    args = parser.parse_args()
    
    if args.monitor:
        # Solo mostrar estado
        config = ExtractorConfig()
        monitor = ProgressMonitor(config.progress_db)
        
        summary = monitor.get_progress_summary()
        
        print("📊 ESTADO ACTUAL DE EXTRACCIÓN")
        print("=" * 50)
        print(f"Total organismos procesados: {summary['total_organismos']}")
        print(f"Exitosos: {summary['successful']}")
        print(f"Fallidos: {summary['failed']}")
        print(f"Sin datos: {summary['no_data']}")
        print(f"Tasa de éxito: {summary['success_rate']:.1f}%")
        print(f"Total datos extraídos: {summary['total_data_extracted']:,}")
        
        if summary['total_data_extracted'] > 0:
            print("\n🏆 TOP ORGANISMOS")
            print("-" * 30)
            top = monitor.get_top_organismos(5)
            for i, org in enumerate(top, 1):
                print(f"{i}. {org['organismo']}")
                print(f"   📊 {org['count']} registros")
                print(f"   💰 ${org['avg_sueldo']:,.0f} promedio")
        
        return
    
    if args.validate:
        # Validar datos existentes
        from etl.validate_and_clean_data import main as validate_main
        validate_main()
        return
    
    # Crear runner
    runner = ExtractionRunner()
    
    if args.test:
        # Modo prueba con pocos organismos
        logger.info("Iniciando prueba con 10 organismos")
        from etl.extract_transparencia_activa_robusto import TransparenciaActivaExtractor
        runner.extractor = TransparenciaActivaExtractor(
            max_workers=4,
            timeout=20,
            max_retries=2
        )
        runner.run_batch_extraction(batch_size=10, max_batches=1)
    else:
        # Modo normal
        logger.info("Iniciando extracción completa")
        from etl.extract_transparencia_activa_robusto import TransparenciaActivaExtractor
        runner.extractor = TransparenciaActivaExtractor(
            max_workers=8,
            timeout=30,
            max_retries=3
        )
        runner.run_batch_extraction(batch_size=50)

if __name__ == '__main__':
    main()
