#!/usr/bin/env python3
"""
Script principal para ejecutar las mejoras de datos:
1. Validación y corrección de datos municipales
2. Extracción de instituciones del Ministerio de Salud
"""

import sys
import subprocess
from pathlib import Path
import logging
import argparse

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_municipal_validation(input_file: str, output_file: str, apply_fixes: bool = True):
    """Ejecuta la validación y corrección de datos municipales."""
    logger.info("Iniciando validacion de datos municipales...")
    
    cmd = [
        sys.executable, 
        "etl/validate_municipal_data.py",
        "--input-file", input_file,
        "--output-file", output_file
    ]
    
    if apply_fixes:
        cmd.append("--apply-fixes")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("Validacion municipal completada exitosamente")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error en validacion municipal: {e}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False

def run_health_institutions_extraction(max_institutions: int = None):
    """Ejecuta la extracción de instituciones del Ministerio de Salud."""
    logger.info("Iniciando extraccion de instituciones del Ministerio de Salud...")
    
    cmd = [
        sys.executable,
        "etl/extract_health_institutions.py"
    ]
    
    if max_institutions:
        cmd.extend(["--max-institutions", str(max_institutions)])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("Extraccion de instituciones de salud completada")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error en extraccion de salud: {e}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False

def run_all_improvements():
    """Ejecuta todas las mejoras de datos."""
    logger.info("Iniciando proceso completo de mejoras de datos")
    
    # Configuración de archivos
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data' / 'processed'
    
    input_file = "datos_reales_consolidados.csv"
    municipal_output = "datos_municipales_corregidos.csv"
    
    success_count = 0
    total_tasks = 2
    
    # 1. Validación y corrección municipal
    if run_municipal_validation(input_file, municipal_output, apply_fixes=True):
        success_count += 1
        logger.info("Tarea 1/2 completada: Validacion municipal")
    else:
        logger.error("Tarea 1/2 fallo: Validacion municipal")
    
    # 2. Extracción de instituciones de salud (limitado para pruebas)
    if run_health_institutions_extraction(max_institutions=5):
        success_count += 1
        logger.info("Tarea 2/2 completada: Extraccion instituciones salud")
    else:
        logger.error("Tarea 2/2 fallo: Extraccion instituciones salud")
    
    # Resumen final
    logger.info("\n" + "="*80)
    logger.info("RESUMEN FINAL DE MEJORAS")
    logger.info("="*80)
    logger.info(f"Tareas completadas exitosamente: {success_count}/{total_tasks}")
    
    if success_count == total_tasks:
        logger.info("Todas las mejoras se completaron exitosamente!")
        logger.info("\nArchivos generados:")
        logger.info(f"  {data_dir / municipal_output}")
        logger.info(f"  {data_dir / 'datos_instituciones_salud.csv'}")
        logger.info(f"  {data_dir / 'reporte_instituciones_salud.json'}")
        
        logger.info("\nProximos pasos:")
        logger.info("  1. Revisar los archivos generados")
        logger.info("  2. Hacer commit de los cambios")
        logger.info("  3. Hacer merge con main")
        logger.info("  4. Subir a GitHub")
        
        return True
    else:
        logger.warning(f"Solo {success_count}/{total_tasks} tareas se completaron")
        return False

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Ejecutar mejoras de datos')
    parser.add_argument('--task', choices=['municipal', 'health', 'all'], 
                       default='all', help='Tarea específica a ejecutar')
    parser.add_argument('--input-file', type=str, 
                       default='datos_reales_consolidados.csv',
                       help='Archivo de entrada para validación municipal')
    parser.add_argument('--output-file', type=str,
                       default='datos_municipales_corregidos.csv',
                       help='Archivo de salida para datos corregidos')
    parser.add_argument('--max-institutions', type=int,
                       help='Número máximo de instituciones de salud a procesar')
    parser.add_argument('--no-fixes', action='store_true',
                       help='Solo validar, no aplicar correcciones')
    
    args = parser.parse_args()
    
    if args.task == 'municipal':
        success = run_municipal_validation(
            args.input_file, 
            args.output_file, 
            apply_fixes=not args.no_fixes
        )
    elif args.task == 'health':
        success = run_health_institutions_extraction(args.max_institutions)
    else:  # all
        success = run_all_improvements()
    
    if success:
        logger.info("Proceso completado exitosamente")
        sys.exit(0)
    else:
        logger.error("Proceso completado con errores")
        sys.exit(1)

if __name__ == '__main__':
    main()
