#!/usr/bin/env python3
"""
Script principal para ejecutar extracción de transparencia activa de manera inteligente.
Maneja lotes, reintentos y monitoreo en tiempo real.
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
import logging
import signal
import json

# Agregar directorio padre al path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from etl.extract_transparencia_activa_robusto import TransparenciaActivaExtractor
from etl.config_extractor import ExtractorConfig, ProgressMonitor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction_run.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ExtractionRunner:
    """Ejecutor inteligente de extracción."""
    
    def __init__(self):
        self.config = ExtractorConfig()
        self.monitor = ProgressMonitor(self.config.progress_db)
        self.extractor = None
        self.running = True
        
        # Configurar manejo de señales para interrupción elegante
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Maneja señales de interrupción."""
        logger.info("Recibida señal de interrupción. Deteniendo extracción...")
        self.running = False
    
    def run_batch_extraction(self, batch_size: int = 50, max_batches: int = None):
        """Ejecuta extracción por lotes."""
        logger.info(f"Iniciando extracción por lotes (tamaño: {batch_size})")
        
        # Obtener lista de organismos
        organismos = self.extractor.organismos_base
        
        # Filtrar organismos ya procesados exitosamente
        processed_organismos = self._get_processed_organismos()
        remaining_organismos = [org for org in organismos if org['nombre'] not in processed_organismos]
        
        logger.info(f"Organismos restantes por procesar: {len(remaining_organismos)}")
        
        if not remaining_organismos:
            logger.info("Todos los organismos ya han sido procesados exitosamente")
            return
        
        # Dividir en lotes
        batches = [remaining_organismos[i:i + batch_size] 
                  for i in range(0, len(remaining_organismos), batch_size)]
        
        if max_batches:
            batches = batches[:max_batches]
        
        logger.info(f"Procesando {len(batches)} lotes")
        
        # Procesar cada lote
        for batch_num, batch in enumerate(batches, 1):
            if not self.running:
                logger.info("Extracción interrumpida por usuario")
                break
            
            logger.info(f"Procesando lote {batch_num}/{len(batches)} ({len(batch)} organismos)")
            
            # Iniciar sesión
            session_id = self.monitor.start_session(len(batch))
            
            try:
                # Procesar lote
                batch_data = self._process_batch(batch)
                
                # Finalizar sesión
                self.monitor.end_session(session_id, 'completed')
                
                logger.info(f"Lote {batch_num} completado. Datos extraídos: {len(batch_data)}")
                
                # Pausa entre lotes
                if batch_num < len(batches):
                    logger.info("Pausa de 30 segundos entre lotes...")
                    time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error procesando lote {batch_num}: {e}")
                self.monitor.end_session(session_id, 'error')
                
                if not self.running:
                    break
                
                # Pausa más larga en caso de error
                logger.info("Pausa de 60 segundos por error...")
                time.sleep(60)
        
        # Generar reporte final
        self._generate_final_report()
    
    def _get_processed_organismos(self) -> set:
        """Obtiene organismos ya procesados exitosamente."""
        conn = self.monitor.db_path
        import sqlite3
        
        conn = sqlite3.connect(conn)
        cursor = conn.cursor()
        
        cursor.execute('SELECT organismo FROM extraction_progress WHERE status = "success"')
        processed = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        return processed
    
    def _process_batch(self, batch: list) -> list:
        """Procesa un lote de organismos."""
        batch_data = []
        
        for organismo_info in batch:
            if not self.running:
                break
            
            organismo = organismo_info['nombre']
            logger.info(f"Procesando {organismo}...")
            
            start_time = time.time()
            
            try:
                # Extraer datos del organismo
                extracted_data = self.extractor.extract_organismo(organismo_info)
                
                processing_time = time.time() - start_time
                
                if extracted_data:
                    batch_data.extend(extracted_data)
                    logger.info(f"SUCCESS {organismo}: {len(extracted_data)} registros ({processing_time:.1f}s)")
                else:
                    logger.info(f"NO DATA {organismo}: Sin datos encontrados")
                
                # Pausa entre organismos
                time.sleep(self.config.get_config('delay_between_requests', 2))
                
            except Exception as e:
                logger.error(f"❌ Error en {organismo}: {e}")
                continue
        
        return batch_data
    
    def _generate_final_report(self):
        """Genera reporte final."""
        logger.info("Generando reporte final...")
        
        summary = self.monitor.get_progress_summary()
        
        report = {
            'fecha_reporte': datetime.now().isoformat(),
            'resumen': summary,
            'top_organismos': self.monitor.get_top_organismos(20),
            'organismos_fallidos': self.monitor.get_failed_organismos()[:10]
        }
        
        # Guardar reporte
        report_file = self.config.base_dir / 'data' / 'processed' / 'reporte_final_extraction.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Exportar datos a CSV
        csv_file = self.config.base_dir / 'data' / 'processed' / 'datos_extraidos_final.csv'
        self.monitor.export_data_to_csv(csv_file)
        
        logger.info(f"Reporte guardado en {report_file}")
        logger.info(f"Datos exportados a {csv_file}")
        
        # Mostrar resumen en consola
        self._print_summary(summary)
    
    def _print_summary(self, summary: dict):
        """Imprime resumen en consola."""
        print("\n" + "="*60)
        print("📊 RESUMEN FINAL DE EXTRACCIÓN")
        print("="*60)
        print(f"Total organismos procesados: {summary['total_organismos']}")
        print(f"Exitosos: {summary['successful']}")
        print(f"Fallidos: {summary['failed']}")
        print(f"Sin datos: {summary['no_data']}")
        print(f"Tasa de éxito: {summary['success_rate']:.1f}%")
        print(f"Total datos extraídos: {summary['total_data_extracted']:,}")
        print("="*60)
    
    def run_retry_failed(self):
        """Reintenta organismos que fallaron."""
        logger.info("Reintentando organismos fallidos...")
        
        failed_organismos = self.monitor.get_failed_organismos()
        
        if not failed_organismos:
            logger.info("No hay organismos fallidos para reintentar")
            return
        
        logger.info(f"Reintentando {len(failed_organismos)} organismos fallidos")
        
        # Obtener información completa de organismos fallidos
        organismos_info = []
        for failed in failed_organismos:
            for org in self.extractor.organismos_base:
                if org['nombre'] == failed['organismo']:
                    organismos_info.append(org)
                    break
        
        # Procesar organismos fallidos
        for organismo_info in organismos_info:
            if not self.running:
                break
            
            organismo = organismo_info['nombre']
            logger.info(f"Reintentando {organismo}...")
            
            try:
                self.extractor.extract_organismo(organismo_info)
                time.sleep(5)  # Pausa más larga para reintentos
            except Exception as e:
                logger.error(f"Error reintentando {organismo}: {e}")
                continue
    
    def run_continuous(self, batch_size: int = 50, sleep_hours: int = 6):
        """Ejecuta extracción continua."""
        logger.info(f"Iniciando extracción continua (lotes de {batch_size}, pausa de {sleep_hours}h)")
        
        while self.running:
            try:
                # Ejecutar lote
                self.run_batch_extraction(batch_size, max_batches=1)
                
                if not self.running:
                    break
                
                # Pausa larga
                logger.info(f"Pausa de {sleep_hours} horas hasta próxima extracción...")
                time.sleep(sleep_hours * 3600)
                
            except KeyboardInterrupt:
                logger.info("Extracción continua interrumpida por usuario")
                break
            except Exception as e:
                logger.error(f"Error en extracción continua: {e}")
                time.sleep(300)  # Pausa de 5 minutos en caso de error

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Extractor de Transparencia Activa')
    parser.add_argument('--mode', choices=['batch', 'retry', 'continuous'], 
                       default='batch', help='Modo de ejecución')
    parser.add_argument('--batch-size', type=int, default=50, 
                       help='Tamaño del lote')
    parser.add_argument('--max-batches', type=int, 
                       help='Número máximo de lotes')
    parser.add_argument('--sleep-hours', type=int, default=6,
                       help='Horas de pausa en modo continuo')
    parser.add_argument('--max-workers', type=int, default=8,
                       help='Número de workers paralelos')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Timeout en segundos')
    
    args = parser.parse_args()
    
    # Crear runner
    runner = ExtractionRunner()
    
    # Configurar extractor
    runner.extractor = TransparenciaActivaExtractor(
        max_workers=args.max_workers,
        timeout=args.timeout,
        max_retries=3
    )
    
    # Ejecutar según modo
    if args.mode == 'batch':
        runner.run_batch_extraction(args.batch_size, args.max_batches)
    elif args.mode == 'retry':
        runner.run_retry_failed()
    elif args.mode == 'continuous':
        runner.run_continuous(args.batch_size, args.sleep_hours)

if __name__ == '__main__':
    main()
