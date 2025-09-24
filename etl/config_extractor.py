#!/usr/bin/env python3
"""
Configuración y monitoreo para el extractor de transparencia activa.
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ExtractorConfig:
    """Configuración del extractor."""
    
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.config_file = self.base_dir / 'data' / 'processed' / 'extractor_config.json'
        self.progress_db = self.base_dir / 'data' / 'processed' / 'extraction_progress.db'
        
        # Configuración por defecto
        self.default_config = {
            'max_workers': 8,
            'timeout': 30,
            'max_retries': 3,
            'delay_between_requests': 2,
            'max_organismos_per_batch': 50,
            'max_links_per_organismo': 5,
            'min_sueldo_threshold': 100000,
            'user_agents': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ],
            'remuneracion_keywords': [
                'remuneración', 'sueldo', 'salario', 'bruto', 'líquido', 'monto',
                'honorario', 'asignación', 'bonificación', 'gratificación'
            ],
            'excluded_domains': [
                'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
                'youtube.com', 'google.com', 'bing.com'
            ]
        }
        
        self.load_config()
    
    def load_config(self):
        """Carga configuración desde archivo."""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
                logger.info("Configuración cargada desde archivo")
            except Exception as e:
                logger.warning(f"Error cargando configuración: {e}")
                self.config = self.default_config.copy()
        else:
            self.config = self.default_config.copy()
            self.save_config()
    
    def save_config(self):
        """Guarda configuración en archivo."""
        try:
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            logger.info("Configuración guardada")
        except Exception as e:
            logger.error(f"Error guardando configuración: {e}")
    
    def get_config(self, key: str, default=None):
        """Obtiene valor de configuración."""
        return self.config.get(key, default)
    
    def set_config(self, key: str, value):
        """Establece valor de configuración."""
        self.config[key] = value
        self.save_config()

class ProgressMonitor:
    """Monitor de progreso de extracción."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Configura base de datos de progreso."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabla de progreso por organismo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_progress (
                organismo TEXT PRIMARY KEY,
                url TEXT,
                status TEXT,
                last_attempt TIMESTAMP,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                last_error TEXT,
                data_count INTEGER DEFAULT 0,
                processing_time REAL DEFAULT 0
            )
        ''')
        
        # Tabla de datos extraídos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extracted_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                organismo TEXT,
                nombre TEXT,
                cargo TEXT,
                estamento TEXT,
                sueldo_bruto REAL,
                fuente TEXT,
                url_origen TEXT,
                fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Crear índices por separado
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_organismo ON extracted_data(organismo)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fecha_extraccion ON extracted_data(fecha_extraccion)')
        
        # Tabla de sesiones de extracción
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_organismos INTEGER,
                successful_organismos INTEGER,
                failed_organismos INTEGER,
                total_data_extracted INTEGER,
                status TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def start_session(self, total_organismos: int) -> int:
        """Inicia nueva sesión de extracción."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO extraction_sessions 
            (start_time, total_organismos, status)
            VALUES (?, ?, 'running')
        ''', (datetime.now(), total_organismos))
        
        session_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return session_id
    
    def end_session(self, session_id: int, status: str = 'completed'):
        """Finaliza sesión de extracción."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Obtener estadísticas
        cursor.execute('SELECT COUNT(*) FROM extraction_progress WHERE status = "success"')
        successful = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM extraction_progress WHERE status = "error"')
        failed = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM extracted_data')
        total_data = cursor.fetchone()[0]
        
        cursor.execute('''
            UPDATE extraction_sessions 
            SET end_time = ?, successful_organismos = ?, failed_organismos = ?, 
                total_data_extracted = ?, status = ?
            WHERE id = ?
        ''', (datetime.now(), successful, failed, total_data, status, session_id))
        
        conn.commit()
        conn.close()
    
    def update_organismo_progress(self, organismo: str, status: str, data_count: int = 0, 
                                error: str = None, processing_time: float = 0):
        """Actualiza progreso de un organismo."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO extraction_progress 
            (organismo, status, last_attempt, success_count, error_count, 
             last_error, data_count, processing_time)
            VALUES (?, ?, ?, 
                COALESCE((SELECT success_count FROM extraction_progress WHERE organismo = ?), 0) + ?,
                COALESCE((SELECT error_count FROM extraction_progress WHERE organismo = ?), 0) + ?,
                ?, ?, ?)
        ''', (organismo, status, datetime.now(), organismo, 1 if status == 'success' else 0,
              organismo, 1 if status == 'error' else 0, error, data_count, processing_time))
        
        conn.commit()
        conn.close()
    
    def get_progress_summary(self) -> dict:
        """Obtiene resumen del progreso."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Estadísticas generales
        cursor.execute('SELECT COUNT(*) FROM extraction_progress')
        total_organismos = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM extraction_progress WHERE status = "success"')
        successful = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM extraction_progress WHERE status = "error"')
        failed = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM extraction_progress WHERE status = "no_data"')
        no_data = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM extracted_data')
        total_data = cursor.fetchone()[0]
        
        # Última sesión
        cursor.execute('''
            SELECT * FROM extraction_sessions 
            ORDER BY start_time DESC LIMIT 1
        ''')
        last_session = cursor.fetchone()
        
        conn.close()
        
        return {
            'total_organismos': total_organismos,
            'successful': successful,
            'failed': failed,
            'no_data': no_data,
            'total_data_extracted': total_data,
            'success_rate': (successful / total_organismos * 100) if total_organismos > 0 else 0,
            'last_session': last_session
        }
    
    def get_failed_organismos(self) -> list:
        """Obtiene lista de organismos que fallaron."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT organismo, last_error, error_count 
            FROM extraction_progress 
            WHERE status = "error"
            ORDER BY error_count DESC
        ''')
        
        failed = cursor.fetchall()
        conn.close()
        
        return [{'organismo': org, 'error': error, 'count': count} for org, error, count in failed]
    
    def get_top_organismos(self, limit: int = 10) -> list:
        """Obtiene top organismos por cantidad de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT organismo, COUNT(*) as count, AVG(sueldo_bruto) as avg_sueldo
            FROM extracted_data 
            GROUP BY organismo 
            ORDER BY count DESC 
            LIMIT ?
        ''', (limit,))
        
        top = cursor.fetchall()
        conn.close()
        
        return [{'organismo': org, 'count': count, 'avg_sueldo': avg} for org, count, avg in top]
    
    def export_data_to_csv(self, output_file: Path):
        """Exporta datos extraídos a CSV."""
        conn = sqlite3.connect(self.db_path)
        
        df = pd.read_sql_query('SELECT * FROM extracted_data', conn)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        conn.close()
        logger.info(f"Datos exportados a {output_file}")

def main():
    """Función principal para monitoreo."""
    config = ExtractorConfig()
    monitor = ProgressMonitor(config.progress_db)
    
    # Mostrar resumen
    summary = monitor.get_progress_summary()
    
    print("📊 RESUMEN DE EXTRACCIÓN")
    print("=" * 50)
    print(f"Total organismos procesados: {summary['total_organismos']}")
    print(f"Exitosos: {summary['successful']}")
    print(f"Fallidos: {summary['failed']}")
    print(f"Sin datos: {summary['no_data']}")
    print(f"Tasa de éxito: {summary['success_rate']:.1f}%")
    print(f"Total datos extraídos: {summary['total_data_extracted']}")
    
    # Mostrar top organismos
    print("\n🏆 TOP ORGANISMOS POR DATOS")
    print("-" * 50)
    top = monitor.get_top_organismos(10)
    for i, org in enumerate(top, 1):
        print(f"{i:2d}. {org['organismo']}")
        print(f"    📊 {org['count']} registros")
        print(f"    💰 ${org['avg_sueldo']:,.0f} promedio")
        print()
    
    # Mostrar organismos fallidos
    print("❌ ORGANISMOS CON ERRORES")
    print("-" * 50)
    failed = monitor.get_failed_organismos()
    for org in failed[:5]:
        print(f"🔴 {org['organismo']}")
        print(f"   Error: {org['error']}")
        print(f"   Intentos: {org['count']}")
        print()

if __name__ == '__main__':
    main()
