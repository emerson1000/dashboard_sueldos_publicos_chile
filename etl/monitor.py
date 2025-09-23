#!/usr/bin/env python3
"""
Sistema de monitoreo para el pipeline ETL de transparencia salarial.

Proporciona funciones para monitorear el estado del pipeline, verificar la calidad
de los datos y generar alertas cuando sea necesario.
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import json
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'data' / 'sueldos.db'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
LOG_DIR = BASE_DIR / 'logs'

# Crear directorio de logs si no existe
LOG_DIR.mkdir(exist_ok=True)

class ETLMonitor:
    """Monitor del pipeline ETL."""
    
    def __init__(self):
        self.db_path = DB_PATH
        self.processed_dir = PROCESSED_DIR
        self.log_dir = LOG_DIR
        
    def check_data_freshness(self, hours_threshold=24):
        """Verifica si los datos están actualizados."""
        logger.info("Verificando frescura de los datos...")
        
        if not self.db_path.exists():
            return False, "Base de datos no existe"
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute('SELECT MAX(fecha_carga) FROM metadata')
            last_update = cursor.fetchone()[0]
            conn.close()
            
            if not last_update:
                return False, "No hay registros de actualización"
            
            last_update_dt = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
            hours_since_update = (datetime.now() - last_update_dt).total_seconds() / 3600
            
            if hours_since_update > hours_threshold:
                return False, f"Datos desactualizados: {hours_since_update:.1f} horas desde la última actualización"
            
            return True, f"Datos actualizados: {hours_since_update:.1f} horas desde la última actualización"
            
        except Exception as e:
            return False, f"Error verificando frescura: {e}"
    
    def check_data_quality(self):
        """Verifica la calidad de los datos."""
        logger.info("Verificando calidad de los datos...")
        
        if not self.db_path.exists():
            return False, "Base de datos no existe"
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Verificar que hay datos
            cursor = conn.execute('SELECT COUNT(*) FROM sueldos')
            total_records = cursor.fetchone()[0]
            
            if total_records == 0:
                return False, "No hay registros en la base de datos"
            
            # Verificar sueldos válidos
            cursor = conn.execute('SELECT COUNT(*) FROM sueldos WHERE sueldo_bruto IS NOT NULL AND sueldo_bruto > 0')
            valid_salaries = cursor.fetchone()[0]
            
            if valid_salaries == 0:
                return False, "No hay sueldos válidos"
            
            # Verificar organismos
            cursor = conn.execute('SELECT COUNT(DISTINCT organismo) FROM sueldos WHERE organismo IS NOT NULL')
            unique_orgs = cursor.fetchone()[0]
            
            if unique_orgs == 0:
                return False, "No hay organismos válidos"
            
            # Verificar fuentes
            cursor = conn.execute('SELECT COUNT(DISTINCT fuente) FROM sueldos WHERE fuente IS NOT NULL')
            unique_sources = cursor.fetchone()[0]
            
            if unique_sources == 0:
                return False, "No hay fuentes válidas"
            
            conn.close()
            
            quality_score = (valid_salaries / total_records) * 100
            
            return True, f"Calidad de datos: {quality_score:.1f}% ({valid_salaries}/{total_records} sueldos válidos, {unique_orgs} organismos, {unique_sources} fuentes)"
            
        except Exception as e:
            return False, f"Error verificando calidad: {e}"
    
    def check_file_sizes(self):
        """Verifica el tamaño de los archivos de datos."""
        logger.info("Verificando tamaños de archivos...")
        
        issues = []
        
        # Verificar archivo consolidado
        csv_file = self.processed_dir / 'sueldos_consolidado.csv'
        if csv_file.exists():
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            if size_mb < 0.1:  # Menos de 100KB
                issues.append(f"Archivo consolidado muy pequeño: {size_mb:.2f} MB")
        else:
            issues.append("Archivo consolidado no existe")
        
        # Verificar base de datos
        if self.db_path.exists():
            size_mb = self.db_path.stat().st_size / (1024 * 1024)
            if size_mb < 0.1:  # Menos de 100KB
                issues.append(f"Base de datos muy pequeña: {size_mb:.2f} MB")
        else:
            issues.append("Base de datos no existe")
        
        if issues:
            return False, "; ".join(issues)
        
        return True, "Tamaños de archivos normales"
    
    def check_data_distribution(self):
        """Verifica la distribución de los datos."""
        logger.info("Verificando distribución de datos...")
        
        if not self.db_path.exists():
            return False, "Base de datos no existe"
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Verificar distribución por fuente
            cursor = conn.execute('SELECT fuente, COUNT(*) FROM sueldos GROUP BY fuente')
            source_dist = dict(cursor.fetchall())
            
            if len(source_dist) == 0:
                return False, "No hay datos por fuente"
            
            # Verificar distribución por estamento
            cursor = conn.execute('SELECT estamento, COUNT(*) FROM sueldos GROUP BY estamento')
            estamento_dist = dict(cursor.fetchall())
            
            if len(estamento_dist) == 0:
                return False, "No hay datos por estamento"
            
            conn.close()
            
            return True, f"Distribución: {len(source_dist)} fuentes, {len(estamento_dist)} estamentos"
            
        except Exception as e:
            return False, f"Error verificando distribución: {e}"
    
    def generate_health_report(self):
        """Genera un reporte de salud del sistema."""
        logger.info("Generando reporte de salud...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        # Verificar frescura
        is_fresh, freshness_msg = self.check_data_freshness()
        report['checks']['data_freshness'] = {
            'status': 'PASS' if is_fresh else 'FAIL',
            'message': freshness_msg
        }
        
        # Verificar calidad
        is_quality_ok, quality_msg = self.check_data_quality()
        report['checks']['data_quality'] = {
            'status': 'PASS' if is_quality_ok else 'FAIL',
            'message': quality_msg
        }
        
        # Verificar tamaños de archivos
        is_size_ok, size_msg = self.check_file_sizes()
        report['checks']['file_sizes'] = {
            'status': 'PASS' if is_size_ok else 'FAIL',
            'message': size_msg
        }
        
        # Verificar distribución
        is_dist_ok, dist_msg = self.check_data_distribution()
        report['checks']['data_distribution'] = {
            'status': 'PASS' if is_dist_ok else 'FAIL',
            'message': dist_msg
        }
        
        # Calcular salud general
        all_passed = all(check['status'] == 'PASS' for check in report['checks'].values())
        report['overall_health'] = 'HEALTHY' if all_passed else 'UNHEALTHY'
        
        return report
    
    def save_health_report(self, report):
        """Guarda el reporte de salud en un archivo."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = self.log_dir / f'health_report_{timestamp}.json'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Reporte de salud guardado en {report_file}")
        
        return report_file
    
    def send_alert(self, report, email_config=None):
        """Envía una alerta si el sistema no está saludable."""
        if report['overall_health'] == 'HEALTHY':
            return
        
        if not email_config:
            logger.warning("Sistema no saludable pero no hay configuración de email")
            return
        
        try:
            # Crear mensaje de email
            msg = MimeMultipart()
            msg['From'] = email_config['from_email']
            msg['To'] = email_config['to_email']
            msg['Subject'] = f"ALERTA: Sistema de Transparencia Salarial - {report['overall_health']}"
            
            # Crear cuerpo del mensaje
            body = f"""
            Sistema de Transparencia Salarial - Reporte de Salud
            
            Estado General: {report['overall_health']}
            Timestamp: {report['timestamp']}
            
            Detalles de las verificaciones:
            """
            
            for check_name, check_result in report['checks'].items():
                body += f"\n- {check_name}: {check_result['status']} - {check_result['message']}"
            
            msg.attach(MimeText(body, 'plain'))
            
            # Enviar email
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            server.starttls()
            server.login(email_config['from_email'], email_config['password'])
            text = msg.as_string()
            server.sendmail(email_config['from_email'], email_config['to_email'], text)
            server.quit()
            
            logger.info("Alerta enviada por email")
            
        except Exception as e:
            logger.error(f"Error enviando alerta por email: {e}")
    
    def run_full_check(self, email_config=None):
        """Ejecuta todas las verificaciones y genera reporte."""
        logger.info("Iniciando verificación completa del sistema...")
        
        report = self.generate_health_report()
        report_file = self.save_health_report(report)
        
        # Mostrar resumen
        logger.info("=== RESUMEN DE SALUD DEL SISTEMA ===")
        logger.info(f"Estado General: {report['overall_health']}")
        
        for check_name, check_result in report['checks'].items():
            logger.info(f"{check_name}: {check_result['status']} - {check_result['message']}")
        
        # Enviar alerta si es necesario
        if report['overall_health'] == 'UNHEALTHY':
            self.send_alert(report, email_config)
        
        return report

def main():
    """Función principal para ejecutar el monitor."""
    monitor = ETLMonitor()
    
    # Configuración de email (opcional)
    email_config = None
    if os.getenv('SMTP_SERVER'):
        email_config = {
            'smtp_server': os.getenv('SMTP_SERVER'),
            'smtp_port': int(os.getenv('SMTP_PORT', 587)),
            'from_email': os.getenv('FROM_EMAIL'),
            'to_email': os.getenv('TO_EMAIL'),
            'password': os.getenv('EMAIL_PASSWORD')
        }
    
    # Ejecutar verificación completa
    report = monitor.run_full_check(email_config)
    
    # Retornar código de salida
    exit_code = 0 if report['overall_health'] == 'HEALTHY' else 1
    return exit_code

if __name__ == '__main__':
    exit(main())
