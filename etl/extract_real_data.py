#!/usr/bin/env python3
"""
Extractor de datos reales usando URLs reales de transparencia activa.
"""

import requests
import pandas as pd
import time
import json
from pathlib import Path
from bs4 import BeautifulSoup
import logging
import re
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class RealDataExtractor:
    """Extractor de datos reales de transparencia activa."""
    
    def __init__(self, max_workers=8, timeout=30, max_retries=3):
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.base_dir / 'data' / 'processed'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurar base de datos
        self.db_path = self.data_dir / 'real_data_extraction.db'
        self.setup_database()
        
        # Headers para evitar bloqueos
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Patrones para identificar datos de remuneraciones
        self.remuneracion_patterns = [
            r'remuneraci[oó]n',
            r'sueldo',
            r'salario',
            r'bruto',
            r'l[ií]quido',
            r'monto',
            r'honorario',
            r'asignaci[oó]n',
            r'bonificaci[oó]n',
            r'gratificaci[oó]n'
        ]
    
    def setup_database(self):
        """Configura base de datos para tracking."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_progress (
                url TEXT PRIMARY KEY,
                organismo TEXT,
                status TEXT,
                last_attempt TIMESTAMP,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                last_error TEXT,
                data_count INTEGER DEFAULT 0
            )
        ''')
        
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
        
        conn.commit()
        conn.close()
    
    def make_request_with_retry(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Hace request con reintentos automáticos."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                    **kwargs
                )
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout en intento {attempt + 1} para {url}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error en intento {attempt + 1} para {url}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        logger.error(f"Falló después de {self.max_retries} intentos: {url}")
        return None
    
    def extract_from_csv(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de archivo CSV."""
        try:
            response = self.make_request_with_retry(url)
            if not response:
                return []
            
            # Leer CSV
            df = pd.read_csv(url)
            return self._process_dataframe(df, organismo, url)
            
        except Exception as e:
            logger.error(f"Error extrayendo CSV {url}: {e}")
            return []
    
    def extract_from_excel(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de archivo Excel."""
        try:
            response = self.make_request_with_retry(url)
            if not response:
                return []
            
            # Leer Excel
            df = pd.read_excel(url)
            return self._process_dataframe(df, organismo, url)
            
        except Exception as e:
            logger.error(f"Error extrayendo Excel {url}: {e}")
            return []
    
    def extract_from_html(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de página HTML."""
        try:
            response = self.make_request_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar tablas con datos
            tables = soup.find_all('table')
            all_data = []
            
            for table in tables:
                try:
                    df = pd.read_html(str(table))[0]
                    table_data = self._process_dataframe(df, organismo, url)
                    all_data.extend(table_data)
                except:
                    continue
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extrayendo HTML {url}: {e}")
            return []
    
    def _process_dataframe(self, df: pd.DataFrame, organismo: str, url: str) -> List[Dict]:
        """Procesa un DataFrame buscando datos de remuneraciones."""
        data = []
        
        # Identificar columnas relevantes
        sueldo_cols = []
        nombre_cols = []
        cargo_cols = []
        estamento_cols = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            
            if any(re.search(pattern, col_lower) for pattern in self.remuneracion_patterns):
                sueldo_cols.append(col)
            elif any(keyword in col_lower for keyword in ['nombre', 'funcionario', 'empleado', 'persona']):
                nombre_cols.append(col)
            elif any(keyword in col_lower for keyword in ['cargo', 'puesto', 'función', 'denominación']):
                cargo_cols.append(col)
            elif any(keyword in col_lower for keyword in ['estamento', 'grado', 'categoría', 'nivel']):
                estamento_cols.append(col)
        
        if not sueldo_cols:
            return data
        
        # Procesar filas
        for idx, row in df.iterrows():
            try:
                sueldo_valor = self._extract_sueldo_value(row, sueldo_cols)
                
                if sueldo_valor and sueldo_valor > 100000:  # Mínimo razonable
                    dato = {
                        'organismo': organismo,
                        'fuente': 'transparencia_activa_real',
                        'url_origen': url,
                        'sueldo_bruto': sueldo_valor,
                        'nombre': self._extract_field_value(row, nombre_cols),
                        'cargo': self._extract_field_value(row, cargo_cols),
                        'estamento': self._extract_field_value(row, estamento_cols)
                    }
                    data.append(dato)
                    
            except Exception as e:
                continue
        
        return data
    
    def _extract_sueldo_value(self, row: pd.Series, sueldo_cols: List[str]) -> Optional[float]:
        """Extrae valor de sueldo de una fila."""
        for col in sueldo_cols:
            valor = row[col]
            if pd.notna(valor):
                try:
                    valor_str = str(valor).strip()
                    valor_str = re.sub(r'[^\d.,]', '', valor_str)
                    
                    if valor_str:
                        # Manejar formato chileno
                        if '.' in valor_str and ',' in valor_str:
                            valor_str = valor_str.replace('.', '').replace(',', '.')
                        elif '.' in valor_str and len(valor_str.split('.')[-1]) <= 2:
                            valor_str = valor_str.replace('.', '')
                        
                        sueldo_num = float(valor_str)
                        if sueldo_num > 100000:
                            return sueldo_num
                except:
                    continue
        return None
    
    def _extract_field_value(self, row: pd.Series, cols: List[str]) -> Optional[str]:
        """Extrae valor de un campo específico."""
        if not cols:
            return None
        
        valor = row[cols[0]]
        return str(valor).strip() if pd.notna(valor) else None
    
    def extract_from_url(self, url_info: Dict) -> List[Dict]:
        """Extrae datos de una URL específica."""
        url = url_info['url']
        organismo = url_info['organismo']
        
        logger.info(f"Extrayendo datos de {organismo}: {url}")
        
        try:
            # Determinar tipo de archivo
            if url.endswith('.csv'):
                return self.extract_from_csv(url, organismo)
            elif url.endswith(('.xlsx', '.xls')):
                return self.extract_from_excel(url, organismo)
            else:
                return self.extract_from_html(url, organismo)
                
        except Exception as e:
            logger.error(f"Error extrayendo datos de {url}: {e}")
            return []
    
    def save_extracted_data(self, data: List[Dict]):
        """Guarda datos extraídos en base de datos."""
        if not data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for item in data:
            cursor.execute('''
                INSERT INTO extracted_data 
                (organismo, nombre, cargo, estamento, sueldo_bruto, fuente, url_origen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (item['organismo'], item.get('nombre'), item.get('cargo'), 
                  item.get('estamento'), item['sueldo_bruto'], item['fuente'], item['url_origen']))
        
        conn.commit()
        conn.close()
    
    def update_progress(self, url: str, organismo: str, status: str, data_count: int = 0, error: str = None):
        """Actualiza progreso en base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO extraction_progress 
            (url, organismo, status, last_attempt, success_count, error_count, last_error, data_count)
            VALUES (?, ?, ?, ?, 
                COALESCE((SELECT success_count FROM extraction_progress WHERE url = ?), 0) + ?,
                COALESCE((SELECT error_count FROM extraction_progress WHERE url = ?), 0) + ?,
                ?, ?)
        ''', (url, organismo, status, datetime.now(), url, 1 if status == 'success' else 0,
              url, 1 if status == 'error' else 0, error, data_count))
        
        conn.commit()
        conn.close()
    
    def load_urls_from_csv(self, csv_file: Path) -> pd.DataFrame:
        """Carga URLs desde archivo CSV."""
        if not csv_file.exists():
            logger.error(f"Archivo CSV no encontrado: {csv_file}")
            return pd.DataFrame()
        
        df = pd.read_csv(csv_file)
        logger.info(f"Cargadas {len(df)} URLs desde {csv_file}")
        return df
    
    def run_extraction(self, csv_file: Path, max_urls: int = None):
        """Ejecuta extracción completa."""
        logger.info("Iniciando extracción de datos reales")
        
        # Cargar URLs
        df_urls = self.load_urls_from_csv(csv_file)
        if df_urls.empty:
            logger.error("No hay URLs para procesar")
            return
        
        # Limitar URLs si se especifica
        if max_urls:
            df_urls = df_urls.head(max_urls)
        
        logger.info(f"Procesando {len(df_urls)} URLs con {self.max_workers} workers")
        
        # Procesamiento paralelo
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.extract_from_url, row.to_dict()): row['url'] 
                for _, row in df_urls.iterrows()
            }
            
            completed = 0
            total_data = 0
            
            for future in as_completed(futures):
                url = futures[future]
                try:
                    extracted_data = future.result()
                    
                    if extracted_data:
                        self.save_extracted_data(extracted_data)
                        self.update_progress(url, df_urls[df_urls['url'] == url]['organismo'].iloc[0], 
                                           'success', len(extracted_data))
                        total_data += len(extracted_data)
                        logger.info(f"SUCCESS {url}: {len(extracted_data)} registros")
                    else:
                        self.update_progress(url, df_urls[df_urls['url'] == url]['organismo'].iloc[0], 
                                           'no_data')
                        logger.info(f"NO DATA {url}")
                    
                except Exception as e:
                    self.update_progress(url, df_urls[df_urls['url'] == url]['organismo'].iloc[0], 
                                       'error', error=str(e))
                    logger.error(f"ERROR {url}: {e}")
                
                completed += 1
                logger.info(f"Progreso: {completed}/{len(df_urls)} - Total datos: {total_data}")
                
                # Pausa ocasional
                if completed % 10 == 0:
                    time.sleep(5)
        
        logger.info(f"Extracción completada. Total datos extraídos: {total_data}")
        
        # Generar reporte final
        self.generate_final_report()
    
    def generate_final_report(self):
        """Genera reporte final de la extracción."""
        conn = sqlite3.connect(self.db_path)
        
        # Estadísticas generales
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM extracted_data')
        total_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT organismo) FROM extracted_data')
        organismos_con_datos = cursor.fetchone()[0]
        
        cursor.execute('SELECT AVG(sueldo_bruto) FROM extracted_data')
        promedio_sueldo = cursor.fetchone()[0]
        
        # Top organismos
        cursor.execute('''
            SELECT organismo, COUNT(*) as count, AVG(sueldo_bruto) as avg_sueldo
            FROM extracted_data 
            GROUP BY organismo 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        top_organismos = cursor.fetchall()
        
        conn.close()
        
        # Guardar reporte
        report = {
            'fecha_extraccion': datetime.now().isoformat(),
            'total_registros': total_records,
            'organismos_con_datos': organismos_con_datos,
            'promedio_sueldo': promedio_sueldo,
            'top_organismos': [
                {'organismo': org, 'registros': count, 'promedio_sueldo': avg}
                for org, count, avg in top_organismos
            ]
        }
        
        report_file = self.data_dir / 'reporte_datos_reales.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Exportar datos a CSV
        csv_file = self.data_dir / 'datos_reales_extraidos.csv'
        df = pd.read_sql_query('SELECT * FROM extracted_data', conn)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        conn.close()
        
        logger.info(f"Reporte guardado en {report_file}")
        logger.info(f"Datos exportados a {csv_file}")
        
        # Mostrar resumen
        print("\n" + "="*60)
        print("📊 RESUMEN FINAL DE EXTRACCIÓN DE DATOS REALES")
        print("="*60)
        print(f"Total registros extraídos: {total_records:,}")
        print(f"Organismos con datos: {organismos_con_datos}")
        print(f"Promedio sueldo: ${promedio_sueldo:,.0f}")
        
        print("\n🏆 TOP ORGANISMOS:")
        for i, (org, count, avg) in enumerate(top_organismos, 1):
            print(f"{i:2d}. {org}")
            print(f"    📊 {count} registros")
            print(f"    💰 ${avg:,.0f} promedio")
        
        print("="*60)

def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extractor de datos reales')
    parser.add_argument('--csv-file', type=str, 
                       default='data/processed/urls_transparencia_completas.csv',
                       help='Archivo CSV con URLs')
    parser.add_argument('--max-urls', type=int, 
                       help='Número máximo de URLs a procesar')
    parser.add_argument('--max-workers', type=int, default=8,
                       help='Número de workers paralelos')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Timeout en segundos')
    
    args = parser.parse_args()
    
    # Crear extractor
    extractor = RealDataExtractor(
        max_workers=args.max_workers,
        timeout=args.timeout,
        max_retries=3
    )
    
    # Ejecutar extracción
    csv_file = Path(args.csv_file)
    extractor.run_extraction(csv_file, args.max_urls)

if __name__ == '__main__':
    main()