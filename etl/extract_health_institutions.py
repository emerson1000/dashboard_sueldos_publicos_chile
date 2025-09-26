#!/usr/bin/env python3
"""
Extractor de datos de remuneraciones de instituciones del Ministerio de Salud.
Basado en la lista oficial de instituciones del MINSAL.
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

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HealthInstitutionsExtractor:
    """Extractor de datos de instituciones del Ministerio de Salud."""
    
    def __init__(self, max_workers=4, timeout=30, max_retries=3):
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.base_dir / 'data' / 'processed'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Headers para evitar bloqueos
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Lista completa de instituciones del Ministerio de Salud
        self.health_institutions = [
            # Servicios de Salud Regionales
            {'nombre': 'Servicio de Salud Arauco', 'url_base': 'https://www.ssarauco.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Ñuble', 'url_base': 'https://www.ssnuble.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud O\'Higgins', 'url_base': 'https://www.ssohiggins.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Aconcagua', 'url_base': 'https://www.ssaconcagua.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Antofagasta', 'url_base': 'https://www.ssantofagasta.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Metropolitano Sur', 'url_base': 'https://www.ssms.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Metropolitano Norte', 'url_base': 'https://www.ssmn.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Fondo Nacional de Salud (FONASA)', 'url_base': 'https://www.fonasa.cl', 'tipo': 'fondo_salud'},
            {'nombre': 'Servicio de Salud Valdivia', 'url_base': 'https://www.ssvaldivia.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Araucanía Norte', 'url_base': 'https://www.ssaraucaniante.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Instituto Nacional del Tórax', 'url_base': 'https://www.incor.cl', 'tipo': 'instituto'},
            {'nombre': 'Instituto de Neurocirugía', 'url_base': 'https://www.neurocirugiainca.cl', 'tipo': 'instituto'},
            {'nombre': 'Instituto Pedro Aguirre Cerda', 'url_base': 'https://www.ipac.cl', 'tipo': 'instituto'},
            {'nombre': 'Servicio de Salud del Reloncaví', 'url_base': 'https://www.ssreloncavi.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud de Chiloé', 'url_base': 'https://www.sschiloe.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Talcahuano', 'url_base': 'https://www.sstalcahuano.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Concepción', 'url_base': 'https://www.ssconcepcion.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Viña Del Mar - Quillota', 'url_base': 'https://www.ssvq.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Valparaíso - San Antonio', 'url_base': 'https://www.ssvsa.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Atacama', 'url_base': 'https://www.ssatacama.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Iquique', 'url_base': 'https://www.ssiquique.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Centro de Referencia de Salud de Peñalolén Cordillera Oriente', 'url_base': 'https://www.crscordillera.cl', 'tipo': 'centro_referencia'},
            {'nombre': 'Servicio de Salud Metropolitano Oriente', 'url_base': 'https://www.ssmo.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Metropolitano Occidente', 'url_base': 'https://www.ssmoc.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Superintendencia de Salud', 'url_base': 'https://www.supersalud.gob.cl', 'tipo': 'superintendencia'},
            {'nombre': 'Instituto de Salud Pública (ISP)', 'url_base': 'https://www.ispch.cl', 'tipo': 'instituto'},
            {'nombre': 'Servicio de Salud Osorno', 'url_base': 'https://www.ssosorno.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Araucanía Sur', 'url_base': 'https://www.ssaraucaniasur.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Instituto Nacional de Geriatría', 'url_base': 'https://www.ing.cl', 'tipo': 'instituto'},
            {'nombre': 'Servicio de Salud Bío-Bío', 'url_base': 'https://www.ssbiobio.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Maule', 'url_base': 'https://www.ssmaule.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Arica', 'url_base': 'https://www.ssarica.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Metropolitano Sur Oriente', 'url_base': 'https://www.ssmso.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Aysén', 'url_base': 'https://www.ssaysen.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Coquimbo', 'url_base': 'https://www.sscoquimbo.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Servicio de Salud Metropolitano Central', 'url_base': 'https://www.ssmc.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Central de Abastecimiento del Sistema Nacional de Servicios de Salud (CENABAST)', 'url_base': 'https://www.cenabast.cl', 'tipo': 'central_abastecimiento'},
            {'nombre': 'Servicio de Salud Magallanes', 'url_base': 'https://www.ssmagallanes.cl', 'tipo': 'servicio_salud'},
            {'nombre': 'Centro de Referencia de Salud de Maipú', 'url_base': 'https://www.crsmaipu.cl', 'tipo': 'centro_referencia'},
            {'nombre': 'Instituto Traumatológico', 'url_base': 'https://www.institutotraulogico.cl', 'tipo': 'instituto'},
            {'nombre': 'Instituto Nacional del Cáncer', 'url_base': 'https://www.incancer.cl', 'tipo': 'instituto'},
        ]
        
        # Patrones para identificar datos de remuneraciones
        self.remuneracion_patterns = [
            r'remuneraci[oó]n',
            r'sueldo',
            r'salario',
            r'bruto',
            r'l[ií]quido',
            r'monto',
            r'honorario',
            r'dotaci[oó]n',
        ]
        
        # Configurar base de datos
        self.db_path = self.data_dir / 'health_institutions_extraction.db'
        self.setup_database()
        
    def setup_database(self):
        """Configura base de datos para tracking."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_progress (
                url TEXT PRIMARY KEY,
                institution_name TEXT,
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
                institution_name TEXT,
                institution_type TEXT,
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
    
    def find_transparency_urls(self, institution: Dict) -> List[str]:
        """Encuentra URLs de transparencia activa para una institución."""
        base_url = institution['url_base']
        transparency_urls = []
        
        # URLs comunes de transparencia
        common_paths = [
            '/transparencia/',
            '/transparencia-activa/',
            '/transparencia/remuneraciones/',
            '/transparencia/dotacion/',
            '/transparencia/personal/',
            '/remuneraciones/',
            '/dotacion/',
            '/personal/',
        ]
        
        for path in common_paths:
            test_url = urljoin(base_url, path)
            response = self.make_request_with_retry(test_url)
            
            if response and response.status_code == 200:
                # Verificar si contiene información de remuneraciones
                if self.contains_salary_data(response.text):
                    transparency_urls.append(test_url)
                    logger.info(f"Encontrada URL de transparencia: {test_url}")
            
            time.sleep(1)  # Pausa entre requests
        
        return transparency_urls
    
    def contains_salary_data(self, html_content: str) -> bool:
        """Verifica si el contenido HTML contiene datos de remuneraciones."""
        content_lower = html_content.lower()
        
        # Buscar patrones de remuneraciones
        for pattern in self.remuneracion_patterns:
            if re.search(pattern, content_lower):
                return True
                
        # Buscar tablas con datos numéricos (posibles sueldos)
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        for table in tables:
            text = table.get_text().lower()
            if any(pattern in text for pattern in ['sueldo', 'remuneracion', 'bruto', 'liquido']):
                return True
                
        return False
    
    def extract_from_url(self, url: str, institution: Dict) -> List[Dict]:
        """Extrae datos de una URL específica."""
        logger.info(f"Extrayendo datos de {institution['nombre']}: {url}")
        
        try:
            response = self.make_request_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar enlaces a archivos de datos
            data_links = self.find_data_links(soup, url)
            all_data = []
            
            for link in data_links:
                try:
                    if link.endswith('.csv'):
                        data = self.extract_from_csv(link, institution)
                    elif link.endswith(('.xlsx', '.xls')):
                        data = self.extract_from_excel(link, institution)
                    else:
                        data = self.extract_from_html_table(link, institution)
                    
                    all_data.extend(data)
                    time.sleep(2)  # Pausa entre extracciones
                    
                except Exception as e:
                    logger.warning(f"Error extrayendo {link}: {e}")
                    continue
            
            # Si no hay enlaces, intentar extraer directamente de la página
            if not all_data:
                all_data = self.extract_from_html_table(url, institution)
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de {url}: {e}")
            return []
    
    def find_data_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Encuentra enlaces a archivos de datos en una página."""
        links = []
        
        # Buscar enlaces a archivos CSV, Excel, etc.
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            
            # Verificar extensiones de archivos de datos
            if any(full_url.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls', '.pdf']):
                # Verificar si el texto del enlace sugiere datos de remuneraciones
                link_text = link.get_text().lower()
                if any(pattern in link_text for pattern in ['remuneracion', 'sueldo', 'dotacion', 'personal']):
                    links.append(full_url)
        
        return links
    
    def extract_from_csv(self, url: str, institution: Dict) -> List[Dict]:
        """Extrae datos de archivo CSV."""
        try:
            df = pd.read_csv(url, encoding='utf-8')
            return self._process_dataframe(df, institution, url)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(url, encoding='latin-1')
                return self._process_dataframe(df, institution, url)
            except Exception as e:
                logger.error(f"Error leyendo CSV {url}: {e}")
                return []
        except Exception as e:
            logger.error(f"Error extrayendo CSV {url}: {e}")
            return []
    
    def extract_from_excel(self, url: str, institution: Dict) -> List[Dict]:
        """Extrae datos de archivo Excel."""
        try:
            df = pd.read_excel(url)
            return self._process_dataframe(df, institution, url)
        except Exception as e:
            logger.error(f"Error extrayendo Excel {url}: {e}")
            return []
    
    def extract_from_html_table(self, url: str, institution: Dict) -> List[Dict]:
        """Extrae datos de tablas HTML."""
        try:
            response = self.make_request_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            tables = soup.find_all('table')
            all_data = []
            
            for table in tables:
                try:
                    df = pd.read_html(str(table))[0]
                    table_data = self._process_dataframe(df, institution, url)
                    all_data.extend(table_data)
                except:
                    continue
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extrayendo HTML {url}: {e}")
            return []
    
    def _process_dataframe(self, df: pd.DataFrame, institution: Dict, url: str) -> List[Dict]:
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
                        'institution_name': institution['nombre'],
                        'institution_type': institution['tipo'],
                        'fuente': 'transparencia_activa_salud',
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
    
    def save_extracted_data(self, data: List[Dict]):
        """Guarda datos extraídos en base de datos."""
        if not data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for item in data:
            cursor.execute('''
                INSERT INTO extracted_data 
                (institution_name, institution_type, nombre, cargo, estamento, sueldo_bruto, fuente, url_origen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (item['institution_name'], item['institution_type'], item.get('nombre'), 
                  item.get('cargo'), item.get('estamento'), item['sueldo_bruto'], 
                  item['fuente'], item['url_origen']))
        
        conn.commit()
        conn.close()
    
    def run_extraction(self, max_institutions: int = None):
        """Ejecuta extracción completa de instituciones de salud."""
        logger.info("Iniciando extracción de instituciones del Ministerio de Salud")
        
        institutions_to_process = self.health_institutions
        if max_institutions:
            institutions_to_process = institutions_to_process[:max_institutions]
        
        logger.info(f"Procesando {len(institutions_to_process)} instituciones")
        
        total_data = 0
        successful_extractions = 0
        
        for institution in institutions_to_process:
            try:
                logger.info(f"Procesando: {institution['nombre']}")
                
                # Encontrar URLs de transparencia
                transparency_urls = self.find_transparency_urls(institution)
                
                if not transparency_urls:
                    logger.warning(f"No se encontraron URLs de transparencia para {institution['nombre']}")
                    continue
                
                # Extraer datos de cada URL
                institution_data = []
                for url in transparency_urls:
                    data = self.extract_from_url(url, institution)
                    institution_data.extend(data)
                    time.sleep(3)  # Pausa entre URLs
                
                if institution_data:
                    self.save_extracted_data(institution_data)
                    total_data += len(institution_data)
                    successful_extractions += 1
                    logger.info(f"SUCCESS {institution['nombre']}: {len(institution_data)} registros")
                else:
                    logger.info(f"NO DATA {institution['nombre']}")
                
                # Pausa entre instituciones
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"ERROR {institution['nombre']}: {e}")
        
        logger.info(f"Extracción completada. Total datos: {total_data}, Instituciones exitosas: {successful_extractions}")
        
        # Generar reporte final
        self.generate_final_report()
    
    def generate_final_report(self):
        """Genera reporte final de la extracción."""
        conn = sqlite3.connect(self.db_path)
        
        # Estadísticas generales
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM extracted_data')
        total_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT institution_name) FROM extracted_data')
        institutions_with_data = cursor.fetchone()[0]
        
        cursor.execute('SELECT AVG(sueldo_bruto) FROM extracted_data')
        avg_salary = cursor.fetchone()[0] or 0
        
        # Top instituciones
        cursor.execute('''
            SELECT institution_name, institution_type, COUNT(*) as count, AVG(sueldo_bruto) as avg_sueldo
            FROM extracted_data 
            GROUP BY institution_name, institution_type 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        top_institutions = cursor.fetchall()
        
        conn.close()
        
        # Guardar reporte
        report = {
            'fecha_extraccion': datetime.now().isoformat(),
            'total_registros': total_records,
            'instituciones_con_datos': institutions_with_data,
            'promedio_sueldo': avg_salary,
            'top_instituciones': [
                {
                    'nombre': name, 
                    'tipo': tipo,
                    'registros': count, 
                    'promedio_sueldo': avg
                }
                for name, tipo, count, avg in top_institutions
            ]
        }
        
        report_file = self.data_dir / 'reporte_instituciones_salud.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Exportar datos a CSV
        csv_file = self.data_dir / 'datos_instituciones_salud.csv'
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('SELECT * FROM extracted_data', conn)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        conn.close()
        
        logger.info(f"Reporte guardado en {report_file}")
        logger.info(f"Datos exportados a {csv_file}")
        
        # Mostrar resumen
        print("\n" + "="*80)
        print("RESUMEN EXTRACCION INSTITUCIONES DEL MINISTERIO DE SALUD")
        print("="*80)
        print(f"Total registros extraidos: {total_records:,}")
        print(f"Instituciones con datos: {institutions_with_data}")
        print(f"Promedio sueldo: ${avg_salary:,.0f}")
        
        if top_institutions:
            print("\nTOP INSTITUCIONES:")
            for i, (name, tipo, count, avg) in enumerate(top_institutions, 1):
                print(f"{i:2d}. {name} ({tipo})")
                print(f"    Registros: {count}")
                print(f"    Promedio: ${avg:,.0f}")
        
        print("="*80)

def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extractor de instituciones del Ministerio de Salud')
    parser.add_argument('--max-institutions', type=int, 
                       help='Número máximo de instituciones a procesar')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Número de workers paralelos')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Timeout en segundos')
    
    args = parser.parse_args()
    
    # Crear extractor
    extractor = HealthInstitutionsExtractor(
        max_workers=args.max_workers,
        timeout=args.timeout,
        max_retries=3
    )
    
    # Ejecutar extracción
    extractor.run_extraction(args.max_institutions)

if __name__ == '__main__':
    main()
