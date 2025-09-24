#!/usr/bin/env python3
"""
Sistema robusto para extraer datos de transparencia activa de 900+ fuentes.
Maneja timeouts, reintentos y procesamiento paralelo para eficiencia.
"""

import requests
import pandas as pd
import time
import asyncio
import aiohttp
from pathlib import Path
from bs4 import BeautifulSoup
import logging
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
import random
from urllib.parse import urljoin, urlparse
import sqlite3
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transparencia_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'
DATA_PROCESSED = BASE_DIR / 'data' / 'processed'

class TransparenciaActivaExtractor:
    """Extractor robusto para datos de transparencia activa."""
    
    def __init__(self, max_workers=10, timeout=30, max_retries=3):
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = None
        self.progress_db = None
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
            r'asignaci[oó]n'
        ]
        
        # URLs base de organismos públicos chilenos
        self.organismos_base = self._load_organismos_list()
    
    def setup_database(self):
        """Configura base de datos para tracking de progreso."""
        self.progress_db = DATA_PROCESSED / 'extraction_progress.db'
        
        conn = sqlite3.connect(self.progress_db)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_progress (
                organismo TEXT PRIMARY KEY,
                url TEXT,
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
    
    def _load_organismos_list(self) -> List[Dict]:
        """Carga lista de organismos desde múltiples fuentes."""
        organismos = []
        
        # Lista base de organismos públicos chilenos
        organismos_base = [
            # Ministerios
            {'nombre': 'Ministerio de Hacienda', 'url': 'https://www.hacienda.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Educación', 'url': 'https://www.mineduc.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Salud', 'url': 'https://www.minsal.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio del Trabajo', 'url': 'https://www.mintrab.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio del Interior', 'url': 'https://www.interior.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Defensa', 'url': 'https://www.defensa.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Justicia', 'url': 'https://www.minjusticia.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Obras Públicas', 'url': 'https://www.mop.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Vivienda', 'url': 'https://www.minvu.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Transportes', 'url': 'https://www.mtt.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Energía', 'url': 'https://www.energia.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio del Medio Ambiente', 'url': 'https://www.mma.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de las Culturas', 'url': 'https://www.cultura.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio del Deporte', 'url': 'https://www.mindep.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de la Mujer', 'url': 'https://www.minmujeryeg.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Ciencia', 'url': 'https://www.minciencia.gob.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Bienes Nacionales', 'url': 'https://www.bienesnacionales.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Minería', 'url': 'https://www.minmineria.cl/transparencia/', 'tipo': 'ministerio'},
            {'nombre': 'Ministerio de Agricultura', 'url': 'https://www.minagri.gob.cl/transparencia/', 'tipo': 'ministerio'},
            
            # Servicios públicos
            {'nombre': 'Servicio de Impuestos Internos', 'url': 'https://www.sii.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de Aduanas', 'url': 'https://www.aduana.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional del Consumidor', 'url': 'https://www.sernac.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de Capacitación', 'url': 'https://www.sence.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de Turismo', 'url': 'https://www.sernatur.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de Pesca', 'url': 'https://www.sernapesca.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Agrícola y Ganadero', 'url': 'https://www.sag.gob.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de Geología', 'url': 'https://www.sernageomin.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de Menores', 'url': 'https://www.sename.cl/transparencia/', 'tipo': 'servicio'},
            {'nombre': 'Servicio Nacional de la Discapacidad', 'url': 'https://www.senadis.gob.cl/transparencia/', 'tipo': 'servicio'},
        ]
        
        # Agregar municipalidades principales
        municipalidades = [
            'Santiago', 'Valparaíso', 'Viña del Mar', 'Concepción', 'Temuco', 'Antofagasta',
            'Valdivia', 'Iquique', 'La Serena', 'Rancagua', 'Talca', 'Chillán', 'Osorno',
            'Puerto Montt', 'Arica', 'Calama', 'Copiapó', 'Coquimbo', 'Quillota', 'San Antonio',
            'Curicó', 'Los Ángeles', 'Chillán Viejo', 'Villa Alemana', 'Quilpué', 'Maipú',
            'Puente Alto', 'Las Condes', 'Providencia', 'Ñuñoa', 'La Reina', 'Macul', 'San Joaquín',
            'La Florida', 'Peñalolén', 'San Miguel', 'La Granja', 'El Bosque', 'Pedro Aguirre Cerda',
            'Lo Espejo', 'Estación Central', 'Cerrillos', 'Pudahuel', 'Cerro Navia', 'Lo Prado',
            'Quinta Normal', 'Renca', 'Huechuraba', 'Conchalí', 'Independencia', 'Recoleta'
        ]
        
        for muni in municipalidades:
            organismos_base.append({
                'nombre': f'Municipalidad de {muni}',
                'url': f'https://www.{muni.lower().replace(" ", "").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")}.cl/transparencia/',
                'tipo': 'municipalidad'
            })
        
        return organismos_base
    
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
                    time.sleep(2 ** attempt)  # Backoff exponencial
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error en intento {attempt + 1} para {url}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        logger.error(f"Falló después de {self.max_retries} intentos: {url}")
        return None
    
    def find_remuneracion_links(self, organismo_info: Dict) -> List[Dict]:
        """Encuentra enlaces a datos de remuneraciones."""
        url = organismo_info['url']
        organismo = organismo_info['nombre']
        
        logger.info(f"Buscando enlaces de remuneraciones en {organismo}")
        
        response = self.make_request_with_retry(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        links_found = []
        
        # Buscar enlaces que contengan palabras clave de remuneraciones
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text().strip().lower()
            
            # Verificar si el enlace es relevante
            if any(pattern in text for pattern in self.remuneracion_patterns):
                full_url = urljoin(url, href)
                links_found.append({
                    'url': full_url,
                    'text': link.get_text().strip(),
                    'organismo': organismo,
                    'tipo': organismo_info['tipo']
                })
        
        # También buscar enlaces a archivos Excel/CSV
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv', '.pdf']):
                full_url = urljoin(url, href)
                links_found.append({
                    'url': full_url,
                    'text': link.get_text().strip(),
                    'organismo': organismo,
                    'tipo': organismo_info['tipo']
                })
        
        logger.info(f"Encontrados {len(links_found)} enlaces relevantes en {organismo}")
        return links_found
    
    def extract_data_from_link(self, link_info: Dict) -> List[Dict]:
        """Extrae datos de un enlace específico."""
        url = link_info['url']
        organismo = link_info['organismo']
        
        try:
            # Determinar tipo de archivo
            if url.endswith('.csv'):
                return self._extract_from_csv(url, organismo)
            elif url.endswith(('.xlsx', '.xls')):
                return self._extract_from_excel(url, organismo)
            elif url.endswith('.pdf'):
                return self._extract_from_pdf(url, organismo)
            else:
                return self._extract_from_html(url, organismo)
                
        except Exception as e:
            logger.error(f"Error extrayendo datos de {url}: {e}")
            return []
    
    def _extract_from_csv(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de archivo CSV."""
        response = self.make_request_with_retry(url)
        if not response:
            return []
        
        try:
            df = pd.read_csv(url)
            return self._process_dataframe(df, organismo, url)
        except Exception as e:
            logger.error(f"Error leyendo CSV {url}: {e}")
            return []
    
    def _extract_from_excel(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de archivo Excel."""
        response = self.make_request_with_retry(url)
        if not response:
            return []
        
        try:
            df = pd.read_excel(url)
            return self._process_dataframe(df, organismo, url)
        except Exception as e:
            logger.error(f"Error leyendo Excel {url}: {e}")
            return []
    
    def _extract_from_pdf(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de archivo PDF (básico)."""
        # Por ahora, solo registrar que se encontró un PDF
        logger.info(f"PDF encontrado en {organismo}: {url}")
        return []
    
    def _extract_from_html(self, url: str, organismo: str) -> List[Dict]:
        """Extrae datos de página HTML."""
        response = self.make_request_with_retry(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar tablas con datos de remuneraciones
        tables = soup.find_all('table')
        data = []
        
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                table_data = self._process_dataframe(df, organismo, url)
                data.extend(table_data)
            except:
                continue
        
        return data
    
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
            
            if any(pattern in col_lower for pattern in self.remuneracion_patterns):
                sueldo_cols.append(col)
            elif any(keyword in col_lower for keyword in ['nombre', 'funcionario', 'empleado']):
                nombre_cols.append(col)
            elif any(keyword in col_lower for keyword in ['cargo', 'puesto', 'función']):
                cargo_cols.append(col)
            elif any(keyword in col_lower for keyword in ['estamento', 'grado', 'categoría']):
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
                        'fuente': 'transparencia_activa',
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
    
    def update_progress(self, organismo: str, status: str, data_count: int = 0, error: str = None):
        """Actualiza progreso en base de datos."""
        conn = sqlite3.connect(self.progress_db)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO extraction_progress 
            (organismo, status, last_attempt, success_count, error_count, last_error, data_count)
            VALUES (?, ?, ?, 
                COALESCE((SELECT success_count FROM extraction_progress WHERE organismo = ?), 0) + ?,
                COALESCE((SELECT error_count FROM extraction_progress WHERE organismo = ?), 0) + ?,
                ?, ?)
        ''', (organismo, status, datetime.now(), organismo, 1 if status == 'success' else 0,
              organismo, 1 if status == 'error' else 0, error, data_count))
        
        conn.commit()
        conn.close()
    
    def save_extracted_data(self, data: List[Dict]):
        """Guarda datos extraídos en base de datos."""
        if not data:
            return
        
        conn = sqlite3.connect(self.progress_db)
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
    
    def extract_organismo(self, organismo_info: Dict) -> List[Dict]:
        """Extrae datos de un organismo específico."""
        organismo = organismo_info['nombre']
        
        try:
            # Buscar enlaces de remuneraciones
            links = self.find_remuneracion_links(organismo_info)
            
            if not links:
                self.update_progress(organismo, 'no_data')
                return []
            
            # Extraer datos de cada enlace
            all_data = []
            for link in links[:5]:  # Limitar a 5 enlaces por organismo
                data = self.extract_data_from_link(link)
                all_data.extend(data)
                
                # Pausa entre enlaces
                time.sleep(random.uniform(1, 3))
            
            # Guardar datos
            if all_data:
                self.save_extracted_data(all_data)
                self.update_progress(organismo, 'success', len(all_data))
                logger.info(f"Extraídos {len(all_data)} registros de {organismo}")
            else:
                self.update_progress(organismo, 'no_data')
            
            return all_data
            
        except Exception as e:
            error_msg = str(e)
            self.update_progress(organismo, 'error', error=error_msg)
            logger.error(f"Error extrayendo {organismo}: {e}")
            return []
    
    def run_extraction(self, max_organismos: int = None):
        """Ejecuta extracción completa."""
        logger.info("Iniciando extracción robusta de transparencia activa")
        
        organismos = self.organismos_base[:max_organismos] if max_organismos else self.organismos_base
        
        logger.info(f"Procesando {len(organismos)} organismos con {self.max_workers} workers")
        
        # Procesamiento paralelo
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.extract_organismo, org_info): org_info['nombre'] 
                for org_info in organismos
            }
            
            completed = 0
            total_data = 0
            
            for future in as_completed(futures):
                extracted_data = future.result()
                completed += 1
                total_data += len(extracted_data)
                
                logger.info(f"Progreso: {completed}/{len(organismos)} - Total datos: {total_data}")
                
                # Pausa ocasional para no sobrecargar
                if completed % 10 == 0:
                    time.sleep(5)
        
        logger.info(f"Extracción completada. Total datos extraídos: {total_data}")
        
        # Generar reporte final
        self.generate_final_report()
    
    def generate_final_report(self):
        """Genera reporte final de la extracción."""
        conn = sqlite3.connect(self.progress_db)
        
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
        
        report_file = DATA_PROCESSED / 'reporte_transparencia_activa.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Reporte guardado en {report_file}")
        logger.info(f"Total registros extraídos: {total_records}")
        logger.info(f"Organismos con datos: {organismos_con_datos}")
        logger.info(f"Promedio sueldo: ${promedio_sueldo:,.0f}")

def main():
    """Función principal."""
    # Configurar parámetros
    max_workers = 8  # Ajustar según tu sistema
    timeout = 30
    max_retries = 3
    max_organismos = 50  # Empezar con 50 organismos para prueba
    
    # Crear extractor
    extractor = TransparenciaActivaExtractor(
        max_workers=max_workers,
        timeout=timeout,
        max_retries=max_retries
    )
    
    # Ejecutar extracción
    extractor.run_extraction(max_organismos=max_organismos)

if __name__ == '__main__':
    main()
