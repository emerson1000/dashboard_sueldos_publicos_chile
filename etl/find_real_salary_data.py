#!/usr/bin/env python3
"""
Busca específicamente datos reales de remuneraciones en URLs conocidas.
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
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

class RealSalaryDataFinder:
    """Busca datos reales de remuneraciones en URLs específicas."""
    
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.base_dir / 'data' / 'processed'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # URLs específicas conocidas con datos de remuneraciones
        self.known_salary_urls = [
            # SII - Datos conocidos de remuneraciones
            {
                'organismo': 'Servicio de Impuestos Internos',
                'url': 'https://www.sii.cl/transparencia/remuneraciones.html',
                'tipo': 'html',
                'descripcion': 'Remuneraciones SII'
            },
            {
                'organismo': 'Servicio de Impuestos Internos',
                'url': 'https://www.sii.cl/transparencia/planta.html',
                'tipo': 'html',
                'descripcion': 'Planta SII'
            },
            {
                'organismo': 'Servicio de Impuestos Internos',
                'url': 'https://www.sii.cl/transparencia/contrata.html',
                'tipo': 'html',
                'descripcion': 'Contrata SII'
            },
            
            # Ministerio de Hacienda
            {
                'organismo': 'Ministerio de Hacienda',
                'url': 'https://www.hacienda.cl/transparencia/remuneraciones.html',
                'tipo': 'html',
                'descripcion': 'Remuneraciones Hacienda'
            },
            {
                'organismo': 'Ministerio de Hacienda',
                'url': 'https://www.hacienda.cl/transparencia/planta.html',
                'tipo': 'html',
                'descripcion': 'Planta Hacienda'
            },
            
            # Ministerio del Trabajo
            {
                'organismo': 'Ministerio del Trabajo',
                'url': 'https://www.mintrab.gob.cl/transparencia/remuneraciones.html',
                'tipo': 'html',
                'descripcion': 'Remuneraciones Trabajo'
            },
            {
                'organismo': 'Ministerio del Trabajo',
                'url': 'https://www.mintrab.gob.cl/transparencia/planta.html',
                'tipo': 'html',
                'descripcion': 'Planta Trabajo'
            },
            
            # Datos abiertos Chile
            {
                'organismo': 'Datos Abiertos Chile',
                'url': 'https://datos.gob.cl/api/3/action/datastore_search?resource_id=8b0b0b0b-0b0b-0b0b-0b0b-0b0b0b0b0b0b',
                'tipo': 'api',
                'descripcion': 'API Datos Abiertos'
            },
            
            # Portal de Transparencia
            {
                'organismo': 'Portal de Transparencia',
                'url': 'https://www.portaltransparencia.cl/PortalPdT/transparencia-activa',
                'tipo': 'html',
                'descripcion': 'Portal Transparencia'
            },
            
            # Municipalidades con datos conocidos
            {
                'organismo': 'Municipalidad de Santiago',
                'url': 'https://www.municipalidadsantiago.cl/transparencia/remuneraciones',
                'tipo': 'html',
                'descripcion': 'Remuneraciones Santiago'
            },
            {
                'organismo': 'Municipalidad de Las Condes',
                'url': 'https://www.lascondes.cl/transparencia/remuneraciones',
                'tipo': 'html',
                'descripcion': 'Remuneraciones Las Condes'
            },
            {
                'organismo': 'Municipalidad de Providencia',
                'url': 'https://www.providencia.cl/transparencia/remuneraciones',
                'tipo': 'html',
                'descripcion': 'Remuneraciones Providencia'
            },
        ]
        
        # Headers para evitar bloqueos
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def make_request(self, url: str, timeout: int = 30) -> requests.Response:
        """Hace request con manejo de errores."""
        try:
            response = requests.get(url, headers=self.headers, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Error haciendo request a {url}: {e}")
            return None
    
    def extract_from_html(self, url_info: dict) -> list:
        """Extrae datos de página HTML."""
        url = url_info['url']
        organismo = url_info['organismo']
        
        logger.info(f"Extrayendo datos HTML de {organismo}: {url}")
        
        response = self.make_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar tablas con datos de remuneraciones
        tables = soup.find_all('table')
        all_data = []
        
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                data = self._process_dataframe(df, organismo, url)
                all_data.extend(data)
            except Exception as e:
                logger.debug(f"Error procesando tabla: {e}")
                continue
        
        # Buscar enlaces a archivos de datos
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text().strip().lower()
            
            if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv']):
                full_url = urljoin(url, href)
                logger.info(f"Encontrado archivo de datos: {full_url}")
                
                # Intentar extraer datos del archivo
                file_data = self._extract_from_file(full_url, organismo)
                all_data.extend(file_data)
        
        return all_data
    
    def extract_from_api(self, url_info: dict) -> list:
        """Extrae datos de API."""
        url = url_info['url']
        organismo = url_info['organismo']
        
        logger.info(f"Extrayendo datos API de {organismo}: {url}")
        
        response = self.make_request(url)
        if not response:
            return []
        
        try:
            data = response.json()
            
            # Procesar datos JSON
            if 'result' in data and 'records' in data['result']:
                records = data['result']['records']
                df = pd.DataFrame(records)
                return self._process_dataframe(df, organismo, url)
            else:
                logger.warning(f"Formato de API no reconocido en {url}")
                return []
                
        except Exception as e:
            logger.error(f"Error procesando API {url}: {e}")
            return []
    
    def _extract_from_file(self, url: str, organismo: str) -> list:
        """Extrae datos de archivo (Excel/CSV)."""
        try:
            if url.endswith('.csv'):
                df = pd.read_csv(url)
            elif url.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(url)
            else:
                return []
            
            return self._process_dataframe(df, organismo, url)
            
        except Exception as e:
            logger.error(f"Error extrayendo archivo {url}: {e}")
            return []
    
    def _process_dataframe(self, df: pd.DataFrame, organismo: str, url: str) -> list:
        """Procesa DataFrame buscando datos de remuneraciones."""
        data = []
        
        # Identificar columnas relevantes
        sueldo_cols = []
        nombre_cols = []
        cargo_cols = []
        estamento_cols = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            
            if any(keyword in col_lower for keyword in ['sueldo', 'remuneracion', 'salario', 'bruto', 'liquido', 'monto']):
                sueldo_cols.append(col)
            elif any(keyword in col_lower for keyword in ['nombre', 'funcionario', 'empleado', 'persona']):
                nombre_cols.append(col)
            elif any(keyword in col_lower for keyword in ['cargo', 'puesto', 'funcion', 'denominacion']):
                cargo_cols.append(col)
            elif any(keyword in col_lower for keyword in ['estamento', 'grado', 'categoria', 'nivel']):
                estamento_cols.append(col)
        
        if not sueldo_cols:
            return data
        
        logger.info(f"Procesando DataFrame con {len(df)} filas, columnas de sueldo: {sueldo_cols}")
        
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
        
        logger.info(f"Extraídos {len(data)} registros válidos de {organismo}")
        return data
    
    def _extract_sueldo_value(self, row: pd.Series, sueldo_cols: list) -> float:
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
    
    def _extract_field_value(self, row: pd.Series, cols: list) -> str:
        """Extrae valor de un campo específico."""
        if not cols:
            return None
        
        valor = row[cols[0]]
        return str(valor).strip() if pd.notna(valor) else None
    
    def run_extraction(self):
        """Ejecuta extracción completa."""
        logger.info("Iniciando búsqueda de datos reales de remuneraciones")
        
        all_data = []
        
        for url_info in self.known_salary_urls:
            try:
                if url_info['tipo'] == 'html':
                    data = self.extract_from_html(url_info)
                elif url_info['tipo'] == 'api':
                    data = self.extract_from_api(url_info)
                else:
                    continue
                
                all_data.extend(data)
                
                # Pausa entre requests
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error procesando {url_info['url']}: {e}")
                continue
        
        # Guardar datos
        if all_data:
            df = pd.DataFrame(all_data)
            output_file = self.data_dir / 'datos_reales_remuneraciones.csv'
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"Datos guardados en {output_file}")
            logger.info(f"Total registros extraídos: {len(df)}")
            
            # Mostrar resumen
            print("\n" + "="*60)
            print("📊 RESUMEN DE EXTRACCIÓN DE DATOS REALES")
            print("="*60)
            print(f"Total registros extraídos: {len(df):,}")
            print(f"Organismos con datos: {df['organismo'].nunique()}")
            
            if df['sueldo_bruto'].notna().any():
                print(f"Sueldo promedio: ${df['sueldo_bruto'].mean():,.0f}")
                print(f"Sueldo mediana: ${df['sueldo_bruto'].median():,.0f}")
                print(f"Rango sueldos: ${df['sueldo_bruto'].min():,.0f} - ${df['sueldo_bruto'].max():,.0f}")
            
            print("\n🏆 TOP ORGANISMOS:")
            org_counts = df['organismo'].value_counts().head(10)
            for organismo, count in org_counts.items():
                print(f"  {organismo}: {count} registros")
            
            print("="*60)
            
            return df
        else:
            logger.warning("No se encontraron datos de remuneraciones")
            return pd.DataFrame()

def main():
    """Función principal."""
    finder = RealSalaryDataFinder()
    df = finder.run_extraction()
    
    if not df.empty:
        print(f"\n✅ Extracción exitosa: {len(df)} registros de datos reales")
    else:
        print("\n❌ No se encontraron datos reales")

if __name__ == '__main__':
    main()

