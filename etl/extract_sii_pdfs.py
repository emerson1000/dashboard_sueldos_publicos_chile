#!/usr/bin/env python3
"""
Extrae datos de remuneraciones del SII desde PDFs históricos.
"""

import requests
import pandas as pd
import time
from pathlib import Path
from bs4 import BeautifulSoup
import logging
import re
import pdfplumber
import io

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'

# Headers para evitar bloqueos
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def obtener_enlaces_sii_historicos():
    """Obtiene todos los enlaces de datos históricos del SII."""
    base_urls = [
        'https://www.sii.cl/transparencia/planta_historico.html',
        'https://www.sii.cl/transparencia/contrata_historico.html',
        'https://www.sii.cl/transparencia/honorarios_historico.html'
    ]
    
    enlaces_todos = []
    
    for base_url in base_urls:
        try:
            logger.info(f"🔍 Obteniendo enlaces de: {base_url}")
            resp = requests.get(base_url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # Buscar todos los enlaces
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # Filtrar enlaces relevantes
                    if any(keyword in href.lower() for keyword in ['dotacion', 'planta', 'contrata', 'honorarios']):
                        full_url = requests.compat.urljoin(base_url, href)
                        
                        # Navegar a la página del año para obtener enlaces mensuales
                        enlaces_mensuales = obtener_enlaces_mensuales_sii(full_url, base_url)
                        enlaces_todos.extend(enlaces_mensuales)
            
            time.sleep(2)
            
        except Exception as e:
            logger.warning(f"Error accediendo a {base_url}: {e}")
    
    return enlaces_todos

def obtener_enlaces_mensuales_sii(url_año, base_url):
    """Obtiene los enlaces mensuales de una página de año específico."""
    enlaces_mensuales = []
    
    try:
        logger.info(f"🔍 Obteniendo enlaces mensuales de: {url_año}")
        resp = requests.get(url_año, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Buscar enlaces a PDFs mensuales
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text().strip()
                
                # Filtrar enlaces a PDFs
                if href.lower().endswith('.pdf'):
                    full_url = requests.compat.urljoin(url_año, href)
                    enlaces_mensuales.append({
                        'url': full_url,
                        'texto': text,
                        'tipo': 'planta' if 'planta' in base_url else 'contrata' if 'contrata' in base_url else 'honorarios'
                    })
                    logger.info(f"📁 PDF encontrado: {text} - {full_url}")
        
        time.sleep(1)
        
    except Exception as e:
        logger.warning(f"Error accediendo a {url_año}: {e}")
    
    return enlaces_mensuales

def procesar_pdf_sii(url, tipo_dotacion):
    """Procesa un PDF específico del SII."""
    datos = []
    
    try:
        logger.info(f"⚙️ Procesando PDF: {url}")
        
        # Descargar PDF
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code != 200:
            logger.warning(f"Error descargando PDF: {url}")
            return datos
        
        # Procesar PDF con pdfplumber
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page_num, page in enumerate(pdf.pages):
                try:
                    # Extraer texto
                    text = page.extract_text()
                    if not text:
                        continue
                    
                    # Extraer tablas
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        
                        # Convertir tabla a DataFrame
                        df = pd.DataFrame(table[1:], columns=table[0])
                        
                        # Procesar cada fila
                        for _, row in df.iterrows():
                            try:
                                # Buscar columnas de sueldo
                                sueldo_valor = None
                                for col in df.columns:
                                    if col and any(k in str(col).lower() for k in ['sueldo', 'remuneracion', 'salario', 'bruto', 'liquido', 'monto']):
                                        valor = row[col]
                                        if valor and str(valor).strip():
                                            valor_str = str(valor).strip()
                                            valor_str = re.sub(r'[\s\$]', '', valor_str)
                                            valor_str = valor_str.replace('.', '').replace(',', '.')
                                            try:
                                                sueldo_num = float(valor_str)
                                                if sueldo_num > 10000:  # Filtra valores triviales
                                                    sueldo_valor = sueldo_num
                                                    break
                                            except Exception:
                                                continue
                                
                                if sueldo_valor is None:
                                    continue
                                
                                # Crear registro
                                dato = {
                                    'fuente': f'sii_{tipo_dotacion}_pdf',
                                    'url_origen': url,
                                    'sueldo_bruto': sueldo_valor,
                                    'organismo': 'Servicio de Impuestos Internos',
                                    'estamento': tipo_dotacion.title(),
                                    'año': extraer_año_de_url(url),
                                    'mes': extraer_mes_de_url(url)
                                }
                                
                                # Buscar otros campos
                                for col in df.columns:
                                    if col and str(col).strip():
                                        valor = row[col]
                                        if valor and str(valor).strip():
                                            col_lower = str(col).lower()
                                            if any(k in col_lower for k in ['nombre', 'funcionario', 'empleado']):
                                                dato['nombre'] = str(valor).strip()
                                            elif any(k in col_lower for k in ['cargo', 'puesto', 'funcion']):
                                                dato['cargo'] = str(valor).strip()
                                            elif any(k in col_lower for k in ['grado', 'tramo', 'escala']):
                                                dato['grado'] = str(valor).strip()
                                
                                # Valores por defecto
                                if 'nombre' not in dato:
                                    dato['nombre'] = 'Sin especificar'
                                if 'cargo' not in dato:
                                    dato['cargo'] = 'Sin especificar'
                                if 'grado' not in dato:
                                    dato['grado'] = 'Sin especificar'
                                
                                datos.append(dato)
                                
                            except Exception as e:
                                logger.warning(f"Error procesando fila: {e}")
                                continue
                
                except Exception as e:
                    logger.warning(f"Error procesando página {page_num}: {e}")
                    continue
        
        logger.info(f"✅ Procesados {len(datos)} registros de {url}")
        
    except Exception as e:
        logger.error(f"Error procesando PDF {url}: {e}")
    
    return datos

def extraer_año_de_url(url):
    """Extrae el año de la URL."""
    match = re.search(r'/(\d{4})/', url)
    return match.group(1) if match else '2024'

def extraer_mes_de_url(url):
    """Extrae el mes de la URL."""
    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }
    
    url_lower = url.lower()
    for mes, numero in meses.items():
        if mes in url_lower:
            return numero
    
    return '01'

def main():
    """Función principal para extraer datos de PDFs del SII."""
    logger.info("🚀 Iniciando extracción de PDFs del SII")
    
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'sii_pdfs' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Obtener enlaces
    logger.info("🔍 Obteniendo enlaces de datos históricos del SII...")
    enlaces = obtener_enlaces_sii_historicos()
    
    if not enlaces:
        logger.warning("⚠️ No se encontraron enlaces del SII")
        return
    
    logger.info(f"📁 Encontrados {len(enlaces)} enlaces para procesar")
    
    # Procesar PDFs
    todos_datos = []
    for i, enlace in enumerate(enlaces, 1):
        logger.info(f"📊 Procesando enlace {i}/{len(enlaces)}")
        datos = procesar_pdf_sii(enlace['url'], enlace['tipo'])
        todos_datos.extend(datos)
        time.sleep(3)  # Pausa entre archivos
    
    # Guardar resultados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_sii_pdfs.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"✅ Datos del SII guardados en {output_file}")
        logger.info(f"🔢 Total de funcionarios extraídos: {len(df)}")
        
        # Estadísticas por tipo
        if 'estamento' in df.columns:
            stats_tipo = df.groupby('estamento').size().sort_values(ascending=False)
            logger.info("📈 Funcionarios por tipo:")
            for tipo, count in stats_tipo.items():
                logger.info(f"  {tipo}: {count} funcionarios")
        
        # Estadísticas por año
        if 'año' in df.columns:
            stats_año = df.groupby('año').size().sort_index()
            logger.info("📅 Funcionarios por año:")
            for año, count in stats_año.items():
                logger.info(f"  {año}: {count} funcionarios")
    else:
        logger.warning("⚠️ No se encontraron datos del SII")

if __name__ == '__main__':
    main()
