#!/usr/bin/env python3
"""
Extracción completa de datos del SII (Servicio de Impuestos Internos).
El SII tiene una de las mayores dotaciones de funcionarios públicos en Chile.
"""

import requests
import pandas as pd
import time
from pathlib import Path
from bs4 import BeautifulSoup
import logging
import re

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

def buscar_archivos_sii():
    """Busca archivos de remuneraciones en el portal de transparencia del SII."""
    base_url = "https://www.sii.cl/transparencia/"
    urls_sii = [
        f"{base_url}remuneraciones/",
        f"{base_url}remuneraciones/2024/",
        f"{base_url}remuneraciones/2023/",
        f"{base_url}remuneraciones/2022/",
        f"{base_url}remuneraciones/2021/",
        f"{base_url}remuneraciones/2020/",
        f"{base_url}remuneraciones/2019/",
        f"{base_url}remuneraciones/2018/",
        f"{base_url}remuneraciones/2017/",
        f"{base_url}remuneraciones/2016/",
        f"{base_url}remuneraciones/2015/",
        f"{base_url}remuneraciones/2014/",
        f"{base_url}remuneraciones/2013/",
        f"{base_url}remuneraciones/2012/",
        f"{base_url}remuneraciones/2011/",
        f"{base_url}remuneraciones/2010/",
    ]
    
    archivos_encontrados = []
    
    for url in urls_sii:
        try:
            logger.info(f"🔍 Buscando archivos en: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # Buscar enlaces a archivos
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # Buscar archivos CSV, Excel o PDF
                    if any(ext in href.lower() for ext in ['.csv', '.xls', '.xlsx', '.pdf']):
                        full_url = requests.compat.urljoin(url, href)
                        archivos_encontrados.append({
                            'url': full_url,
                            'texto': text,
                            'año': extraer_año_de_url(url)
                        })
                        logger.info(f"📁 Archivo encontrado: {text} - {full_url}")
            
            time.sleep(2)  # Pausa para no saturar el servidor
            
        except Exception as e:
            logger.warning(f"Error accediendo a {url}: {e}")
    
    return archivos_encontrados

def extraer_año_de_url(url):
    """Extrae el año de la URL."""
    match = re.search(r'/(\d{4})/', url)
    return match.group(1) if match else '2024'

def procesar_archivo_sii(archivo_info):
    """Procesa un archivo específico del SII."""
    url = archivo_info['url']
    datos = []
    
    try:
        logger.info(f"⚙️ Procesando archivo: {url}")
        
        if url.lower().endswith('.csv'):
            df = pd.read_csv(url, encoding='latin-1', sep=None, engine='python')
        elif url.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(url)
        else:
            logger.warning(f"Formato no soportado: {url}")
            return datos
        
        logger.info(f"📊 Archivo cargado: {len(df)} filas, {len(df.columns)} columnas")
        
        # Procesar el DataFrame
        for _, row in df.iterrows():
            try:
                # Buscar columnas de sueldo
                sueldo_valor = None
                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(k in col_lower for k in ['sueldo', 'remuneracion', 'salario', 'bruto', 'liquido', 'monto']):
                        valor = row[col]
                        if pd.notna(valor):
                            valor_str = str(valor)
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
                    'fuente': 'sii_completo',
                    'url_origen': url,
                    'sueldo_bruto': sueldo_valor,
                    'año': archivo_info['año']
                }
                
                # Buscar otros campos
                for col in df.columns:
                    col_lower = str(col).lower()
                    valor = row[col]
                    
                    if pd.notna(valor):
                        if any(k in col_lower for k in ['nombre', 'funcionario', 'empleado']):
                            dato['nombre'] = str(valor)
                        elif any(k in col_lower for k in ['cargo', 'puesto', 'funcion']):
                            dato['cargo'] = str(valor)
                        elif any(k in col_lower for k in ['estamento', 'grado', 'categoria']):
                            dato['estamento'] = str(valor)
                        elif any(k in col_lower for k in ['organismo', 'dependencia', 'servicio']):
                            dato['organismo'] = str(valor)
                
                # Si no hay organismo, usar SII
                if 'organismo' not in dato:
                    dato['organismo'] = 'Servicio de Impuestos Internos'
                
                datos.append(dato)
                
            except Exception as e:
                logger.warning(f"Error procesando fila: {e}")
                continue
        
        logger.info(f"✅ Procesados {len(datos)} registros de {url}")
        
    except Exception as e:
        logger.error(f"Error procesando archivo {url}: {e}")
    
    return datos

def main():
    """Función principal para extraer datos completos del SII."""
    logger.info("🚀 Iniciando extracción completa del SII")
    
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'sii_completo' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Buscar archivos
    logger.info("🔍 Buscando archivos en el portal del SII...")
    archivos = buscar_archivos_sii()
    
    if not archivos:
        logger.warning("⚠️ No se encontraron archivos en el SII")
        return
    
    logger.info(f"📁 Encontrados {len(archivos)} archivos para procesar")
    
    # Procesar archivos
    todos_datos = []
    for i, archivo in enumerate(archivos, 1):
        logger.info(f"📊 Procesando archivo {i}/{len(archivos)}")
        datos = procesar_archivo_sii(archivo)
        todos_datos.extend(datos)
        time.sleep(3)  # Pausa entre archivos
    
    # Guardar resultados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_sii_completo.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"✅ Datos del SII guardados en {output_file}")
        logger.info(f"🔢 Total de funcionarios extraídos: {len(df)}")
        
        # Estadísticas por año
        if 'año' in df.columns:
            stats_año = df.groupby('año').size().sort_index()
            logger.info("📈 Funcionarios por año:")
            for año, count in stats_año.items():
                logger.info(f"  {año}: {count} funcionarios")
    else:
        logger.warning("⚠️ No se encontraron datos del SII")

if __name__ == '__main__':
    main()
