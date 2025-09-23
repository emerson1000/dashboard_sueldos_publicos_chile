#!/usr/bin/env python3
"""
Extracción mejorada de datos desde fuentes alternativas de información pública.

Este script amplía la búsqueda de datos de remuneraciones utilizando, cuando es
posible, las APIs de portales de datos abiertos (por ejemplo, datos.gob.cl) para
localizar datasets relevantes y descargar recursos en formato CSV o Excel. Para
otras fuentes que no ofrecen API se mantiene un rastreo heurístico, pero se
aumenta la cantidad de enlaces analizados. Los datos extraídos se normalizan
con una heurística similar a la empleada en otros extractores.

Limitaciones:
- La red puede bloquear la descarga de ciertos portales si requieren
  autenticación o usan mecanismos anti‑scraping.
- Los portales de transparencia suelen publicar datos de sueldos en secciones
  específicas por organismo; este script está orientado a fuentes generales y
  puede no alcanzar toda la cobertura deseada.

Guarda los resultados en `data/raw/fuentes_alternativas/<YYYY-MM>/funcionarios_fuentes_alternativas.csv`.
"""

import requests
import pandas as pd
import time
from pathlib import Path
from bs4 import BeautifulSoup
import logging
import re
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'

# Palabras clave para buscar datasets de remuneraciones
KEYWORDS = ['remuneracion', 'remuneraciones', 'sueldo', 'sueldos', 'personal', 'funcionario', 'plantilla', 'dotacion']

# Fuentes alternativas de datos públicos
FUENTES_ALTERNATIVAS = {
    'datos_gob_cl': {
        'url': 'https://datos.gob.cl/',
        'buscar': KEYWORDS
    },
    'transparencia_gob_cl': {
        'url': 'https://www.transparencia.gob.cl/',
        'buscar': KEYWORDS
    },
    'portal_transparencia': {
        'url': 'https://portaltransparencia.cl/',
        'buscar': KEYWORDS
    },
    'sii_transparencia': {
        'url': 'https://www.sii.cl/transparencia/',
        'buscar': ['remuneraciones', 'funcionarios', 'personal']
    },
    'contraloria_transparencia': {
        'url': 'https://www.contraloria.cl/',
        'buscar': ['remuneraciones', 'funcionarios', 'personal']
    },
    'dipres_transparencia': {
        'url': 'https://www.dipres.gob.cl/',
        'buscar': ['remuneraciones', 'funcionarios', 'personal']
    }
}

# Cabecera de navegador para requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}


def buscar_datos_datos_gob_cl(keywords):
    """Utiliza la API CKAN de datos.gob.cl para buscar datasets relacionados con las palabras clave."""
    api_search_url = 'https://datos.gob.cl/api/3/action/package_search'
    datos = []
    for keyword in keywords:
        try:
            logger.info(f"🔎 Buscando datasets en datos.gob.cl con la palabra clave: {keyword}")
            resp = requests.get(api_search_url, params={'q': keyword}, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for pkg in data.get('result', {}).get('results', []):
                resources = pkg.get('resources', [])
                for res in resources:
                    fmt = res.get('format', '').lower()
                    if fmt in ('csv', 'xls', 'xlsx'):  # formatos potencialmente tabulares
                        url = res.get('url')
                        if not url:
                            continue
                        try:
                            logger.info(f"📥 Descargando recurso {url}")
                            # Descarga recurso y procesa
                            if url.lower().endswith('.csv'):
                                df = pd.read_csv(url, encoding='latin-1', sep=None, engine='python')
                            elif url.lower().endswith(('.xls', '.xlsx')):
                                df = pd.read_excel(url)
                            else:
                                continue
                            datos.extend(procesar_dataframe_fuente(df, 'datos_gob_cl_api', url))
                            # Pequeña pausa para no saturar el servicio
                            time.sleep(1)
                        except Exception as e:
                            logger.warning(f"Error al procesar recurso {url}: {e}")
        except Exception as e:
            logger.warning(f"Error consultando datos.gob.cl API para '{keyword}': {e}")
    return datos


def buscar_datos_portal(fuente, config):
    """Realiza una búsqueda heurística en la página principal de la fuente para encontrar enlaces a datasets."""
    url_base = config['url']
    keywords = config['buscar']
    try:
        logger.info(f"🌐 Visitando {url_base}")
        resp = requests.get(url_base, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        enlaces_relevantes = []
        # Examina hasta 20 enlaces en la página principal que contengan keywords
        for link in soup.find_all('a', href=True)[:200]:
            href = link.get('href', '')
            text = link.get_text().strip()
            combined = f"{href} {text}".lower()
            if any(keyword in combined for keyword in keywords):
                full_url = requests.compat.urljoin(url_base, href)
                enlaces_relevantes.append({'url': full_url, 'text': text})
        logger.info(f"🔗 {fuente}: {len(enlaces_relevantes)} enlaces potenciales encontrados")
        datos = []
        # Procesa hasta los primeros 10 enlaces relevantes para cada fuente
        for enlace in enlaces_relevantes[:10]:
            try:
                datos.extend(procesar_enlace_fuente(enlace, fuente))
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Error procesando enlace {enlace['url']}: {e}")
        return datos
    except Exception as e:
        logger.warning(f"Error accediendo a {url_base}: {e}")
        return []


def procesar_enlace_fuente(enlace, fuente):
    """Procesa un enlace específico descargando posibles tablas de sueldos."""
    url = enlace['url']
    datos = []
    try:
        logger.info(f"⚙️ Procesando enlace: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        # Intentar como CSV/Excel directo
        lower_url = url.lower()
        if lower_url.endswith('.csv'):
            try:
                df = pd.read_csv(url, encoding='latin-1', sep=None, engine='python')
                return procesar_dataframe_fuente(df, fuente, url)
            except Exception:
                pass
        if lower_url.endswith(('.xls', '.xlsx')):
            try:
                df = pd.read_excel(url)
                return procesar_dataframe_fuente(df, fuente, url)
            except Exception:
                pass
        # Analizar tablas HTML en la página
        soup = BeautifulSoup(resp.content, 'html.parser')
        tables = soup.find_all('table')
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                datos.extend(procesar_dataframe_fuente(df, fuente, url))
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"No se pudo acceder a {url}: {e}")
    return datos


def procesar_dataframe_fuente(df, fuente, url):
    """Normaliza un DataFrame identificando columnas de sueldos y metadatos de funcionarios."""
    datos = []
    if df.empty:
        return datos
    columnas_sueldo = []
    columnas_nombre = []
    columnas_cargo = []
    columnas_estamento = []
    columnas_organismo = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(k in col_lower for k in ['sueldo', 'remuneracion', 'salario', 'bruto', 'liquido', 'monto', 'renta']):
            columnas_sueldo.append(col)
        if any(k in col_lower for k in ['nombre', 'funcionario', 'empleado', 'persona', 'apellido']):
            columnas_nombre.append(col)
        if any(k in col_lower for k in ['cargo', 'puesto', 'funcion', 'denominacion']):
            columnas_cargo.append(col)
        if any(k in col_lower for k in ['estamento', 'grado', 'categoria', 'nivel', 'tramo']):
            columnas_estamento.append(col)
        if any(k in col_lower for k in ['organismo', 'dependencia', 'servicio', 'ministerio', 'institucion']):
            columnas_organismo.append(col)
    if not columnas_sueldo:
        return datos
    for _, row in df.iterrows():
        try:
            sueldo_valor = None
            for col in columnas_sueldo:
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
            dato = {
                'fuente': fuente,
                'url_origen': url,
                'sueldo_bruto': sueldo_valor
            }
            if columnas_organismo:
                val = row[columnas_organismo[0]]
                dato['organismo'] = str(val) if pd.notna(val) else None
            if columnas_nombre:
                val = row[columnas_nombre[0]]
                dato['nombre'] = str(val) if pd.notna(val) else None
            if columnas_cargo:
                val = row[columnas_cargo[0]]
                dato['cargo'] = str(val) if pd.notna(val) else None
            if columnas_estamento:
                val = row[columnas_estamento[0]]
                dato['estamento'] = str(val) if pd.notna(val) else None
            datos.append(dato)
        except Exception:
            continue
    return datos


def buscar_organismos_especificos():
    """Busca datos específicos de organismos grandes conocidos."""
    organismos_urls = [
        'https://www.sii.cl/transparencia/remuneraciones/',
        'https://www.contraloria.cl/transparencia/remuneraciones/',
        'https://www.dipres.gob.cl/transparencia/remuneraciones/',
        'https://www.minsal.cl/transparencia/remuneraciones/',
        'https://www.mineduc.cl/transparencia/remuneraciones/',
        'https://www.mintrab.gob.cl/transparencia/remuneraciones/',
        'https://www.minsalud.gob.cl/transparencia/remuneraciones/',
        'https://www.minsalud.gob.cl/transparencia/remuneraciones/',
        'https://www.minsalud.gob.cl/transparencia/remuneraciones/',
        'https://www.minsalud.gob.cl/transparencia/remuneraciones/',
    ]
    
    datos = []
    for url in organismos_urls:
        try:
            logger.info(f"🔍 Buscando en organismo específico: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                # Buscar enlaces a archivos CSV/Excel
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    if any(ext in href.lower() for ext in ['.csv', '.xls', '.xlsx']):
                        full_url = requests.compat.urljoin(url, href)
                        try:
                            if href.lower().endswith('.csv'):
                                df = pd.read_csv(full_url, encoding='latin-1', sep=None, engine='python')
                            else:
                                df = pd.read_excel(full_url)
                            datos.extend(procesar_dataframe_fuente(df, 'organismo_especifico', full_url))
                            logger.info(f"✅ Procesado archivo: {full_url}")
                        except Exception as e:
                            logger.warning(f"Error procesando {full_url}: {e}")
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Error accediendo a {url}: {e}")
    return datos

def main():
    """Función principal para extraer datos de fuentes alternativas."""
    logger.info("🚀 Iniciando extracción desde fuentes alternativas")
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'fuentes_alternativas' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    todos_datos = []
    
    # Primero, usar la API de datos.gob.cl
    logger.info("🎯 Buscando en datos.gob.cl vía API…")
    todos_datos.extend(buscar_datos_datos_gob_cl(KEYWORDS))
    
    # Buscar organismos específicos
    logger.info("🏛️ Buscando en organismos específicos…")
    todos_datos.extend(buscar_organismos_especificos())
    
    # Luego, hacer rastreo heurístico en las páginas principales de las otras fuentes
    for fuente, config in FUENTES_ALTERNATIVAS.items():
        if fuente == 'datos_gob_cl':
            continue  # ya cubierto por la API
        logger.info(f"🌐 Rastreo heurístico en {fuente}")
        datos = buscar_datos_portal(fuente, config)
        todos_datos.extend(datos)
        time.sleep(2)
    
    # Guardar resultados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_fuentes_alternativas.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"✅ Datos de fuentes alternativas guardados en {output_file}")
        logger.info(f"🔢 Total de funcionarios extraídos: {len(df)}")
    else:
        logger.warning("⚠️ No se encontraron datos de funcionarios en fuentes alternativas")


if __name__ == '__main__':
    main()