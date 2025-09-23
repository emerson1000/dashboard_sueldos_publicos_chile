#!/usr/bin/env python3
"""
Extracción detallada de datos de organismos específicos con información completa.
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

# URLs específicas de organismos con datos de remuneraciones
ORGANISMOS_URLS = {
    'ministerio_educacion': {
        'url': 'https://www.mineduc.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Educación'
    },
    'ministerio_salud': {
        'url': 'https://www.minsal.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Salud'
    },
    'ministerio_trabajo': {
        'url': 'https://www.mintrab.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio del Trabajo y Previsión Social'
    },
    'ministerio_hacienda': {
        'url': 'https://www.hacienda.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Hacienda'
    },
    'ministerio_interior': {
        'url': 'https://www.interior.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio del Interior y Seguridad Pública'
    },
    'ministerio_justicia': {
        'url': 'https://www.minjusticia.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Justicia y Derechos Humanos'
    },
    'ministerio_defensa': {
        'url': 'https://www.defensa.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Defensa Nacional'
    },
    'ministerio_relaciones_exteriores': {
        'url': 'https://www.minrel.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Relaciones Exteriores'
    },
    'ministerio_obras_publicas': {
        'url': 'https://www.mop.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Obras Públicas'
    },
    'ministerio_transporte': {
        'url': 'https://www.mtt.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Transportes y Telecomunicaciones'
    },
    'ministerio_vivienda': {
        'url': 'https://www.minvu.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Vivienda y Urbanismo'
    },
    'ministerio_medio_ambiente': {
        'url': 'https://www.mma.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio del Medio Ambiente'
    },
    'ministerio_energia': {
        'url': 'https://www.energia.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Energía'
    },
    'ministerio_mineria': {
        'url': 'https://www.minmineria.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Minería'
    },
    'ministerio_agricultura': {
        'url': 'https://www.minagri.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Agricultura'
    },
    'ministerio_desarrollo_social': {
        'url': 'https://www.desarrollosocial.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Desarrollo Social y Familia'
    },
    'ministerio_cultura': {
        'url': 'https://www.cultura.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de las Culturas, las Artes y el Patrimonio'
    },
    'ministerio_deporte': {
        'url': 'https://www.mindep.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio del Deporte'
    },
    'ministerio_mujer': {
        'url': 'https://www.minmujeryeg.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de la Mujer y la Equidad de Género'
    },
    'ministerio_ciencia': {
        'url': 'https://www.minciencia.gob.cl/transparencia/remuneraciones/',
        'nombre': 'Ministerio de Ciencia, Tecnología, Conocimiento e Innovación'
    }
}

def buscar_archivos_organismo(organismo_id, config):
    """Busca archivos de remuneraciones en un organismo específico."""
    url = config['url']
    nombre = config['nombre']
    
    logger.info(f"🔍 Buscando archivos en {nombre}: {url}")
    
    archivos_encontrados = []
    
    try:
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
                        'organismo': nombre,
                        'organismo_id': organismo_id
                    })
                    logger.info(f"📁 Archivo encontrado: {text} - {full_url}")
        
        # También buscar en subdirectorios por año
        for año in ['2024', '2023', '2022', '2021', '2020']:
            año_url = f"{url}{año}/"
            try:
                resp_año = requests.get(año_url, headers=HEADERS, timeout=20)
                if resp_año.status_code == 200:
                    soup_año = BeautifulSoup(resp_año.content, 'html.parser')
                    for link in soup_año.find_all('a', href=True):
                        href = link.get('href', '')
                        text = link.get_text().strip()
                        if any(ext in href.lower() for ext in ['.csv', '.xls', '.xlsx']):
                            full_url = requests.compat.urljoin(año_url, href)
                            archivos_encontrados.append({
                                'url': full_url,
                                'texto': text,
                                'organismo': nombre,
                                'organismo_id': organismo_id,
                                'año': año
                            })
            except Exception as e:
                logger.warning(f"Error accediendo a {año_url}: {e}")
            
            time.sleep(1)
        
    except Exception as e:
        logger.warning(f"Error accediendo a {url}: {e}")
    
    return archivos_encontrados

def procesar_archivo_organismo(archivo_info):
    """Procesa un archivo específico de un organismo."""
    url = archivo_info['url']
    organismo = archivo_info['organismo']
    organismo_id = archivo_info['organismo_id']
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
                    'fuente': f'organismo_{organismo_id}',
                    'url_origen': url,
                    'sueldo_bruto': sueldo_valor,
                    'organismo': organismo,
                    'año': archivo_info.get('año', '2024')
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
                        elif any(k in col_lower for k in ['estamento', 'grado', 'categoria', 'nivel']):
                            dato['estamento'] = str(valor)
                        elif any(k in col_lower for k in ['grado', 'tramo', 'escala']):
                            dato['grado'] = str(valor)
                
                # Valores por defecto si no se encuentran
                if 'nombre' not in dato:
                    dato['nombre'] = 'Sin especificar'
                if 'cargo' not in dato:
                    dato['cargo'] = 'Sin especificar'
                if 'estamento' not in dato:
                    dato['estamento'] = 'Sin especificar'
                if 'grado' not in dato:
                    dato['grado'] = 'Sin especificar'
                
                datos.append(dato)
                
            except Exception as e:
                logger.warning(f"Error procesando fila: {e}")
                continue
        
        logger.info(f"✅ Procesados {len(datos)} registros de {url}")
        
    except Exception as e:
        logger.error(f"Error procesando archivo {url}: {e}")
    
    return datos

def main():
    """Función principal para extraer datos detallados de organismos."""
    logger.info("🚀 Iniciando extracción detallada de organismos")
    
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'organismos_detallados' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    todos_datos = []
    
    # Procesar cada organismo
    for organismo_id, config in ORGANISMOS_URLS.items():
        logger.info(f"🏛️ Procesando {config['nombre']}")
        
        # Buscar archivos
        archivos = buscar_archivos_organismo(organismo_id, config)
        
        if not archivos:
            logger.warning(f"⚠️ No se encontraron archivos en {config['nombre']}")
            continue
        
        logger.info(f"📁 Encontrados {len(archivos)} archivos en {config['nombre']}")
        
        # Procesar archivos
        for archivo in archivos:
            datos = procesar_archivo_organismo(archivo)
            todos_datos.extend(datos)
            time.sleep(2)  # Pausa entre archivos
        
        time.sleep(3)  # Pausa entre organismos
    
    # Guardar resultados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_organismos_detallados.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"✅ Datos de organismos guardados en {output_file}")
        logger.info(f"🔢 Total de funcionarios extraídos: {len(df)}")
        
        # Estadísticas por organismo
        if 'organismo' in df.columns:
            stats_organismo = df.groupby('organismo').size().sort_values(ascending=False)
            logger.info("📈 Funcionarios por organismo:")
            for organismo, count in stats_organismo.items():
                logger.info(f"  {organismo}: {count} funcionarios")
        
        # Estadísticas por estamento
        if 'estamento' in df.columns:
            stats_estamento = df.groupby('estamento').size().sort_values(ascending=False)
            logger.info("📋 Funcionarios por estamento:")
            for estamento, count in stats_estamento.items():
                logger.info(f"  {estamento}: {count} funcionarios")
    else:
        logger.warning("⚠️ No se encontraron datos de organismos")

if __name__ == '__main__':
    main()
