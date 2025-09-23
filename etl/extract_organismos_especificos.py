#!/usr/bin/env python3
"""
Script para extraer datos específicos de organismos públicos importantes.
Busca en fuentes conocidas que publican datos de funcionarios.
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

# URLs específicas de organismos que sabemos que publican datos
ORGANISMOS_ESPECIFICOS = {
    'ministerio_hacienda': {
        'url': 'https://www.hacienda.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_educacion': {
        'url': 'https://www.mineduc.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_salud': {
        'url': 'https://www.minsal.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_trabajo': {
        'url': 'https://www.mintrab.gob.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_interior': {
        'url': 'https://www.interior.gob.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_defensa': {
        'url': 'https://www.defensa.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_justicia': {
        'url': 'https://www.minjusticia.gob.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_obras_publicas': {
        'url': 'https://www.mop.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_vivienda': {
        'url': 'https://www.minvu.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    },
    'ministerio_transporte': {
        'url': 'https://www.mtt.gob.cl/transparencia/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario']
    }
}

def buscar_datos_organismo(organismo, config):
    """Busca datos específicos en un organismo."""
    logger.info(f"Buscando datos en {organismo}")
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(config['url'], headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar enlaces relacionados con remuneraciones
        enlaces_relevantes = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().strip().lower()
            
            for keyword in config['buscar']:
                if keyword in href or keyword in text:
                    full_url = requests.compat.urljoin(config['url'], link['href'])
                    enlaces_relevantes.append({
                        'url': full_url,
                        'text': link.get_text().strip(),
                        'keyword': keyword
                    })
        
        logger.info(f"Encontrados {len(enlaces_relevantes)} enlaces relevantes")
        
        # Procesar enlaces encontrados
        todos_datos = []
        for enlace in enlaces_relevantes[:5]:  # Limitar a 5 enlaces
            try:
                datos = procesar_enlace_organismo(enlace, organismo)
                if datos:
                    todos_datos.extend(datos)
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Error procesando enlace {enlace['url']}: {e}")
        
        return todos_datos
        
    except Exception as e:
        logger.error(f"Error buscando datos en {organismo}: {e}")
        return []

def procesar_enlace_organismo(enlace, organismo):
    """Procesa un enlace específico de un organismo."""
    url = enlace['url']
    
    try:
        logger.info(f"Procesando enlace: {url}")
        
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # Intentar leer como Excel
        if url.endswith(('.xlsx', '.xls')):
            try:
                df = pd.read_excel(url)
                return procesar_dataframe_organismo(df, organismo, url)
            except:
                pass
        
        # Intentar leer como CSV
        if url.endswith('.csv'):
            try:
                df = pd.read_csv(url)
                return procesar_dataframe_organismo(df, organismo, url)
            except:
                pass
        
        # Buscar tablas HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        
        datos = []
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                datos_tabla = procesar_dataframe_organismo(df, organismo, url)
                if datos_tabla:
                    datos.extend(datos_tabla)
            except:
                continue
        
        return datos
        
    except Exception as e:
        logger.warning(f"Error procesando enlace {url}: {e}")
        return []

def procesar_dataframe_organismo(df, organismo, url):
    """Procesa un DataFrame de un organismo específico."""
    datos = []
    
    # Buscar columnas relevantes
    columnas_sueldo = []
    columnas_nombre = []
    columnas_cargo = []
    columnas_estamento = []
    
    for col in df.columns:
        col_lower = str(col).lower()
        
        if any(keyword in col_lower for keyword in ['sueldo', 'remuneracion', 'salario', 'bruto', 'liquido', 'monto']):
            columnas_sueldo.append(col)
        
        if any(keyword in col_lower for keyword in ['nombre', 'funcionario', 'empleado', 'persona', 'apellido']):
            columnas_nombre.append(col)
        
        if any(keyword in col_lower for keyword in ['cargo', 'puesto', 'funcion', 'denominacion']):
            columnas_cargo.append(col)
        
        if any(keyword in col_lower for keyword in ['estamento', 'grado', 'categoria', 'nivel']):
            columnas_estamento.append(col)
    
    if not columnas_sueldo:
        return datos
    
    # Procesar filas
    for idx, row in df.iterrows():
        try:
            # Buscar sueldo válido
            sueldo_valido = False
            sueldo_valor = None
            
            for col_sueldo in columnas_sueldo:
                valor = row[col_sueldo]
                if pd.notna(valor):
                    try:
                        # Limpiar valor
                        valor_str = str(valor).strip()
                        valor_str = re.sub(r'[^\d.,]', '', valor_str)
                        
                        if valor_str:
                            # Manejar separadores de miles
                            if '.' in valor_str and ',' in valor_str:
                                valor_str = valor_str.replace('.', '').replace(',', '.')
                            elif '.' in valor_str:
                                parts = valor_str.split('.')
                                if len(parts) > 2:
                                    valor_str = valor_str.replace('.', '')
                            
                            sueldo_num = float(valor_str)
                            
                            # Verificar que sea un sueldo razonable
                            if sueldo_num > 100000:
                                sueldo_valido = True
                                sueldo_valor = sueldo_num
                                break
                    except:
                        continue
            
            if sueldo_valido:
                dato = {
                    'organismo': organismo,
                    'fuente': 'organismo_especifico',
                    'url_origen': url,
                    'sueldo_bruto': sueldo_valor
                }
                
                # Agregar información adicional
                if columnas_nombre:
                    nombre = row[columnas_nombre[0]]
                    dato['nombre'] = str(nombre) if pd.notna(nombre) else None
                
                if columnas_cargo:
                    cargo = row[columnas_cargo[0]]
                    dato['cargo'] = str(cargo) if pd.notna(cargo) else None
                
                if columnas_estamento:
                    estamento = row[columnas_estamento[0]]
                    dato['estamento'] = str(estamento) if pd.notna(estamento) else None
                
                datos.append(dato)
                
        except Exception as e:
            continue
    
    return datos

def main():
    """Función principal para extraer datos de organismos específicos."""
    logger.info("Iniciando extracción de datos de organismos específicos")
    
    # Crear directorio de destino
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'organismos_especificos' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    todos_datos = []
    
    # Procesar cada organismo
    for organismo, config in ORGANISMOS_ESPECIFICOS.items():
        logger.info(f"Procesando {organismo}...")
        datos = buscar_datos_organismo(organismo, config)
        todos_datos.extend(datos)
        
        # Pausa entre organismos
        time.sleep(3)
    
    # Guardar datos encontrados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_organismos.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Datos guardados en {output_file}")
        logger.info(f"Total de funcionarios encontrados: {len(df)}")
        
        # Mostrar resumen
        if len(df) > 0:
            logger.info(f"Promedio sueldo: ${df['sueldo_bruto'].mean():,.0f}")
            logger.info(f"Rango sueldos: ${df['sueldo_bruto'].min():,.0f} - ${df['sueldo_bruto'].max():,.0f}")
            logger.info(f"Organismos con datos: {df['organismo'].nunique()}")
            
            # Mostrar distribución por organismo
            logger.info("Distribución por organismo:")
            for org, count in df['organismo'].value_counts().head(10).items():
                logger.info(f"  {org}: {count} funcionarios")
    else:
        logger.warning("No se encontraron datos de funcionarios")

if __name__ == '__main__':
    main()
