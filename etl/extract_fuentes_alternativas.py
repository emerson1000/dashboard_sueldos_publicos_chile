#!/usr/bin/env python3
"""
Script para extraer datos de fuentes alternativas de información pública.
Busca en portales de datos abiertos y APIs públicas.
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

# Fuentes alternativas de datos públicos
FUENTES_ALTERNATIVAS = {
    'datos_gob_cl': {
        'url': 'https://datos.gob.cl/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario', 'personal']
    },
    'transparencia_gob_cl': {
        'url': 'https://www.transparencia.gob.cl/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario', 'personal']
    },
    'portal_transparencia': {
        'url': 'https://portaltransparencia.cl/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario', 'personal']
    },
    'mercadopublico_cl': {
        'url': 'https://www.mercadopublico.cl/',
        'buscar': ['remuneracion', 'sueldo', 'funcionario', 'personal']
    }
}

def buscar_datos_fuente(fuente, config):
    """Busca datos en una fuente alternativa."""
    logger.info(f"Buscando datos en {fuente}")
    
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
        for enlace in enlaces_relevantes[:3]:  # Limitar a 3 enlaces por fuente
            try:
                datos = procesar_enlace_fuente(enlace, fuente)
                if datos:
                    todos_datos.extend(datos)
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Error procesando enlace {enlace['url']}: {e}")
        
        return todos_datos
        
    except Exception as e:
        logger.error(f"Error buscando datos en {fuente}: {e}")
        return []

def procesar_enlace_fuente(enlace, fuente):
    """Procesa un enlace específico de una fuente alternativa."""
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
                return procesar_dataframe_fuente(df, fuente, url)
            except:
                pass
        
        # Intentar leer como CSV
        if url.endswith('.csv'):
            try:
                df = pd.read_csv(url)
                return procesar_dataframe_fuente(df, fuente, url)
            except:
                pass
        
        # Buscar tablas HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        
        datos = []
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                datos_tabla = procesar_dataframe_fuente(df, fuente, url)
                if datos_tabla:
                    datos.extend(datos_tabla)
            except:
                continue
        
        return datos
        
    except Exception as e:
        logger.warning(f"Error procesando enlace {url}: {e}")
        return []

def procesar_dataframe_fuente(df, fuente, url):
    """Procesa un DataFrame de una fuente alternativa."""
    datos = []
    
    # Buscar columnas relevantes
    columnas_sueldo = []
    columnas_nombre = []
    columnas_cargo = []
    columnas_estamento = []
    columnas_organismo = []
    
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
        
        if any(keyword in col_lower for keyword in ['organismo', 'dependencia', 'servicio', 'ministerio']):
            columnas_organismo.append(col)
    
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
                    'fuente': fuente,
                    'url_origen': url,
                    'sueldo_bruto': sueldo_valor
                }
                
                # Agregar información adicional
                if columnas_organismo:
                    organismo = row[columnas_organismo[0]]
                    dato['organismo'] = str(organismo) if pd.notna(organismo) else None
                
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
    """Función principal para extraer datos de fuentes alternativas."""
    logger.info("Iniciando extracción de datos de fuentes alternativas")
    
    # Crear directorio de destino
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'fuentes_alternativas' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    todos_datos = []
    
    # Procesar cada fuente
    for fuente, config in FUENTES_ALTERNATIVAS.items():
        logger.info(f"Procesando {fuente}...")
        datos = buscar_datos_fuente(fuente, config)
        todos_datos.extend(datos)
        
        # Pausa entre fuentes
        time.sleep(3)
    
    # Guardar datos encontrados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_fuentes_alternativas.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Datos guardados en {output_file}")
        logger.info(f"Total de funcionarios encontrados: {len(df)}")
        
        # Mostrar resumen
        if len(df) > 0:
            logger.info(f"Promedio sueldo: ${df['sueldo_bruto'].mean():,.0f}")
            logger.info(f"Rango sueldos: ${df['sueldo_bruto'].min():,.0f} - ${df['sueldo_bruto'].max():,.0f}")
            logger.info(f"Fuentes con datos: {df['fuente'].nunique()}")
            
            # Mostrar distribución por fuente
            logger.info("Distribución por fuente:")
            for fuente, count in df['fuente'].value_counts().head(10).items():
                logger.info(f"  {fuente}: {count} funcionarios")
    else:
        logger.warning("No se encontraron datos de funcionarios")

if __name__ == '__main__':
    main()
