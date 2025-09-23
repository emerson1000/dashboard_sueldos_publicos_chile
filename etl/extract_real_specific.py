#!/usr/bin/env python3
"""
Script para extraer datos reales de funcionarios de URLs específicos conocidos.
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

# URLs específicos conocidos que tienen datos de funcionarios
URLS_ESPECIFICOS = {
    'ministerio_trabajo': 'https://www.mintrab.gob.cl/transparencia/remuneraciones.html',
    'ministerio_hacienda': 'https://www.hacienda.cl/transparencia/',
    'ministerio_obras_publicas': 'https://www.mop.cl/transparencia/',
    'ministerio_vivienda': 'https://www.minvu.cl/transparencia/',
    'ministerio_cultura': 'https://www.cultura.gob.cl/transparencia/',
    'ministerio_mujer': 'https://www.minmujeryeg.gob.cl/transparencia/',
    'ministerio_bienes_nacionales': 'https://www.bienesnacionales.cl/transparencia/',
    'sii': 'https://www.sii.cl/transparencia/2025/plantilla_escala_ene.html',
    'contraloria': 'https://www.contraloria.cl/transparencia/',
    'dipres': 'https://www.dipres.gob.cl/transparencia/'
}

def extraer_datos_url(organismo, url):
    """Extrae datos de un URL específico."""
    logger.info(f"Extrayendo datos de {organismo}: {url}")
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Respuesta exitosa: {response.status_code}")
        
        # Intentar leer como Excel
        if url.endswith(('.xlsx', '.xls')):
            try:
                df = pd.read_excel(url)
                return procesar_dataframe_real(df, organismo, url)
            except:
                pass
        
        # Intentar leer como CSV
        if url.endswith('.csv'):
            try:
                df = pd.read_csv(url)
                return procesar_dataframe_real(df, organismo, url)
            except:
                pass
        
        # Buscar tablas HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        
        datos = []
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                datos_tabla = procesar_dataframe_real(df, organismo, url)
                if datos_tabla:
                    datos.extend(datos_tabla)
            except:
                continue
        
        # Buscar enlaces a archivos de datos
        enlaces_datos = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv']):
                full_url = requests.compat.urljoin(url, href)
                enlaces_datos.append(full_url)
        
        # Procesar enlaces encontrados
        for enlace in enlaces_datos[:3]:  # Limitar a 3 enlaces
            try:
                datos_enlace = extraer_datos_url(f"{organismo}_archivo", enlace)
                if datos_enlace:
                    datos.extend(datos_enlace)
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Error procesando enlace {enlace}: {e}")
        
        return datos
        
    except Exception as e:
        logger.error(f"Error extrayendo datos de {organismo}: {e}")
        return []

def procesar_dataframe_real(df, organismo, url):
    """Procesa un DataFrame buscando datos reales de funcionarios."""
    datos = []
    
    logger.info(f"Procesando DataFrame: {len(df)} filas, {len(df.columns)} columnas")
    
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
    
    logger.info(f"Columnas encontradas - Sueldo: {len(columnas_sueldo)}, Nombre: {len(columnas_nombre)}, Cargo: {len(columnas_cargo)}, Estamento: {len(columnas_estamento)}")
    
    if not columnas_sueldo:
        logger.warning("No se encontraron columnas de sueldo")
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
                            if sueldo_num > 50000:  # Mínimo 50,000 pesos
                                sueldo_valido = True
                                sueldo_valor = sueldo_num
                                break
                    except:
                        continue
            
            if sueldo_valido:
                dato = {
                    'organismo': organismo,
                    'fuente': 'url_especifico',
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
    
    logger.info(f"Procesadas {len(datos)} filas con datos válidos")
    return datos

def main():
    """Función principal para extraer datos reales específicos."""
    logger.info("Iniciando extracción de datos reales específicos")
    
    # Crear directorio de destino
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'datos_reales_especificos' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    todos_datos = []
    
    # Procesar cada URL específico
    for organismo, url in URLS_ESPECIFICOS.items():
        logger.info(f"Procesando {organismo}...")
        datos = extraer_datos_url(organismo, url)
        todos_datos.extend(datos)
        
        # Pausa entre organismos
        time.sleep(2)
    
    # Guardar datos encontrados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_reales_especificos.csv'
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
