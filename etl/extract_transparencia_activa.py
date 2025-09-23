#!/usr/bin/env python3
"""
Script para extraer datos de transparencia activa de organismos públicos.
Busca específicamente datos de remuneraciones de funcionarios.
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

# URLs específicas de transparencia activa con datos de remuneraciones
TRANSPARENCIA_URLS = {
    'ministerio_hacienda': 'https://www.hacienda.cl/transparencia/remuneraciones/',
    'ministerio_educacion': 'https://www.mineduc.cl/transparencia/remuneraciones/',
    'ministerio_salud': 'https://www.minsal.cl/transparencia/remuneraciones/',
    'ministerio_trabajo': 'https://www.mintrab.gob.cl/transparencia/remuneraciones/',
    'ministerio_interior': 'https://www.interior.gob.cl/transparencia/remuneraciones/',
    'ministerio_defensa': 'https://www.defensa.cl/transparencia/remuneraciones/',
    'ministerio_justicia': 'https://www.minjusticia.gob.cl/transparencia/remuneraciones/',
    'ministerio_obras_publicas': 'https://www.mop.cl/transparencia/remuneraciones/',
    'ministerio_vivienda': 'https://www.minvu.cl/transparencia/remuneraciones/',
    'ministerio_transporte': 'https://www.mtt.gob.cl/transparencia/remuneraciones/',
    'ministerio_energia': 'https://www.energia.gob.cl/transparencia/remuneraciones/',
    'ministerio_medio_ambiente': 'https://www.mma.gob.cl/transparencia/remuneraciones/',
    'ministerio_cultura': 'https://www.cultura.gob.cl/transparencia/remuneraciones/',
    'ministerio_deportes': 'https://www.mindep.cl/transparencia/remuneraciones/',
    'ministerio_mujer': 'https://www.minmujeryeg.gob.cl/transparencia/remuneraciones/',
    'ministerio_ciencia': 'https://www.minciencia.gob.cl/transparencia/remuneraciones/',
    'ministerio_bienes_nacionales': 'https://www.bienesnacionales.cl/transparencia/remuneraciones/',
    'ministerio_mineria': 'https://www.minmineria.cl/transparencia/remuneraciones/',
    'ministerio_agricultura': 'https://www.minagri.gob.cl/transparencia/remuneraciones/',
}

def buscar_datos_remuneraciones(organismo, url_base):
    """Busca datos de remuneraciones en el portal de transparencia."""
    logger.info(f"Buscando datos de remuneraciones en {organismo}")
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url_base, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar enlaces a archivos Excel/CSV
        archivos_encontrados = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text().strip()
            
            # Buscar archivos de datos
            if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv']):
                full_url = requests.compat.urljoin(url_base, href)
                archivos_encontrados.append({
                    'url': full_url,
                    'text': text,
                    'organismo': organismo
                })
        
        logger.info(f"Encontrados {len(archivos_encontrados)} archivos de datos")
        
        # Procesar archivos encontrados
        todos_datos = []
        for archivo in archivos_encontrados[:3]:  # Limitar a 3 archivos por organismo
            try:
                datos = procesar_archivo_remuneraciones(archivo)
                if datos:
                    todos_datos.extend(datos)
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Error procesando archivo {archivo['url']}: {e}")
        
        return todos_datos
        
    except Exception as e:
        logger.error(f"Error buscando datos en {organismo}: {e}")
        return []

def procesar_archivo_remuneraciones(archivo_info):
    """Procesa un archivo específico de remuneraciones."""
    url = archivo_info['url']
    organismo = archivo_info['organismo']
    
    try:
        logger.info(f"Procesando archivo: {url}")
        
        # Descargar archivo
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Determinar tipo de archivo
        if url.endswith('.csv'):
            df = pd.read_csv(url)
        elif url.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(url)
        else:
            logger.warning(f"Tipo de archivo no soportado: {url}")
            return []
        
        logger.info(f"Archivo leído: {len(df)} filas, {len(df.columns)} columnas")
        
        # Procesar datos
        datos = procesar_dataframe_remuneraciones(df, organismo, url)
        
        return datos
        
    except Exception as e:
        logger.error(f"Error procesando archivo {url}: {e}")
        return []

def procesar_dataframe_remuneraciones(df, organismo, url):
    """Procesa un DataFrame buscando datos de remuneraciones."""
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
                                # Formato: 1.234.567,89
                                valor_str = valor_str.replace('.', '').replace(',', '.')
                            elif '.' in valor_str:
                                # Verificar si es separador de miles
                                parts = valor_str.split('.')
                                if len(parts) > 2:
                                    valor_str = valor_str.replace('.', '')
                            
                            sueldo_num = float(valor_str)
                            
                            # Verificar que sea un sueldo razonable
                            if sueldo_num > 100000:  # Mínimo 100,000 pesos
                                sueldo_valido = True
                                sueldo_valor = sueldo_num
                                break
                    except:
                        continue
            
            if sueldo_valido:
                dato = {
                    'organismo': organismo,
                    'fuente': 'transparencia_activa',
                    'url_origen': url,
                    'sueldo_bruto': sueldo_valor
                }
                
                # Agregar información adicional si está disponible
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
    """Función principal para extraer datos de transparencia activa."""
    logger.info("Iniciando extracción de datos de transparencia activa")
    
    # Crear directorio de destino
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'transparencia_activa' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    todos_datos = []
    
    # Procesar cada organismo
    for organismo, url in TRANSPARENCIA_URLS.items():
        logger.info(f"Procesando {organismo}...")
        datos = buscar_datos_remuneraciones(organismo, url)
        todos_datos.extend(datos)
        
        # Pausa entre organismos
        time.sleep(3)
    
    # Guardar datos encontrados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'remuneraciones_transparencia.csv'
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
        logger.warning("No se encontraron datos de remuneraciones")

if __name__ == '__main__':
    main()
