#!/usr/bin/env python3
"""
Script mejorado para extraer datos reales de funcionarios públicos.
Busca en portales de transparencia activa de organismos públicos.
"""

import requests
import pandas as pd
import time
from pathlib import Path
from bs4 import BeautifulSoup
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'

# Lista de organismos públicos importantes con sus portales de transparencia
ORGANISMOS_TRANSPARENCIA = {
    'presidencia': 'https://www.presidencia.cl/transparencia/',
    'ministerio_hacienda': 'https://www.hacienda.cl/transparencia/',
    'ministerio_educacion': 'https://www.mineduc.cl/transparencia/',
    'ministerio_salud': 'https://www.minsal.cl/transparencia/',
    'ministerio_trabajo': 'https://www.mintrab.gob.cl/transparencia/',
    'ministerio_interior': 'https://www.interior.gob.cl/transparencia/',
    'ministerio_defensa': 'https://www.defensa.cl/transparencia/',
    'ministerio_justicia': 'https://www.minjusticia.gob.cl/transparencia/',
    'ministerio_obras_publicas': 'https://www.mop.cl/transparencia/',
    'ministerio_vivienda': 'https://www.minvu.cl/transparencia/',
    'ministerio_transporte': 'https://www.mtt.gob.cl/transparencia/',
    'ministerio_energia': 'https://www.energia.gob.cl/transparencia/',
    'ministerio_medio_ambiente': 'https://www.mma.gob.cl/transparencia/',
    'ministerio_cultura': 'https://www.cultura.gob.cl/transparencia/',
    'ministerio_deportes': 'https://www.mindep.cl/transparencia/',
    'ministerio_mujer': 'https://www.minmujeryeg.gob.cl/transparencia/',
    'ministerio_ciencia': 'https://www.minciencia.gob.cl/transparencia/',
    'ministerio_bienes_nacionales': 'https://www.bienesnacionales.cl/transparencia/',
    'ministerio_mineria': 'https://www.minmineria.cl/transparencia/',
    'ministerio_agricultura': 'https://www.minagri.gob.cl/transparencia/',
}

def buscar_datos_funcionarios(organismo, url_base):
    """Busca datos de funcionarios en el portal de transparencia de un organismo."""
    logger.info(f"Buscando datos de funcionarios en {organismo}")
    
    try:
        # Buscar páginas relacionadas con remuneraciones
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url_base, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar enlaces relacionados con remuneraciones, sueldos, funcionarios
        keywords = ['remuneracion', 'sueldo', 'funcionario', 'personal', 'empleado', 'salario']
        links = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower()
            
            for keyword in keywords:
                if keyword in href or keyword in text:
                    full_url = requests.compat.urljoin(url_base, link['href'])
                    links.append({
                        'url': full_url,
                        'text': link.get_text().strip(),
                        'keyword': keyword
                    })
        
        logger.info(f"Encontrados {len(links)} enlaces relacionados con remuneraciones")
        
        # Procesar los enlaces encontrados
        datos_encontrados = []
        for link in links[:5]:  # Limitar a los primeros 5 enlaces
            try:
                datos = procesar_enlace_remuneraciones(link['url'], organismo)
                if datos:
                    datos_encontrados.extend(datos)
                time.sleep(1)  # Pausa entre requests
            except Exception as e:
                logger.warning(f"Error procesando enlace {link['url']}: {e}")
        
        return datos_encontrados
        
    except Exception as e:
        logger.error(f"Error buscando datos en {organismo}: {e}")
        return []

def procesar_enlace_remuneraciones(url, organismo):
    """Procesa un enlace específico buscando datos de remuneraciones."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Intentar leer como Excel
        if url.endswith(('.xlsx', '.xls')):
            try:
                df = pd.read_excel(url)
                return procesar_dataframe_remuneraciones(df, organismo, url)
            except:
                pass
        
        # Intentar leer como CSV
        if url.endswith('.csv'):
            try:
                df = pd.read_csv(url)
                return procesar_dataframe_remuneraciones(df, organismo, url)
            except:
                pass
        
        # Buscar tablas HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        
        datos = []
        for table in tables:
            try:
                df = pd.read_html(str(table))[0]
                datos_tabla = procesar_dataframe_remuneraciones(df, organismo, url)
                if datos_tabla:
                    datos.extend(datos_tabla)
            except:
                continue
        
        return datos
        
    except Exception as e:
        logger.warning(f"Error procesando enlace {url}: {e}")
        return []

def procesar_dataframe_remuneraciones(df, organismo, url):
    """Procesa un DataFrame buscando datos de remuneraciones."""
    datos = []
    
    # Buscar columnas que puedan contener sueldos
    columnas_sueldo = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['sueldo', 'remuneracion', 'salario', 'bruto', 'liquido']):
            columnas_sueldo.append(col)
    
    if not columnas_sueldo:
        return datos
    
    # Buscar columnas de nombres/funcionarios
    columnas_nombre = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['nombre', 'funcionario', 'empleado', 'persona']):
            columnas_nombre.append(col)
    
    # Buscar columnas de cargo/estamento
    columnas_cargo = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['cargo', 'estamento', 'grado', 'categoria']):
            columnas_cargo.append(col)
    
    # Procesar filas con datos válidos
    for idx, row in df.iterrows():
        try:
            # Verificar si la fila tiene datos de sueldo
            sueldo_valido = False
            sueldo_valor = None
            
            for col_sueldo in columnas_sueldo:
                valor = row[col_sueldo]
                if pd.notna(valor):
                    # Intentar convertir a número
                    try:
                        sueldo_num = float(str(valor).replace('.', '').replace(',', '.'))
                        if sueldo_num > 100000:  # Sueldo mínimo razonable
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
                
                # Agregar nombre si está disponible
                if columnas_nombre:
                    nombre = row[columnas_nombre[0]]
                    dato['nombre'] = str(nombre) if pd.notna(nombre) else None
                
                # Agregar cargo si está disponible
                if columnas_cargo:
                    cargo = row[columnas_cargo[0]]
                    dato['cargo'] = str(cargo) if pd.notna(cargo) else None
                
                datos.append(dato)
                
        except Exception as e:
            continue
    
    return datos

def main():
    """Función principal para extraer datos reales de funcionarios."""
    logger.info("Iniciando extracción de datos reales de funcionarios públicos")
    
    # Crear directorio de destino
    y_m = time.strftime("%Y-%m")
    dest_dir = DATA_RAW / 'funcionarios_reales' / y_m
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    todos_datos = []
    
    # Procesar cada organismo
    for organismo, url in ORGANISMOS_TRANSPARENCIA.items():
        logger.info(f"Procesando {organismo}...")
        datos = buscar_datos_funcionarios(organismo, url)
        todos_datos.extend(datos)
        
        # Pausa entre organismos
        time.sleep(2)
    
    # Guardar datos encontrados
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        output_file = dest_dir / 'funcionarios_reales.csv'
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Datos guardados en {output_file}")
        logger.info(f"Total de funcionarios encontrados: {len(df)}")
        
        # Mostrar resumen
        if len(df) > 0:
            logger.info(f"Promedio sueldo: ${df['sueldo_bruto'].mean():,.0f}")
            logger.info(f"Rango sueldos: ${df['sueldo_bruto'].min():,.0f} - ${df['sueldo_bruto'].max():,.0f}")
            logger.info(f"Organismos con datos: {df['organismo'].nunique()}")
    else:
        logger.warning("No se encontraron datos de funcionarios")

if __name__ == '__main__':
    main()
