#!/usr/bin/env python3
"""
Obtiene URLs reales de transparencia activa con datos de remuneraciones.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
from pathlib import Path
import logging
from urllib.parse import urljoin, urlparse
import re

logger = logging.getLogger(__name__)

class RealTransparenciaURLs:
    """Obtiene URLs reales de transparencia activa."""
    
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.output_dir = self.base_dir / 'data' / 'processed'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # URLs base de organismos públicos chilenos con transparencia activa
        self.organismos_base = [
            # Ministerios con transparencia activa real
            {'nombre': 'Ministerio de Hacienda', 'url': 'https://www.hacienda.cl/transparencia/', 'codigo': 'MH'},
            {'nombre': 'Ministerio de Educación', 'url': 'https://www.mineduc.cl/transparencia/', 'codigo': 'ME'},
            {'nombre': 'Ministerio de Salud', 'url': 'https://www.minsal.cl/transparencia/', 'codigo': 'MS'},
            {'nombre': 'Ministerio del Trabajo', 'url': 'https://www.mintrab.gob.cl/transparencia/', 'codigo': 'MT'},
            {'nombre': 'Ministerio del Interior', 'url': 'https://www.interior.gob.cl/transparencia/', 'codigo': 'MI'},
            {'nombre': 'Ministerio de Defensa', 'url': 'https://www.defensa.cl/transparencia/', 'codigo': 'MD'},
            {'nombre': 'Ministerio de Justicia', 'url': 'https://www.minjusticia.gob.cl/transparencia/', 'codigo': 'MJ'},
            {'nombre': 'Ministerio de Obras Públicas', 'url': 'https://www.mop.cl/transparencia/', 'codigo': 'MOP'},
            {'nombre': 'Ministerio de Vivienda', 'url': 'https://www.minvu.cl/transparencia/', 'codigo': 'MV'},
            {'nombre': 'Ministerio de Transportes', 'url': 'https://www.mtt.gob.cl/transparencia/', 'codigo': 'MTT'},
            {'nombre': 'Ministerio de Energía', 'url': 'https://www.energia.gob.cl/transparencia/', 'codigo': 'ME'},
            {'nombre': 'Ministerio del Medio Ambiente', 'url': 'https://www.mma.gob.cl/transparencia/', 'codigo': 'MMA'},
            {'nombre': 'Ministerio de las Culturas', 'url': 'https://www.cultura.gob.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Ministerio del Deporte', 'url': 'https://www.mindep.cl/transparencia/', 'codigo': 'MD'},
            {'nombre': 'Ministerio de la Mujer', 'url': 'https://www.minmujeryeg.gob.cl/transparencia/', 'codigo': 'MM'},
            {'nombre': 'Ministerio de Ciencia', 'url': 'https://www.minciencia.gob.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Ministerio de Bienes Nacionales', 'url': 'https://www.bienesnacionales.cl/transparencia/', 'codigo': 'MBN'},
            {'nombre': 'Ministerio de Minería', 'url': 'https://www.minmineria.cl/transparencia/', 'codigo': 'MM'},
            {'nombre': 'Ministerio de Agricultura', 'url': 'https://www.minagri.gob.cl/transparencia/', 'codigo': 'MA'},
            
            # Servicios públicos importantes
            {'nombre': 'Servicio de Impuestos Internos', 'url': 'https://www.sii.cl/transparencia/', 'codigo': 'SII'},
            {'nombre': 'Servicio Nacional de Aduanas', 'url': 'https://www.aduana.cl/transparencia/', 'codigo': 'SNA'},
            {'nombre': 'Servicio Nacional del Consumidor', 'url': 'https://www.sernac.cl/transparencia/', 'codigo': 'SERNAC'},
            {'nombre': 'Servicio Nacional de Capacitación', 'url': 'https://www.sence.cl/transparencia/', 'codigo': 'SENCE'},
            {'nombre': 'Servicio Nacional de Turismo', 'url': 'https://www.sernatur.cl/transparencia/', 'codigo': 'SERNATUR'},
            {'nombre': 'Servicio Nacional de Pesca', 'url': 'https://www.sernapesca.cl/transparencia/', 'codigo': 'SERNAPESCA'},
            {'nombre': 'Servicio Agrícola y Ganadero', 'url': 'https://www.sag.gob.cl/transparencia/', 'codigo': 'SAG'},
            {'nombre': 'Servicio Nacional de Geología', 'url': 'https://www.sernageomin.cl/transparencia/', 'codigo': 'SERNAGEOMIN'},
            {'nombre': 'Servicio Nacional de Menores', 'url': 'https://www.sename.cl/transparencia/', 'codigo': 'SENAME'},
            {'nombre': 'Servicio Nacional de la Discapacidad', 'url': 'https://www.senadis.gob.cl/transparencia/', 'codigo': 'SENADIS'},
            
            # Municipalidades principales
            {'nombre': 'Municipalidad de Santiago', 'url': 'https://www.municipalidadsantiago.cl/transparencia/', 'codigo': 'MS'},
            {'nombre': 'Municipalidad de Valparaíso', 'url': 'https://www.munivalpo.cl/transparencia/', 'codigo': 'MV'},
            {'nombre': 'Municipalidad de Viña del Mar', 'url': 'https://www.vinadelmar.cl/transparencia/', 'codigo': 'VDM'},
            {'nombre': 'Municipalidad de Concepción', 'url': 'https://www.concepcion.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Temuco', 'url': 'https://www.temuco.cl/transparencia/', 'codigo': 'MT'},
            {'nombre': 'Municipalidad de Antofagasta', 'url': 'https://www.muniantofagasta.cl/transparencia/', 'codigo': 'MA'},
            {'nombre': 'Municipalidad de Valdivia', 'url': 'https://www.munivaldivia.cl/transparencia/', 'codigo': 'MV'},
            {'nombre': 'Municipalidad de Iquique', 'url': 'https://www.muniquique.cl/transparencia/', 'codigo': 'MI'},
            {'nombre': 'Municipalidad de La Serena', 'url': 'https://www.laserena.cl/transparencia/', 'codigo': 'LS'},
            {'nombre': 'Municipalidad de Rancagua', 'url': 'https://www.rancagua.cl/transparencia/', 'codigo': 'MR'},
            {'nombre': 'Municipalidad de Talca', 'url': 'https://www.talca.cl/transparencia/', 'codigo': 'MT'},
            {'nombre': 'Municipalidad de Chillán', 'url': 'https://www.chillan.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Osorno', 'url': 'https://www.osorno.cl/transparencia/', 'codigo': 'MO'},
            {'nombre': 'Municipalidad de Puerto Montt', 'url': 'https://www.puertomontt.cl/transparencia/', 'codigo': 'PM'},
            {'nombre': 'Municipalidad de Arica', 'url': 'https://www.arica.cl/transparencia/', 'codigo': 'MA'},
            {'nombre': 'Municipalidad de Calama', 'url': 'https://www.calama.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Copiapó', 'url': 'https://www.copiapo.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Coquimbo', 'url': 'https://www.coquimbo.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Quillota', 'url': 'https://www.quillota.cl/transparencia/', 'codigo': 'MQ'},
            {'nombre': 'Municipalidad de San Antonio', 'url': 'https://www.sanantonio.cl/transparencia/', 'codigo': 'SA'},
            {'nombre': 'Municipalidad de Curicó', 'url': 'https://www.curico.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Los Ángeles', 'url': 'https://www.losangeles.cl/transparencia/', 'codigo': 'LA'},
            {'nombre': 'Municipalidad de Chillán Viejo', 'url': 'https://www.chillanviejo.cl/transparencia/', 'codigo': 'CV'},
            {'nombre': 'Municipalidad de Villa Alemana', 'url': 'https://www.villaalemana.cl/transparencia/', 'codigo': 'VA'},
            {'nombre': 'Municipalidad de Quilpué', 'url': 'https://www.quilpue.cl/transparencia/', 'codigo': 'MQ'},
            {'nombre': 'Municipalidad de Maipú', 'url': 'https://www.maipu.cl/transparencia/', 'codigo': 'MM'},
            {'nombre': 'Municipalidad de Puente Alto', 'url': 'https://www.puentealto.cl/transparencia/', 'codigo': 'PA'},
            {'nombre': 'Municipalidad de Las Condes', 'url': 'https://www.lascondes.cl/transparencia/', 'codigo': 'LC'},
            {'nombre': 'Municipalidad de Providencia', 'url': 'https://www.providencia.cl/transparencia/', 'codigo': 'MP'},
            {'nombre': 'Municipalidad de Ñuñoa', 'url': 'https://www.nunoa.cl/transparencia/', 'codigo': 'MN'},
            {'nombre': 'Municipalidad de La Reina', 'url': 'https://www.lareina.cl/transparencia/', 'codigo': 'LR'},
            {'nombre': 'Municipalidad de Macul', 'url': 'https://www.macul.cl/transparencia/', 'codigo': 'MM'},
            {'nombre': 'Municipalidad de San Joaquín', 'url': 'https://www.sanjoaquin.cl/transparencia/', 'codigo': 'SJ'},
            {'nombre': 'Municipalidad de La Florida', 'url': 'https://www.laflorida.cl/transparencia/', 'codigo': 'LF'},
            {'nombre': 'Municipalidad de Peñalolén', 'url': 'https://www.penalolen.cl/transparencia/', 'codigo': 'MP'},
            {'nombre': 'Municipalidad de San Miguel', 'url': 'https://www.sanmiguel.cl/transparencia/', 'codigo': 'SM'},
            {'nombre': 'Municipalidad de La Granja', 'url': 'https://www.lagranja.cl/transparencia/', 'codigo': 'LG'},
            {'nombre': 'Municipalidad de El Bosque', 'url': 'https://www.elbosque.cl/transparencia/', 'codigo': 'EB'},
            {'nombre': 'Municipalidad de Pedro Aguirre Cerda', 'url': 'https://www.pedroaguirrecerda.cl/transparencia/', 'codigo': 'PAC'},
            {'nombre': 'Municipalidad de Lo Espejo', 'url': 'https://www.loespejo.cl/transparencia/', 'codigo': 'LE'},
            {'nombre': 'Municipalidad de Estación Central', 'url': 'https://www.estacioncentral.cl/transparencia/', 'codigo': 'EC'},
            {'nombre': 'Municipalidad de Cerrillos', 'url': 'https://www.cerrillos.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Pudahuel', 'url': 'https://www.pudahuel.cl/transparencia/', 'codigo': 'MP'},
            {'nombre': 'Municipalidad de Cerro Navia', 'url': 'https://www.cerronavia.cl/transparencia/', 'codigo': 'CN'},
            {'nombre': 'Municipalidad de Lo Prado', 'url': 'https://www.loprado.cl/transparencia/', 'codigo': 'LP'},
            {'nombre': 'Municipalidad de Quinta Normal', 'url': 'https://www.quintanormal.cl/transparencia/', 'codigo': 'QN'},
            {'nombre': 'Municipalidad de Renca', 'url': 'https://www.renca.cl/transparencia/', 'codigo': 'MR'},
            {'nombre': 'Municipalidad de Huechuraba', 'url': 'https://www.huechuraba.cl/transparencia/', 'codigo': 'MH'},
            {'nombre': 'Municipalidad de Conchalí', 'url': 'https://www.conchali.cl/transparencia/', 'codigo': 'MC'},
            {'nombre': 'Municipalidad de Independencia', 'url': 'https://www.independencia.cl/transparencia/', 'codigo': 'MI'},
            {'nombre': 'Municipalidad de Recoleta', 'url': 'https://www.recoleta.cl/transparencia/', 'codigo': 'MR'},
        ]
        
        # Patrones para identificar datos de remuneraciones
        self.remuneracion_patterns = [
            r'remuneraci[oó]n',
            r'sueldo',
            r'salario',
            r'bruto',
            r'l[ií]quido',
            r'monto',
            r'honorario',
            r'asignaci[oó]n',
            r'bonificaci[oó]n',
            r'gratificaci[oó]n',
            r'personal',
            r'funcionario',
            r'empleado',
            r'planta',
            r'contrata'
        ]
    
    def get_real_urls(self, organismo_info):
        """Obtiene URLs reales de transparencia activa para un organismo."""
        organismo = organismo_info['nombre']
        url_base = organismo_info['url']
        
        logger.info(f"Obteniendo URLs reales para {organismo}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            response = requests.get(url_base, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar enlaces relacionados con remuneraciones
            remuneracion_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text().strip().lower()
                
                # Verificar si el enlace es relevante
                if any(re.search(pattern, text) for pattern in self.remuneracion_patterns):
                    full_url = urljoin(url_base, href)
                    remuneracion_links.append({
                        'url': full_url,
                        'text': link.get_text().strip(),
                        'organismo': organismo,
                        'codigo': organismo_info['codigo']
                    })
            
            # También buscar enlaces a archivos de datos
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv', '.pdf']):
                    full_url = urljoin(url_base, href)
                    remuneracion_links.append({
                        'url': full_url,
                        'text': link.get_text().strip(),
                        'organismo': organismo,
                        'codigo': organismo_info['codigo']
                    })
            
            logger.info(f"Encontrados {len(remuneracion_links)} enlaces relevantes en {organismo}")
            return remuneracion_links
            
        except Exception as e:
            logger.error(f"Error obteniendo URLs para {organismo}: {e}")
            return []
    
    def process_all_organismos(self):
        """Procesa todos los organismos para obtener URLs reales."""
        logger.info("Obteniendo URLs reales de transparencia activa")
        
        all_urls = []
        
        for organismo_info in self.organismos_base:
            urls = self.get_real_urls(organismo_info)
            all_urls.extend(urls)
            
            # Pausa entre organismos
            time.sleep(2)
        
        # Guardar URLs encontradas
        if all_urls:
            df_urls = pd.DataFrame(all_urls)
            output_file = self.output_dir / 'urls_transparencia_real.csv'
            df_urls.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"URLs guardadas en {output_file}")
            logger.info(f"Total URLs encontradas: {len(all_urls)}")
            
            # Mostrar resumen por organismo
            organismo_counts = df_urls['organismo'].value_counts()
            logger.info("URLs por organismo:")
            for organismo, count in organismo_counts.head(10).items():
                logger.info(f"  {organismo}: {count} URLs")
            
            return df_urls
        else:
            logger.warning("No se encontraron URLs de remuneraciones")
            return pd.DataFrame()
    
    def get_portal_transparencia_urls(self):
        """Obtiene URLs del Portal de Transparencia central."""
        logger.info("Obteniendo URLs del Portal de Transparencia")
        
        portal_urls = []
        
        # URL del portal de transparencia
        portal_base = "https://www.portaltransparencia.cl"
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            response = requests.get(portal_base, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar enlaces a organismos
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text().strip()
                
                if 'organismo' in href.lower() or 'institucion' in href.lower():
                    full_url = urljoin(portal_base, href)
                    portal_urls.append({
                        'url': full_url,
                        'text': text,
                        'organismo': 'Portal Transparencia',
                        'codigo': 'PT'
                    })
            
            logger.info(f"Encontradas {len(portal_urls)} URLs del portal")
            return portal_urls
            
        except Exception as e:
            logger.error(f"Error obteniendo URLs del portal: {e}")
            return []

def main():
    """Función principal."""
    logger.info("Iniciando obtención de URLs reales de transparencia activa")
    
    url_getter = RealTransparenciaURLs()
    
    # Obtener URLs de organismos individuales
    df_organismos = url_getter.process_all_organismos()
    
    # Obtener URLs del portal central
    portal_urls = url_getter.get_portal_transparencia_urls()
    
    if portal_urls:
        df_portal = pd.DataFrame(portal_urls)
        output_file = url_getter.output_dir / 'urls_portal_transparencia.csv'
        df_portal.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"URLs del portal guardadas en {output_file}")
    
    # Combinar todas las URLs
    all_urls = []
    if not df_organismos.empty:
        all_urls.extend(df_organismos.to_dict('records'))
    if portal_urls:
        all_urls.extend(portal_urls)
    
    if all_urls:
        df_all = pd.DataFrame(all_urls)
        output_file = url_getter.output_dir / 'urls_transparencia_completas.csv'
        df_all.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Total URLs encontradas: {len(all_urls)}")
        logger.info(f"URLs completas guardadas en {output_file}")
        
        # Mostrar resumen
        print("\n" + "="*60)
        print("📊 RESUMEN DE URLs ENCONTRADAS")
        print("="*60)
        print(f"Total URLs: {len(all_urls)}")
        print(f"Organismos únicos: {df_all['organismo'].nunique()}")
        
        print("\n🏆 TOP ORGANISMOS CON MÁS URLs:")
        organismo_counts = df_all['organismo'].value_counts().head(10)
        for organismo, count in organismo_counts.items():
            print(f"  {organismo}: {count} URLs")
        
        print("="*60)

if __name__ == '__main__':
    main()

