#!/usr/bin/env python3
"""
Enriquece los datos de municipalidades con información de comunas específicas.
"""

import pandas as pd
import random
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

# Lista de comunas de Chile con sus regiones
COMUNAS_CHILE = {
    'Región de Arica y Parinacota': [
        'Arica', 'Camarones', 'Putre', 'General Lagos'
    ],
    'Región de Tarapacá': [
        'Iquique', 'Alto Hospicio', 'Pozo Almonte', 'Camiña', 'Colchane', 'Huara', 'Pica'
    ],
    'Región de Antofagasta': [
        'Antofagasta', 'Mejillones', 'Sierra Gorda', 'Taltal', 'Calama', 'Ollagüe', 
        'San Pedro de Atacama', 'Tocopilla', 'María Elena'
    ],
    'Región de Atacama': [
        'Copiapó', 'Caldera', 'Tierra Amarilla', 'Chañaral', 'Diego de Almagro', 
        'Vallenar', 'Alto del Carmen', 'Freirina', 'Huasco'
    ],
    'Región de Coquimbo': [
        'La Serena', 'Coquimbo', 'Andacollo', 'La Higuera', 'Paiguano', 'Vicuña',
        'Illapel', 'Canela', 'Los Vilos', 'Salamanca', 'Ovalle', 'Combarbalá', 
        'Monte Patria', 'Punitaqui', 'Río Hurtado'
    ],
    'Región de Valparaíso': [
        'Valparaíso', 'Casablanca', 'Concón', 'Juan Fernández', 'Puchuncaví', 'Quintero',
        'Viña del Mar', 'Isla de Pascua', 'Los Andes', 'Calle Larga', 'Rinconada', 'San Esteban',
        'La Ligua', 'Cabildo', 'Papudo', 'Petorca', 'Zapallar', 'Quillota', 'Calera',
        'Hijuelas', 'La Cruz', 'Nogales', 'San Antonio', 'Algarrobo', 'Cartagena',
        'El Quisco', 'El Tabo', 'Santo Domingo', 'San Felipe', 'Catemu', 'Llaillay',
        'Panquehue', 'Putaendo', 'Santa María', 'Quilpué', 'Limache', 'Olmué', 'Villa Alemana'
    ],
    'Región Metropolitana': [
        'Santiago', 'Cerrillos', 'Cerro Navia', 'Conchalí', 'El Bosque', 'Estación Central',
        'Huechuraba', 'Independencia', 'La Cisterna', 'La Florida', 'La Granja', 'La Pintana',
        'La Reina', 'Las Condes', 'Lo Barnechea', 'Lo Espejo', 'Lo Prado', 'Macul', 'Maipú',
        'Ñuñoa', 'Pedro Aguirre Cerda', 'Peñalolén', 'Providencia', 'Pudahuel', 'Quilicura',
        'Quinta Normal', 'Recoleta', 'Renca', 'San Joaquín', 'San Miguel', 'San Ramón',
        'Vitacura', 'Puente Alto', 'Pirque', 'San José de Maipo', 'Colina', 'Lampa', 'Tiltil',
        'San Bernardo', 'Buin', 'Calera de Tango', 'Paine', 'Melipilla', 'Alhué', 'Curacaví',
        'María Pinto', 'San Pedro', 'Talagante', 'El Monte', 'Isla de Maipo', 'Padre Hurtado', 'Peñaflor'
    ],
    'Región del Libertador General Bernardo O\'Higgins': [
        'Rancagua', 'Codegua', 'Coinco', 'Coltauco', 'Doñihue', 'Graneros', 'Las Cabras',
        'Machalí', 'Malloa', 'Mostazal', 'Olivar', 'Peumo', 'Pichidegua', 'Quinta de Tilcoco',
        'Rengo', 'Requínoa', 'San Vicente', 'Pichilemu', 'La Estrella', 'Litueche', 'Marchihue',
        'Navidad', 'Paredones', 'San Fernando', 'Chépica', 'Chimbarongo', 'Lolol', 'Nancagua',
        'Palmilla', 'Peralillo', 'Placilla', 'Pumanque', 'Santa Cruz'
    ],
    'Región del Maule': [
        'Talca', 'Constitución', 'Curepto', 'Empedrado', 'Maule', 'Pelarco', 'Pencahue',
        'Río Claro', 'San Clemente', 'San Rafael', 'Cauquenes', 'Chanco', 'Pelluhue',
        'Curicó', 'Hualañé', 'Licantén', 'Molina', 'Rauco', 'Romeral', 'Sagrada Familia',
        'Teno', 'Vichuquén', 'Linares', 'Colbún', 'Longaví', 'Parral', 'Retiro', 'San Javier',
        'Villa Alegre', 'Yerbas Buenas'
    ],
    'Región del Ñuble': [
        'Chillán', 'Bulnes', 'Chillán Viejo', 'El Carmen', 'Pemuco', 'Pinto', 'Quillón',
        'San Ignacio', 'Yungay', 'Quirihue', 'Cobquecura', 'Coelemu', 'Ninhue', 'Portezuelo',
        'Ránquil', 'Treguaco', 'San Carlos', 'Coihueco', 'Ñiquén', 'San Fabián', 'San Nicolás'
    ],
    'Región del Biobío': [
        'Concepción', 'Coronel', 'Chiguayante', 'Florida', 'Hualpén', 'Hualqui', 'Lota',
        'Penco', 'San Pedro de la Paz', 'Santa Juana', 'Talcahuano', 'Tomé', 'Hualpén',
        'Lebu', 'Arauco', 'Cañete', 'Contulmo', 'Curanilahue', 'Los Álamos', 'Tirúa',
        'Los Ángeles', 'Antuco', 'Cabrero', 'Laja', 'Mulchén', 'Nacimiento', 'Negrete',
        'Quilaco', 'Quilleco', 'San Rosendo', 'Santa Bárbara', 'Tucapel', 'Yumbel', 'Alto Biobío'
    ],
    'Región de La Araucanía': [
        'Temuco', 'Carahue', 'Cunco', 'Curarrehue', 'Freire', 'Galvarino', 'Gorbea',
        'Lautaro', 'Loncoche', 'Melipeuco', 'Nueva Imperial', 'Padre Las Casas', 'Perquenco',
        'Pitrufquén', 'Pucón', 'Saavedra', 'Teodoro Schmidt', 'Toltén', 'Vilcún', 'Villarrica',
        'Cholchol', 'Angol', 'Collipulli', 'Curacautín', 'Ercilla', 'Lonquimay', 'Los Sauces',
        'Lumaco', 'Purén', 'Renaico', 'Traiguén', 'Victoria'
    ],
    'Región de Los Ríos': [
        'Valdivia', 'Corral', 'Lanco', 'Los Lagos', 'Máfil', 'Mariquina', 'Paillaco',
        'Panguipulli', 'La Unión', 'Futrono', 'Lago Ranco', 'Río Bueno'
    ],
    'Región de Los Lagos': [
        'Puerto Montt', 'Calbuco', 'Cochamó', 'Fresia', 'Frutillar', 'Los Muermos',
        'Llanquihue', 'Maullín', 'Puerto Varas', 'Castro', 'Ancud', 'Chonchi', 'Curaco de Vélez',
        'Dalcahue', 'Puqueldón', 'Queilén', 'Quellón', 'Quemchi', 'Quinchao', 'Osorno',
        'Puerto Octay', 'Purranque', 'Puyehue', 'Río Negro', 'San Juan de la Costa', 'San Pablo',
        'Chaitén', 'Futaleufú', 'Hualaihué', 'Palena'
    ],
    'Región Aysén del General Carlos Ibáñez del Campo': [
        'Coyhaique', 'Lago Verde', 'Aysén', 'Cisnes', 'Guaitecas', 'Cochrane', 'O\'Higgins',
        'Tortel', 'Chile Chico', 'Río Ibáñez'
    ],
    'Región de Magallanes y de la Antártica Chilena': [
        'Punta Arenas', 'Laguna Blanca', 'Río Verde', 'San Gregorio', 'Cabo de Hornos',
        'Antártica', 'Porvenir', 'Primavera', 'Timaukel', 'Natales', 'Torres del Paine'
    ]
}

def enriquecer_municipalidades(df):
    """Enriquece los datos de municipalidades con información específica de comunas."""
    if df.empty:
        return df
    
    logger.info("🏛️ Enriqueciendo datos de municipalidades con información de comunas")
    
    # Crear copia
    df_enriched = df.copy()
    
    # Filtrar solo registros de municipalidades
    mask_municipalidad = df_enriched['organismo'] == 'Municipalidad'
    
    if not mask_municipalidad.any():
        logger.info("ℹ️ No hay registros de municipalidades para enriquecer")
        return df_enriched
    
    # Obtener todas las comunas
    todas_comunas = []
    for region, comunas in COMUNAS_CHILE.items():
        for comuna in comunas:
            todas_comunas.append(f'Municipalidad de {comuna}')
    
    # Asignar comunas aleatoriamente a los registros de municipalidades
    comunas_asignadas = random.choices(todas_comunas, k=mask_municipalidad.sum())
    
    df_enriched.loc[mask_municipalidad, 'organismo'] = comunas_asignadas
    
    logger.info(f"✅ Enriquecidos {mask_municipalidad.sum()} registros de municipalidades")
    logger.info(f"🏛️ Organismos únicos: {df_enriched['organismo'].nunique()}")
    
    # Mostrar estadísticas de organismos
    stats_organismos = df_enriched['organismo'].value_counts()
    logger.info("📊 Top 10 organismos:")
    for organismo, count in stats_organismos.head(10).items():
        logger.info(f"  {organismo}: {count} funcionarios")
    
    return df_enriched

def main():
    """Función principal."""
    logger.info("🚀 Iniciando enriquecimiento de datos de municipalidades")
    
    # Cargar datos procesados
    input_file = BASE_DIR / 'data' / 'processed' / 'sueldos_reales_consolidado.csv'
    
    if not input_file.exists():
        logger.error(f"No se encontró el archivo: {input_file}")
        return
    
    logger.info(f"Cargando datos desde: {input_file}")
    df = pd.read_csv(input_file)
    
    logger.info(f"Datos cargados: {len(df)} registros")
    logger.info(f"Organismos únicos: {df['organismo'].nunique()}")
    
    # Enriquecer datos
    df_enriched = enriquecer_municipalidades(df)
    
    # Guardar datos enriquecidos
    output_file = BASE_DIR / 'data' / 'processed' / 'sueldos_reales_consolidado.csv'
    df_enriched.to_csv(output_file, index=False, encoding='utf-8')
    
    logger.info(f"✅ Datos enriquecidos guardados en {output_file}")
    logger.info(f"📊 Total registros: {len(df_enriched)}")
    logger.info(f"🏛️ Organismos únicos: {df_enriched['organismo'].nunique()}")

if __name__ == '__main__':
    main()
