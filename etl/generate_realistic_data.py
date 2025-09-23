#!/usr/bin/env python3
"""
Genera datos sintéticos realistas de funcionarios públicos basados en las escalas del SII.
Crea variaciones que simulan funcionarios reales con diferentes organismos y cargos.
"""

import pandas as pd
import numpy as np
import random
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'

# Organismos públicos reales
ORGANISMOS = [
    'Presidencia de la República',
    'Ministerio de Hacienda',
    'Ministerio de Educación',
    'Ministerio de Salud',
    'Ministerio del Trabajo',
    'Ministerio del Interior',
    'Ministerio de Defensa',
    'Ministerio de Justicia',
    'Ministerio de Obras Públicas',
    'Ministerio de Vivienda',
    'Ministerio de Transportes',
    'Ministerio de Energía',
    'Ministerio del Medio Ambiente',
    'Ministerio de las Culturas',
    'Ministerio del Deporte',
    'Ministerio de la Mujer',
    'Ministerio de Ciencia',
    'Ministerio de Bienes Nacionales',
    'Ministerio de Minería',
    'Ministerio de Agricultura',
    'Servicio de Impuestos Internos',
    'Servicio Nacional de Aduanas',
    'Servicio de Registro Civil',
    'Servicio Nacional de Menores',
    'Servicio Nacional de Discapacidad',
    'Servicio Nacional del Consumidor',
    'Servicio Nacional de Turismo',
    'Servicio Nacional de Pesca',
    'Servicio Agrícola y Ganadero',
    'Servicio Nacional de Capacitación',
    'Gobierno Regional Metropolitano',
    'Gobierno Regional de Valparaíso',
    'Gobierno Regional del Biobío',
    'Gobierno Regional de La Araucanía',
    'Gobierno Regional de Los Lagos',
    'Gobierno Regional de Aysén',
    'Gobierno Regional de Magallanes',
    'Gobierno Regional de Los Ríos',
    'Gobierno Regional de Arica y Parinacota',
    'Gobierno Regional de Ñuble'
]

# Estamentos y sus características
ESTAMENTOS = {
    'Directivo': {
        'grados': list(range(1, 11)),
        'cargos': ['Director', 'Subdirector', 'Jefe de División', 'Jefe de Departamento', 'Coordinador'],
        'variacion_sueldo': 0.1  # 10% de variación
    },
    'Profesional': {
        'grados': list(range(1, 16)),
        'cargos': ['Profesional', 'Analista', 'Especialista', 'Consultor', 'Asesor'],
        'variacion_sueldo': 0.15  # 15% de variación
    },
    'Técnico': {
        'grados': list(range(1, 12)),
        'cargos': ['Técnico', 'Operador', 'Supervisor', 'Coordinador Técnico'],
        'variacion_sueldo': 0.2  # 20% de variación
    },
    'Administrativo': {
        'grados': list(range(1, 8)),
        'cargos': ['Administrativo', 'Secretario', 'Asistente', 'Auxiliar'],
        'variacion_sueldo': 0.25  # 25% de variación
    },
    'Fiscalizador': {
        'grados': list(range(1, 10)),
        'cargos': ['Fiscalizador', 'Inspector', 'Supervisor de Campo'],
        'variacion_sueldo': 0.15  # 15% de variación
    }
}

def generar_datos_sinteticos():
    """Genera datos sintéticos realistas de funcionarios."""
    logger.info("Generando datos sintéticos de funcionarios públicos...")
    
    # Leer escalas del SII
    sii_file = DATA_RAW / 'sii' / '2025-09' / 'escala.csv'
    if not sii_file.exists():
        logger.error("No se encontró el archivo de escalas del SII")
        return None
    
    df_sii = pd.read_csv(sii_file)
    logger.info(f"Escalas del SII cargadas: {len(df_sii)} registros")
    
    # Generar funcionarios sintéticos
    funcionarios = []
    
    # Generar múltiples funcionarios por cada escala
    for _, escala in df_sii.iterrows():
        estamento = escala['Estamento']
        grado = escala['Grado']
        sueldo_base = escala['Remuneracion Bruta Mensualizada']
        
        # Determinar cuántos funcionarios generar para esta escala
        if estamento == 'Directivo':
            num_funcionarios = random.randint(5, 15)  # Pocos directivos
        elif estamento == 'Profesional':
            num_funcionarios = random.randint(20, 50)  # Muchos profesionales
        elif estamento == 'Técnico':
            num_funcionarios = random.randint(15, 35)  # Técnicos moderados
        elif estamento == 'Administrativo':
            num_funcionarios = random.randint(10, 25)  # Administrativos moderados
        elif estamento == 'Fiscalizador':
            num_funcionarios = random.randint(8, 20)  # Pocos fiscalizadores
        else:
            num_funcionarios = random.randint(5, 15)
        
        # Generar funcionarios para esta escala
        for i in range(num_funcionarios):
            funcionario = generar_funcionario_individual(estamento, grado, sueldo_base)
            funcionarios.append(funcionario)
    
    # Crear DataFrame
    df_funcionarios = pd.DataFrame(funcionarios)
    
    logger.info(f"Generados {len(df_funcionarios)} funcionarios sintéticos")
    
    return df_funcionarios

def generar_funcionario_individual(estamento, grado, sueldo_base):
    """Genera un funcionario individual con variaciones realistas."""
    
    # Seleccionar organismo aleatorio
    organismo = random.choice(ORGANISMOS)
    
    # Seleccionar cargo basado en estamento
    if estamento in ESTAMENTOS:
        cargo = random.choice(ESTAMENTOS[estamento]['cargos'])
        variacion = ESTAMENTOS[estamento]['variacion_sueldo']
    else:
        cargo = 'Funcionario'
        variacion = 0.2
    
    # Generar nombre sintético
    nombres = ['Juan', 'María', 'Carlos', 'Ana', 'Luis', 'Carmen', 'Pedro', 'Elena', 'Miguel', 'Isabel']
    apellidos = ['González', 'Rodríguez', 'García', 'Martínez', 'López', 'Hernández', 'Pérez', 'Sánchez', 'Ramírez', 'Torres']
    
    nombre = f"{random.choice(nombres)} {random.choice(apellidos)}"
    
    # Calcular sueldo con variación
    variacion_porcentaje = random.uniform(-variacion, variacion)
    # Limpiar el sueldo base (eliminar separadores de miles)
    sueldo_base_limpio = str(sueldo_base).replace('.', '').replace(',', '.')
    sueldo_final = float(sueldo_base_limpio) * (1 + variacion_porcentaje)
    
    # Agregar variaciones adicionales por organismo
    if 'Ministerio' in organismo:
        sueldo_final *= random.uniform(0.95, 1.05)  # ±5% para ministerios
    elif 'Servicio' in organismo:
        sueldo_final *= random.uniform(0.90, 1.10)  # ±10% para servicios
    elif 'Gobierno Regional' in organismo:
        sueldo_final *= random.uniform(0.85, 1.15)  # ±15% para gobiernos regionales
    
    # Asegurar que el sueldo sea razonable
    sueldo_final = max(sueldo_final, 500000)  # Mínimo 500,000 pesos
    
    return {
        'organismo': organismo,
        'nombre': nombre,
        'cargo': cargo,
        'grado': grado,
        'estamento': estamento,
        'sueldo_bruto': round(sueldo_final),
        'fuente': 'sintetico_realista',
        'archivo_origen': 'generado_automaticamente',
        'fecha_procesamiento': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def main():
    """Función principal para generar datos sintéticos."""
    logger.info("Iniciando generación de datos sintéticos realistas")
    
    # Generar datos
    df_funcionarios = generar_datos_sinteticos()
    
    if df_funcionarios is not None:
        # Crear directorio de destino
        y_m = pd.Timestamp.now().strftime("%Y-%m")
        dest_dir = DATA_RAW / 'sinteticos' / y_m
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar datos
        output_file = dest_dir / 'funcionarios_sinteticos.csv'
        df_funcionarios.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Datos sintéticos guardados en {output_file}")
        
        # Mostrar resumen
        logger.info("📊 RESUMEN DE DATOS SINTÉTICOS:")
        logger.info(f"  📈 Total de funcionarios: {len(df_funcionarios):,}")
        logger.info(f"  🏛️ Organismos únicos: {df_funcionarios['organismo'].nunique()}")
        logger.info(f"  📋 Estamentos únicos: {df_funcionarios['estamento'].nunique()}")
        logger.info(f"  💰 Promedio sueldo: ${df_funcionarios['sueldo_bruto'].mean():,.0f}")
        logger.info(f"  💰 Mediana sueldo: ${df_funcionarios['sueldo_bruto'].median():,.0f}")
        logger.info(f"  💰 Rango sueldos: ${df_funcionarios['sueldo_bruto'].min():,.0f} - ${df_funcionarios['sueldo_bruto'].max():,.0f}")
        
        # Mostrar distribución por estamento
        logger.info("📋 Distribución por estamento:")
        for estamento, count in df_funcionarios['estamento'].value_counts().items():
            logger.info(f"  {estamento}: {count:,} funcionarios")
        
        # Mostrar distribución por organismo
        logger.info("🏛️ Top 10 organismos:")
        for org, count in df_funcionarios['organismo'].value_counts().head(10).items():
            logger.info(f"  {org}: {count:,} funcionarios")
        
        return df_funcionarios
    else:
        logger.error("No se pudieron generar datos sintéticos")
        return None

if __name__ == '__main__':
    main()
