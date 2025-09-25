#!/usr/bin/env python3
"""
Debug script para investigar los datos de Concepción.
"""

import pandas as pd

print("=== INVESTIGACIÓN DATOS DE CONCEPCIÓN ===")

# Cargar datos filtrados
df = pd.read_parquet('data/processed/sueldos_filtrados_small.parquet')

# Buscar Concepción
concepcion = df[df['organismo'].str.contains('Concepción', case=False, na=False)]

print(f"Total registros de Concepción: {len(concepcion)}")

if len(concepcion) > 0:
    print("\nDetalles de Concepción:")
    print(concepcion[['organismo', 'estamento', 'sueldo_bruto', 'fuente']].to_string())
    
    print("\nSueldos:")
    print(f"Min: ${concepcion['sueldo_bruto'].min():,.0f}")
    print(f"Max: ${concepcion['sueldo_bruto'].max():,.0f}")
    print(f"Promedio: ${concepcion['sueldo_bruto'].mean():,.0f}")
else:
    print("No se encontraron datos de Concepción")

# Buscar en datos originales (antes del filtrado)
print("\n=== BUSCANDO EN DATOS ORIGINALES ===")

# Cargar datos categorizados (antes del filtrado)
df_orig = pd.read_parquet('data/processed/sueldos_categorizados_small.parquet')
concepcion_orig = df_orig[df_orig['organismo'].str.contains('Concepción', case=False, na=False)]

print(f"Total registros de Concepción (originales): {len(concepcion_orig)}")

if len(concepcion_orig) > 0:
    print("\nDetalles originales:")
    print(concepcion_orig[['organismo', 'estamento', 'sueldo_bruto', 'fuente']].to_string())
    
    print("\nSueldos originales:")
    print(f"Min: ${concepcion_orig['sueldo_bruto'].min():,.0f}")
    print(f"Max: ${concepcion_orig['sueldo_bruto'].max():,.0f}")
    print(f"Promedio: ${concepcion_orig['sueldo_bruto'].mean():,.0f}")
    
    # Ver cuántos se perdieron
    perdidos = len(concepcion_orig) - len(concepcion)
    print(f"\nRegistros perdidos por filtrado: {perdidos}")
    
    if perdidos > 0:
        print("\nRegistros que se perdieron:")
        perdidos_df = concepcion_orig[~concepcion_orig.index.isin(concepcion.index)]
        print(perdidos_df[['organismo', 'estamento', 'sueldo_bruto', 'fuente']].to_string())

# Buscar todas las municipalidades que contengan "concepción" o "conce"
print("\n=== BUSCANDO VARIACIONES DE CONCEPCIÓN ===")
variaciones = df[df['organismo'].str.contains('conce', case=False, na=False)]
print(f"Municipalidades con 'conce': {len(variaciones)}")
if len(variaciones) > 0:
    print(variaciones['organismo'].unique())

# Buscar en datos completos (no solo small)
print("\n=== BUSCANDO EN DATOS COMPLETOS ===")
try:
    df_completo = pd.read_parquet('data/processed/sueldos_filtrados.parquet')
    concepcion_completo = df_completo[df_completo['organismo'].str.contains('Concepción', case=False, na=False)]
    print(f"Total registros de Concepción (completos): {len(concepcion_completo)}")
    
    if len(concepcion_completo) > 0:
        print("\nDetalles completos:")
        print(concepcion_completo[['organismo', 'estamento', 'sueldo_bruto', 'fuente']].head(10).to_string())
        
        print("\nSueldos completos:")
        print(f"Min: ${concepcion_completo['sueldo_bruto'].min():,.0f}")
        print(f"Max: ${concepcion_completo['sueldo_bruto'].max():,.0f}")
        print(f"Promedio: ${concepcion_completo['sueldo_bruto'].mean():,.0f}")
        print(f"Total funcionarios: {len(concepcion_completo)}")
        
except Exception as e:
    print(f"Error cargando datos completos: {e}")

