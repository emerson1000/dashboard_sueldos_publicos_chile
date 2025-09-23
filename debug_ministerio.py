#!/usr/bin/env python3
"""
Debug script para investigar los datos del ministerio_trabajo.
"""

import pandas as pd

# Cargar datos originales
df_orig = pd.read_csv('data/raw/consolidado/2025-09/todos_los_datos.csv')
mt_orig = df_orig[df_orig['organismo'] == 'ministerio_trabajo']

print("=== DATOS ORIGINALES DEL MINISTERIO TRABAJO ===")
print(f"Total registros: {len(mt_orig)}")
print("\nColumnas con datos no nulos:")
for col in mt_orig.columns:
    non_null = mt_orig[col].notna().sum()
    if non_null > 0:
        print(f"  {col}: {non_null} valores")

print("\n=== PRIMEROS 3 REGISTROS ===")
print(mt_orig.head(3).to_string())

print("\n=== COLUMNAS DE SUELDO ===")
sueldo_cols = [col for col in mt_orig.columns if any(word in col.lower() for word in ['sueldo', 'remuneracion', 'bruto'])]
for col in sueldo_cols:
    print(f"\n{col}:")
    print(f"  Valores no nulos: {mt_orig[col].notna().sum()}")
    if mt_orig[col].notna().sum() > 0:
        print(f"  Valores únicos: {mt_orig[col].unique()[:5]}")

print("\n=== DATOS PROCESADOS ===")
df_proc = pd.read_parquet('data/processed/sueldos_categorizados_small.parquet')
mt_proc = df_proc[df_proc['organismo'] == 'ministerio_trabajo']

print(f"Total registros procesados: {len(mt_proc)}")
print(f"Sueldo mínimo: ${mt_proc['sueldo_bruto'].min():,.0f}")
print(f"Sueldo máximo: ${mt_proc['sueldo_bruto'].max():,.0f}")
print(f"Sueldo promedio: ${mt_proc['sueldo_bruto'].mean():,.0f}")

print("\n=== COMPARACIÓN ===")
print("Los sueldos procesados parecen ser de años anteriores o en una unidad diferente.")
print("Sueldo mínimo actual en Chile: ~$500,000")
print("Sueldo mínimo en datos: $109,234")
print("Ratio: 500,000 / 109,234 =", round(500000/109234, 2))
