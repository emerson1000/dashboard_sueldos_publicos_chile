#!/usr/bin/env python3
"""
Compara datos de municipalidades antes y después del filtrado.
"""

import pandas as pd

print("=== COMPARACION MUNICIPALIDADES ===")

# Cargar datos anteriores (categorizados)
df_old = pd.read_parquet('data/processed/sueldos_categorizados_small.parquet')
mun_old = df_old[df_old['categoria_organismo'] == 'Municipalidades']

print("ANTES (categorizados):")
print(f"  Total funcionarios: {len(mun_old)}")
print(f"  Municipalidades únicas: {mun_old['organismo'].nunique()}")
print(f"  Estamentos únicos: {mun_old['estamento'].nunique()}")

# Cargar datos nuevos (filtrados)
df_new = pd.read_parquet('data/processed/sueldos_filtrados_small.parquet')
mun_new = df_new[df_new['categoria_organismo'] == 'Municipalidades']

print("\nDESPUÉS (filtrados):")
print(f"  Total funcionarios: {len(mun_new)}")
print(f"  Municipalidades únicas: {mun_new['organismo'].nunique()}")
print(f"  Estamentos únicos: {mun_new['estamento'].nunique()}")

print("\nDIFERENCIA:")
print(f"  Funcionarios perdidos: {len(mun_old) - len(mun_new)}")
print(f"  Municipalidades perdidas: {mun_old['organismo'].nunique() - mun_new['organismo'].nunique()}")

print("\n=== ESTAMENTOS ANTES ===")
print(mun_old['estamento'].value_counts())

print("\n=== ESTAMENTOS DESPUÉS ===")
print(mun_new['estamento'].value_counts())

print("\n=== MUNICIPALIDADES PERDIDAS ===")
mun_old_set = set(mun_old['organismo'].unique())
mun_new_set = set(mun_new['organismo'].unique())
perdidas = mun_old_set - mun_new_set
print(f"Municipalidades perdidas: {len(perdidas)}")
if perdidas:
    for mun in sorted(perdidas)[:10]:  # Mostrar primeras 10
        count_old = len(mun_old[mun_old['organismo'] == mun])
        print(f"  {mun}: {count_old} funcionarios perdidos")

print("\n=== ANÁLISIS DE SUELDOS ===")
print("Sueldos mínimos:")
print(f"  Antes: ${mun_old['sueldo_bruto'].min():,.0f}")
print(f"  Después: ${mun_new['sueldo_bruto'].min():,.0f}")

print("Sueldos máximos:")
print(f"  Antes: ${mun_old['sueldo_bruto'].max():,.0f}")
print(f"  Después: ${mun_new['sueldo_bruto'].max():,.0f}")

print("Sueldos promedio:")
print(f"  Antes: ${mun_old['sueldo_bruto'].mean():,.0f}")
print(f"  Después: ${mun_new['sueldo_bruto'].mean():,.0f}")


