#!/usr/bin/env python3
"""
Debug script para investigar los datos de planta del SII.
"""

import pandas as pd

# Cargar datos originales del SII
df_orig = pd.read_csv('data/raw/sii_tablas/2025-09/sii_combinado.csv')

print("=== DATOS ORIGINALES DEL SII ===")
print(f"Total registros: {len(df_orig)}")
print("\nPor tipo:")
print(df_orig['tipo'].value_counts())

# Separar por tipo
planta = df_orig[df_orig['tipo'] == 'planta']
honorarios = df_orig[df_orig['tipo'] == 'honorarios']

print(f"\n=== DATOS DE PLANTA ===")
print(f"Total registros: {len(planta)}")

# Buscar columnas de sueldo
sueldo_cols = [col for col in planta.columns if any(word in col.lower() for word in ['sueldo', 'remuneracion', 'honorario', 'pago', 'bruto'])]
print(f"\nColumnas de sueldo disponibles: {sueldo_cols}")

# Revisar cada columna de sueldo
for col in sueldo_cols:
    print(f"\n--- {col} ---")
    non_null = planta[col].notna().sum()
    print(f"Valores no nulos: {non_null}")
    if non_null > 0:
        print(f"Primeros 5 valores: {planta[col].head().tolist()}")
        print(f"Valores únicos: {planta[col].unique()[:10]}")

print(f"\n=== DATOS DE HONORARIOS ===")
print(f"Total registros: {len(honorarios)}")

# Revisar honorarios
for col in sueldo_cols:
    print(f"\n--- {col} ---")
    non_null = honorarios[col].notna().sum()
    print(f"Valores no nulos: {non_null}")
    if non_null > 0:
        print(f"Primeros 5 valores: {honorarios[col].head().tolist()}")

print(f"\n=== COLUMNAS DISPONIBLES ===")
print("Todas las columnas:")
for i, col in enumerate(planta.columns):
    print(f"{i+1:2d}. {col}")

print(f"\n=== PRIMER REGISTRO DE PLANTA ===")
if len(planta) > 0:
    print(planta.iloc[0].to_dict())

