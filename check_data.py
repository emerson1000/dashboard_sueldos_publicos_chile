#!/usr/bin/env python3
import pandas as pd

# Cargar datos
df = pd.read_csv('data/processed/datos_reales_consolidados.csv', low_memory=False)

print(f"Total registros originales: {len(df):,}")
print(f"Registros con sueldo_bruto válido: {len(df.dropna(subset=['sueldo_bruto'])):,}")

# Convertir sueldo_bruto a numérico
df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
df_clean = df.dropna(subset=['sueldo_bruto'])

print(f"Registros después de conversión numérica: {len(df_clean):,}")

# Aplicar filtro de sueldos razonables
df_filtered = df_clean[(df_clean['sueldo_bruto'] >= 200000) & (df_clean['sueldo_bruto'] <= 50000000)]
print(f"Registros después de filtro 200K-50M: {len(df_filtered):,}")

print(f"\nEstadísticas de sueldo_bruto:")
print(f"Min: ${df_clean['sueldo_bruto'].min():,.0f}")
print(f"Max: ${df_clean['sueldo_bruto'].max():,.0f}")
print(f"Promedio: ${df_clean['sueldo_bruto'].mean():,.0f}")

print(f"\nRegistros fuera del rango:")
print(f"Sueldos < $200K: {len(df_clean[df_clean['sueldo_bruto'] < 200000]):,}")
print(f"Sueldos > $50M: {len(df_clean[df_clean['sueldo_bruto'] > 50000000]):,}")
