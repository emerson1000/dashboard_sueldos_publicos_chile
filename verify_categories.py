#!/usr/bin/env python3
"""
Verifica la categorización correcta de organismos vs municipalidades.
"""

import pandas as pd
from pathlib import Path

def verify_categories():
    """Verifica la categorización de organismos."""
    data_file = Path("data/processed/datos_reales_consolidados.csv")
    
    if not data_file.exists():
        print("❌ Archivo de datos no encontrado")
        return
    
    df = pd.read_csv(data_file, low_memory=False)
    
    print("🔍 VERIFICACIÓN DE CATEGORIZACIÓN")
    print("=" * 50)
    
    # Verificar columnas
    print(f"📊 Total registros: {len(df):,}")
    print(f"📋 Columnas disponibles: {list(df.columns)}")
    
    # Verificar categorización
    if 'categoria_organismo' in df.columns:
        print("\n🏢 DISTRIBUCIÓN POR CATEGORÍA:")
        categoria_dist = df['categoria_organismo'].value_counts()
        for categoria, count in categoria_dist.items():
            print(f"  {categoria}: {count:,} registros")
        
        # Verificar organismos por categoría
        print("\n🔍 ORGANISMOS POR CATEGORÍA:")
        for categoria in categoria_dist.index:
            organismos_cat = df[df['categoria_organismo'] == categoria]['organismo'].unique()
            print(f"\n{categoria} ({len(organismos_cat)} organismos):")
            for org in sorted(organismos_cat)[:10]:  # Mostrar solo los primeros 10
                count = len(df[df['organismo'] == org])
                print(f"  - {org}: {count:,} registros")
            if len(organismos_cat) > 10:
                print(f"  ... y {len(organismos_cat) - 10} organismos más")
        
        # Verificar sueldos por categoría
        print("\n💰 SUELDOS POR CATEGORÍA:")
        sueldo_cat = df.groupby('categoria_organismo')['sueldo_bruto'].agg(['count', 'mean', 'median']).round(0)
        sueldo_cat.columns = ['Registros', 'Promedio', 'Mediana']
        print(sueldo_cat)
        
    else:
        print("❌ Columna 'categoria_organismo' no encontrada")
    
    # Verificar organismos específicos
    print("\n🏛️ TOP ORGANISMOS:")
    org_counts = df['organismo'].value_counts().head(10)
    for org, count in org_counts.items():
        print(f"  {org}: {count:,} registros")
    
    print("\n✅ Verificación completada")

if __name__ == "__main__":
    verify_categories()

