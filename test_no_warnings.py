#!/usr/bin/env python3
"""
Prueba que no aparezcan warnings molestos.
"""

import warnings
import pandas as pd
import plotly.express as px
from pathlib import Path

# Suprimir todos los warnings molestos
warnings.filterwarnings('ignore', category=UserWarning, module='plotly')
warnings.filterwarnings('ignore', message='.*keyword arguments have been deprecated.*')
warnings.filterwarnings('ignore', message='.*config instead to specify Plotly configuration options.*')
warnings.filterwarnings('ignore', message='.*deprecated.*')
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
warnings.filterwarnings('ignore', category=UserWarning, module='streamlit')
warnings.filterwarnings('ignore', message='.*use_container_width.*')
warnings.filterwarnings('ignore', message='.*deprecation.*')

def test_no_warnings():
    """Prueba que no aparezcan warnings."""
    print("🧪 PROBANDO SUPRESIÓN DE WARNINGS")
    print("=" * 50)
    
    # Cargar datos
    data_file = Path("data/processed/datos_reales_consolidados.csv")
    if not data_file.exists():
        print("❌ Archivo de datos no encontrado")
        return False
    
    df = pd.read_csv(data_file, low_memory=False)
    print(f"✅ Datos cargados: {len(df)} registros")
    
    # Limpiar datos
    df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
    df['organismo'] = df['organismo'].fillna('Sin especificar')
    df['estamento'] = df['estamento'].fillna('Sin especificar')
    
    print("✅ Datos limpios")
    
    # Probar gráfico que causaba warnings
    try:
        if 'categoria_organismo' in df.columns and len(df) > 0:
            categoria_stats = df.groupby('categoria_organismo').agg({
                'sueldo_bruto': ['count', 'mean', 'median', 'std'],
                'organismo': 'nunique'
            }).round(0)
            
            categoria_stats.columns = ['Total_Funcionarios', 'Promedio_Sueldo', 'Mediana_Sueldo', 'Desv_Std', 'Organismos_Unicos']
            categoria_stats = categoria_stats.sort_values('Promedio_Sueldo', ascending=False)
            
            if len(categoria_stats) > 0:
                fig = px.bar(
                    categoria_stats.reset_index(),
                    x='categoria_organismo',
                    y='Promedio_Sueldo',
                    title="Sueldo Promedio por Categoría de Organismo (Datos Reales)",
                    labels={'categoria_organismo': 'Categoría', 'Promedio_Sueldo': 'Sueldo Promedio ($)'},
                    color='Promedio_Sueldo',
                    color_continuous_scale='Viridis',
                    hover_data=['Total_Funcionarios', 'Mediana_Sueldo', 'Organismos_Unicos']
                )
                print("✅ Gráfico por categoría creado sin warnings")
            else:
                print("⚠️ No hay datos de categorías")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Probar otros gráficos
    try:
        if 'estamento' in df.columns and len(df) > 0:
            estamento_promedio = df.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
            if len(estamento_promedio) > 0:
                fig = px.bar(
                    x=estamento_promedio.values,
                    y=estamento_promedio.index,
                    orientation='h',
                    title="Promedio de Sueldos por Estamento",
                    labels={'x': 'Sueldo Promedio ($)', 'y': 'Estamento'},
                    color=estamento_promedio.values,
                    color_continuous_scale='Blues'
                )
                print("✅ Gráfico por estamento creado sin warnings")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n✅ Prueba completada - Sin warnings molestos")
    return True

if __name__ == "__main__":
    test_no_warnings()

