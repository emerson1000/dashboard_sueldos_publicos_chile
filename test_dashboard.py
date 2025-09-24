#!/usr/bin/env python3
"""
Script de prueba para verificar que el dashboard funciona correctamente.
"""

import pandas as pd
import plotly.express as px
from pathlib import Path

def test_dashboard_components():
    """Prueba los componentes del dashboard."""
    print("🧪 PROBANDO COMPONENTES DEL DASHBOARD")
    print("=" * 50)
    
    # Cargar datos
    data_file = Path("data/processed/datos_reales_consolidados.csv")
    if not data_file.exists():
        print("❌ Archivo de datos no encontrado")
        return False
    
    df = pd.read_csv(data_file, low_memory=False)
    print(f"✅ Datos cargados: {len(df)} registros")
    
    # Limpiar datos como en el dashboard
    df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
    df['organismo'] = df['organismo'].fillna('Sin especificar')
    df['estamento'] = df['estamento'].fillna('Sin especificar')
    df['cargo'] = df['cargo'].fillna('Sin especificar')
    df['nombre'] = df['nombre'].fillna('Sin especificar')
    
    print(f"✅ Datos limpios: {len(df)} registros")
    
    # Probar gráfico por estamento
    try:
        if 'estamento' in df.columns and len(df) > 0:
            estamento_promedio = df.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
            if len(estamento_promedio) > 0:
                fig = px.bar(
                    x=estamento_promedio.values,
                    y=estamento_promedio.index,
                    orientation='h',
                    title="Promedio de Sueldos por Estamento (Datos Reales)",
                    labels={'x': 'Sueldo Promedio ($)', 'y': 'Estamento'},
                    color=estamento_promedio.values,
                    color_continuous_scale='Blues'
                )
                print("✅ Gráfico por estamento: OK")
            else:
                print("⚠️ No hay datos de estamentos")
    except Exception as e:
        print(f"❌ Error en gráfico por estamento: {e}")
    
    # Probar gráfico por organismo
    try:
        if 'organismo' in df.columns and len(df) > 0:
            organismo_promedio = df.groupby('organismo')['sueldo_bruto'].mean().sort_values(ascending=False).head(20)
            if len(organismo_promedio) > 0:
                fig = px.bar(
                    x=organismo_promedio.index,
                    y=organismo_promedio.values,
                    title="Top 20 Organismos por Sueldo Promedio (Datos Reales)",
                    labels={'x': 'Organismo', 'y': 'Sueldo Promedio ($)'},
                    color=organismo_promedio.values,
                    color_continuous_scale='Greens'
                )
                print("✅ Gráfico por organismo: OK")
            else:
                print("⚠️ No hay datos de organismos")
    except Exception as e:
        print(f"❌ Error en gráfico por organismo: {e}")
    
    # Probar gráfico por categoría
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
                print("✅ Gráfico por categoría: OK")
            else:
                print("⚠️ No hay datos de categorías")
    except Exception as e:
        print(f"❌ Error en gráfico por categoría: {e}")
    
    # Probar histograma
    try:
        if len(df) > 0 and 'sueldo_bruto' in df.columns:
            fig = px.histogram(
                df,
                x='sueldo_bruto',
                nbins=30,
                title="Distribución de Sueldos (Datos Reales)",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'},
                color_discrete_sequence=['#1f77b4']
            )
            print("✅ Histograma: OK")
        else:
            print("⚠️ No hay datos para histograma")
    except Exception as e:
        print(f"❌ Error en histograma: {e}")
    
    # Probar top sueldos
    try:
        if len(df) > 0 and 'sueldo_bruto' in df.columns:
            top_sueldos = df.nlargest(20, 'sueldo_bruto')[['organismo', 'nombre', 'cargo', 'estamento', 'sueldo_bruto']]
            if len(top_sueldos) > 0:
                fig = px.bar(
                    top_sueldos,
                    x='sueldo_bruto',
                    y='organismo',
                    orientation='h',
                    title="Top 20 Sueldos Más Altos (Datos Reales)",
                    labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'organismo': 'Organismo'},
                    color='sueldo_bruto',
                    color_continuous_scale='Reds',
                    hover_data=['nombre', 'cargo', 'estamento']
                )
                print("✅ Gráfico top sueldos: OK")
            else:
                print("⚠️ No hay datos para top sueldos")
        else:
            print("⚠️ No hay datos para top sueldos")
    except Exception as e:
        print(f"❌ Error en gráfico top sueldos: {e}")
    
    print("\n✅ Todas las pruebas completadas")
    return True

if __name__ == "__main__":
    test_dashboard_components()
