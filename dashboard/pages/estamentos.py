#!/usr/bin/env python3
"""
Análisis detallado de remuneraciones por estamento del sector público chileno.

Proporciona análisis comparativos, estadísticas descriptivas y visualizaciones
específicas para cada estamento del sector público.
"""

import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = BASE_DIR / 'data' / 'sueldos.db'
CSV_PATH = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado.csv'

@st.cache_data
def load_data():
    """Carga y limpia los datos."""
    try:
        if DB_PATH.exists():
            conn = sqlite3.connect(DB_PATH)
            df = pd.read_sql_query('SELECT * FROM sueldos', conn)
            conn.close()
        elif CSV_PATH.exists():
            df = pd.read_csv(CSV_PATH)
        else:
            return pd.DataFrame()
        
        # Limpiar datos
        df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
        df['organismo'] = df['organismo'].fillna('Sin especificar')
        df['estamento'] = df['estamento'].fillna('Sin especificar')
        df['grado'] = df['grado'].fillna('Sin especificar')
        
        return df
    except Exception as e:
        st.error(f"Error al cargar datos: {e}")
        return pd.DataFrame()

def calculate_estamento_stats(df, estamento):
    """Calcula estadísticas para un estamento específico."""
    estamento_data = df[df['estamento'] == estamento]
    
    if estamento_data.empty:
        return {}
    
    stats = {
        'total_registros': len(estamento_data),
        'organismos_unicos': estamento_data['organismo'].nunique(),
        'promedio_sueldo': estamento_data['sueldo_bruto'].mean(),
        'mediana_sueldo': estamento_data['sueldo_bruto'].median(),
        'min_sueldo': estamento_data['sueldo_bruto'].min(),
        'max_sueldo': estamento_data['sueldo_bruto'].max(),
        'desv_std': estamento_data['sueldo_bruto'].std(),
        'percentil_25': estamento_data['sueldo_bruto'].quantile(0.25),
        'percentil_75': estamento_data['sueldo_bruto'].quantile(0.75),
        'iqr': estamento_data['sueldo_bruto'].quantile(0.75) - estamento_data['sueldo_bruto'].quantile(0.25)
    }
    
    return stats

def main():
    st.set_page_config(page_title="Análisis por Estamento", layout="wide")
    
    st.title("🏛️ Análisis por Estamento")
    st.markdown("### Comparación detallada de remuneraciones por estamento del sector público")
    
    df = load_data()
    
    if df.empty:
        st.error("🚨 No hay datos disponibles.")
        return
    
    if 'estamento' not in df.columns:
        st.error("❌ El conjunto de datos no tiene columna 'estamento'.")
        return
    
    estamentos = sorted(df['estamento'].dropna().unique())
    if not estamentos:
        st.error("❌ No se encontraron estamentos en los datos.")
        return
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros")
    
    # Selección de estamento
    selected_estamento = st.sidebar.selectbox(
        "Seleccionar estamento para análisis detallado",
        estamentos,
        index=0
    )
    
    # Filtro por organismo (opcional)
    organismos_estamento = sorted(df[df['estamento'] == selected_estamento]['organismo'].unique())
    organismos_seleccionados = st.sidebar.multiselect(
        "Filtrar por organismos (opcional)",
        organismos_estamento,
        default=organismos_estamento
    )
    
    # Filtro por rango de sueldo
    estamento_data = df[df['estamento'] == selected_estamento]
    if not estamento_data.empty:
        min_sueldo, max_sueldo = st.sidebar.slider(
            "Rango de sueldo bruto",
            min_value=int(estamento_data['sueldo_bruto'].min()),
            max_value=int(estamento_data['sueldo_bruto'].max()),
            value=(int(estamento_data['sueldo_bruto'].min()), int(estamento_data['sueldo_bruto'].max())),
            format="$%d"
        )
    
    # Aplicar filtros
    df_filtered = df[df['estamento'] == selected_estamento].copy()
    
    if organismos_seleccionados:
        df_filtered = df_filtered[df_filtered['organismo'].isin(organismos_seleccionados)]
    
    if not estamento_data.empty:
        df_filtered = df_filtered[
            (df_filtered['sueldo_bruto'] >= min_sueldo) & 
            (df_filtered['sueldo_bruto'] <= max_sueldo)
        ]
    
    # Estadísticas del estamento seleccionado
    stats = calculate_estamento_stats(df_filtered, selected_estamento)
    
    if stats:
        st.header(f"📊 Estadísticas del Estamento: {selected_estamento}")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", f"{stats['total_registros']:,}")
        
        with col2:
            st.metric("Promedio Sueldo", f"${stats['promedio_sueldo']:,.0f}")
        
        with col3:
            st.metric("Mediana Sueldo", f"${stats['mediana_sueldo']:,.0f}")
        
        with col4:
            st.metric("Organismos", f"{stats['organismos_unicos']:,}")
        
        # Estadísticas adicionales
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Sueldo Mínimo", f"${stats['min_sueldo']:,.0f}")
        
        with col2:
            st.metric("Sueldo Máximo", f"${stats['max_sueldo']:,.0f}")
        
        with col3:
            st.metric("Desv. Estándar", f"${stats['desv_std']:,.0f}")
    
    # Comparación entre estamentos
    st.header("📈 Comparación entre Estamentos")
    
    # Gráfico de barras comparativo
    estamento_comparison = df.groupby('estamento')['sueldo_bruto'].agg(['mean', 'median', 'count']).round(0)
    estamento_comparison.columns = ['Promedio', 'Mediana', 'Cantidad']
    estamento_comparison = estamento_comparison.sort_values('Promedio', ascending=True)
    
    fig = px.bar(
        estamento_comparison.reset_index(),
        x='Promedio',
        y='estamento',
        orientation='h',
        title="Comparación de Sueldos Promedio por Estamento",
        labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'estamento': 'Estamento'},
        color='Promedio',
        color_continuous_scale='Blues',
        hover_data=['Mediana', 'Cantidad']
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    
    # Box plot comparativo
    fig_box = px.box(
        df,
        x='estamento',
        y='sueldo_bruto',
        title="Distribución de Sueldos por Estamento",
        labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'estamento': 'Estamento'}
    )
    fig_box.update_layout(height=400)
    st.plotly_chart(fig_box, use_container_width=True)
    
    # Análisis detallado del estamento seleccionado
    if not df_filtered.empty:
        st.header(f"🔍 Análisis Detallado: {selected_estamento}")
        
        # Tabs para diferentes análisis
        tab1, tab2, tab3 = st.tabs(["🏢 Por Organismo", "📊 Distribución", "🔝 Top Sueldos"])
        
        with tab1:
            # Promedio por organismo
            org_stats = df_filtered.groupby('organismo').agg({
                'sueldo_bruto': ['mean', 'median', 'count']
            }).round(0)
            org_stats.columns = ['Promedio', 'Mediana', 'Cantidad']
            org_stats = org_stats[org_stats['Cantidad'] >= 3]  # Solo organismos con al menos 3 registros
            org_stats = org_stats.sort_values('Promedio', ascending=True)
            
            if not org_stats.empty:
                fig = px.bar(
                    org_stats.reset_index(),
                    x='Promedio',
                    y='organismo',
                    orientation='h',
                    title=f"Promedio de Sueldos por Organismo - {selected_estamento}",
                    labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'organismo': 'Organismo'},
                    color='Promedio',
                    color_continuous_scale='Greens',
                    hover_data=['Mediana', 'Cantidad']
                )
                fig.update_layout(height=max(400, len(org_stats) * 20))
                st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            else:
                st.info("No hay suficientes datos para mostrar el análisis por organismo.")
        
        with tab2:
            # Histograma de distribución
            fig_hist = px.histogram(
                df_filtered,
                x='sueldo_bruto',
                nbins=30,
                title=f"Distribución de Sueldos - {selected_estamento}",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'}
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True, config={"responsive": True})
            
            # Estadísticas descriptivas
            st.subheader("📋 Estadísticas Descriptivas")
            desc_stats = df_filtered['sueldo_bruto'].describe()
            st.dataframe(desc_stats.to_frame().round(0), use_container_width=True)
        
        with tab3:
            # Top sueldos del estamento
            top_sueldos = df_filtered.nlargest(20, 'sueldo_bruto')
            
            fig = px.bar(
                top_sueldos,
                x='sueldo_bruto',
                y='organismo',
                orientation='h',
                title=f"Top 20 Sueldos Más Altos - {selected_estamento}",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'organismo': 'Organismo'},
                color='sueldo_bruto',
                color_continuous_scale='Reds',
                hover_data=['cargo', 'grado']
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            
            # Tabla detallada
            st.subheader("📋 Detalle de Top Sueldos")
            display_cols = ['organismo', 'nombre', 'cargo', 'grado', 'sueldo_bruto']
            available_cols = [col for col in display_cols if col in top_sueldos.columns]
            st.dataframe(
                top_sueldos[available_cols].reset_index(drop=True),
                use_container_width=True
            )
    
    # Resumen ejecutivo
    st.header("📝 Resumen Ejecutivo")
    
    if stats:
        st.info(f"""
        **Estamento {selected_estamento}:**
        - Representa {stats['total_registros']:,} funcionarios ({stats['total_registros']/len(df)*100:.1f}% del total)
        - Sueldo promedio: ${stats['promedio_sueldo']:,.0f}
        - Sueldo mediano: ${stats['mediana_sueldo']:,.0f}
        - Rango salarial: ${stats['min_sueldo']:,.0f} - ${stats['max_sueldo']:,.0f}
        - Dispersión salarial: ${stats['desv_std']:,.0f} (coeficiente de variación: {stats['desv_std']/stats['promedio_sueldo']:.2f})
        """)

if __name__ == '__main__':
    main()