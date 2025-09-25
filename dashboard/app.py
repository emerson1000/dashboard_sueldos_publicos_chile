#!/usr/bin/env python3
"""
Dashboard avanzado de Transparencia Salarial para funcionarios públicos chilenos.

Proporciona análisis comprehensivos de remuneraciones con visualizaciones interactivas,
métricas de equidad, comparaciones temporales y herramientas de exploración de datos.
"""

import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Configuración de la página
st.set_page_config(
    page_title="Transparencia Salarial Chile",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'data' / 'sueldos.db'
CSV_PATH = BASE_DIR / 'data' / 'processed' / 'sueldos_consolidado.csv'

# Estilos CSS personalizados
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    .stSelectbox > div > div {
        background-color: white;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Carga los datos desde la base SQLite o el CSV consolidado."""
    try:
        if DB_PATH.exists():
            conn = sqlite3.connect(DB_PATH)
            df = pd.read_sql_query('SELECT * FROM sueldos', conn)
            conn.close()
        elif CSV_PATH.exists():
            df = pd.read_csv(CSV_PATH)
        else:
            return pd.DataFrame()
        
        # Limpiar y procesar datos
        df = clean_data(df)
        return df
    except Exception as e:
        st.error(f"Error al cargar datos: {e}")
        return pd.DataFrame()

def clean_data(df):
    """Limpia y procesa los datos para análisis."""
    if df.empty:
        return df
    
    # Convertir sueldo_bruto a numérico
    df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
    
    # Limpiar organismos
    df['organismo'] = df['organismo'].fillna('Sin especificar')
    df['organismo'] = df['organismo'].str.strip()
    
    # Limpiar estamentos
    df['estamento'] = df['estamento'].fillna('Sin especificar')
    df['estamento'] = df['estamento'].str.strip()
    
    # Limpiar grados
    df['grado'] = df['grado'].fillna('Sin especificar')
    df['grado'] = df['grado'].astype(str).str.strip()
    
    # Agregar categorías de sueldo
    df['categoria_sueldo'] = pd.cut(
        df['sueldo_bruto'], 
        bins=[0, 500000, 1000000, 1500000, 2000000, float('inf')],
        labels=['< $500K', '$500K-$1M', '$1M-$1.5M', '$1.5M-$2M', '> $2M'],
        include_lowest=True
    )
    
    return df

def create_summary_metrics(df):
    """Crea métricas resumen del dataset."""
    if df.empty:
        return {}
    
    metrics = {
        'total_registros': len(df),
        'organismos_unicos': df['organismo'].nunique(),
        'estamentos_unicos': df['estamento'].nunique(),
        'promedio_sueldo': df['sueldo_bruto'].mean(),
        'mediana_sueldo': df['sueldo_bruto'].median(),
        'min_sueldo': df['sueldo_bruto'].min(),
        'max_sueldo': df['sueldo_bruto'].max(),
        'desv_std': df['sueldo_bruto'].std(),
        'coef_variacion': df['sueldo_bruto'].std() / df['sueldo_bruto'].mean() if df['sueldo_bruto'].mean() > 0 else 0
    }
    
    return metrics

def create_equity_metrics(df):
    """Calcula métricas de equidad salarial."""
    if df.empty or 'estamento' not in df.columns:
        return {}
    
    equity_metrics = {}
    
    # Ratio entre estamentos
    estamento_means = df.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
    if len(estamento_means) > 1:
        equity_metrics['ratio_max_min'] = estamento_means.iloc[0] / estamento_means.iloc[-1]
        equity_metrics['diferencia_max_min'] = estamento_means.iloc[0] - estamento_means.iloc[-1]
    
    # Gini coefficient (simplificado)
    sorted_salaries = np.sort(df['sueldo_bruto'].dropna())
    n = len(sorted_salaries)
    if n > 1:
        cumsum = np.cumsum(sorted_salaries)
        equity_metrics['gini_coefficient'] = (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n
    
    return equity_metrics

def main():
    # Header principal
    st.markdown('<h1 class="main-header">🏛️ Transparencia Salarial Chile</h1>', unsafe_allow_html=True)
    st.markdown("### Análisis de Remuneraciones del Sector Público")
    
    # Cargar datos
    df = load_data()
    
    if df.empty:
        st.error("🚨 No hay datos disponibles. Por favor ejecuta el pipeline ETL para cargar información.")
        st.info("💡 Puedes ejecutar: `python etl/extract_dipres.py && python etl/extract_sii.py && python etl/extract_contraloria.py && python etl/transform.py && python etl/load.py`")
        return
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros de Análisis")
    
    # Filtro por organismo
    organismos = sorted(df['organismo'].dropna().unique())
    organismos_seleccionados = st.sidebar.multiselect(
        "Seleccionar organismos",
        organismos,
        default=organismos[:min(10, len(organismos))] if len(organismos) > 0 else []
    )
    
    # Filtro por estamento
    estamentos = sorted(df['estamento'].dropna().unique())
    estamentos_seleccionados = st.sidebar.multiselect(
        "Seleccionar estamentos",
        estamentos,
        default=estamentos
    )
    
    # Filtro por rango de sueldo
    st.sidebar.subheader("💰 Rango de Sueldo")
    min_sueldo, max_sueldo = st.sidebar.slider(
        "Rango de sueldo bruto",
        min_value=int(df['sueldo_bruto'].min()),
        max_value=int(df['sueldo_bruto'].max()),
        value=(int(df['sueldo_bruto'].min()), int(df['sueldo_bruto'].max())),
        format="$%d"
    )
    
    # Aplicar filtros
    df_filtered = df.copy()
    
    if organismos_seleccionados:
        df_filtered = df_filtered[df_filtered['organismo'].isin(organismos_seleccionados)]
    
    if estamentos_seleccionados:
        df_filtered = df_filtered[df_filtered['estamento'].isin(estamentos_seleccionados)]
    
    df_filtered = df_filtered[
        (df_filtered['sueldo_bruto'] >= min_sueldo) & 
        (df_filtered['sueldo_bruto'] <= max_sueldo)
    ]
    
    # Métricas principales
    st.header("📊 Métricas Principales")
    
    metrics = create_summary_metrics(df_filtered)
    equity_metrics = create_equity_metrics(df_filtered)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Registros",
            f"{metrics['total_registros']:,}",
            delta=f"{len(df_filtered) - len(df):+,}" if len(df_filtered) != len(df) else None
        )
    
    with col2:
        st.metric(
            "Promedio Sueldo",
            f"${metrics['promedio_sueldo']:,.0f}",
            delta=f"${metrics['promedio_sueldo'] - df['sueldo_bruto'].mean():+,.0f}" if len(df_filtered) != len(df) else None
        )
    
    with col3:
        st.metric(
            "Mediana Sueldo",
            f"${metrics['mediana_sueldo']:,.0f}",
            delta=f"${metrics['mediana_sueldo'] - df['sueldo_bruto'].median():+,.0f}" if len(df_filtered) != len(df) else None
        )
    
    with col4:
        st.metric(
            "Organismos",
            f"{metrics['organismos_unicos']:,}",
            delta=f"{metrics['organismos_unicos'] - df['organismo'].nunique():+,}" if len(df_filtered) != len(df) else None
        )
    
    # Métricas de equidad
    if equity_metrics:
        st.subheader("⚖️ Métricas de Equidad")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'ratio_max_min' in equity_metrics:
                st.metric("Ratio Max/Min Estamentos", f"{equity_metrics['ratio_max_min']:.2f}x")
        
        with col2:
            if 'diferencia_max_min' in equity_metrics:
                st.metric("Diferencia Max-Min", f"${equity_metrics['diferencia_max_min']:,.0f}")
        
        with col3:
            if 'gini_coefficient' in equity_metrics:
                st.metric("Coeficiente Gini", f"{equity_metrics['gini_coefficient']:.3f}")
    
    # Visualizaciones principales
    st.header("📈 Análisis Visual")
    
    # Tabs para diferentes análisis
    tab1, tab2, tab3, tab4 = st.tabs(["🏛️ Por Estamento", "🏢 Por Organismo", "📊 Distribución", "🔍 Top Sueldos"])
    
    with tab1:
        if 'estamento' in df_filtered.columns and not df_filtered.empty:
            # Gráfico de barras por estamento
            estamento_stats = df_filtered.groupby('estamento').agg({
                'sueldo_bruto': ['mean', 'median', 'count']
            }).round(0)
            estamento_stats.columns = ['Promedio', 'Mediana', 'Cantidad']
            estamento_stats = estamento_stats.sort_values('Promedio', ascending=True)
            
            fig = px.bar(
                estamento_stats.reset_index(),
                x='Promedio',
                y='estamento',
                orientation='h',
                title="Promedio de Sueldo Bruto por Estamento",
                labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'estamento': 'Estamento'},
                color='Promedio',
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            
            # Box plot por estamento
            fig_box = px.box(
                df_filtered,
                x='estamento',
                y='sueldo_bruto',
                title="Distribución de Sueldos por Estamento",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'estamento': 'Estamento'}
            )
            fig_box.update_layout(height=400)
            st.plotly_chart(fig_box, use_container_width=True)
    
    with tab2:
        if 'organismo' in df_filtered.columns and not df_filtered.empty:
            # Top organismos por promedio
            org_stats = df_filtered.groupby('organismo').agg({
                'sueldo_bruto': ['mean', 'count']
            }).round(0)
            org_stats.columns = ['Promedio', 'Cantidad']
            org_stats = org_stats[org_stats['Cantidad'] >= 5]  # Solo organismos con al menos 5 registros
            org_stats = org_stats.sort_values('Promedio', ascending=True).tail(20)
            
            fig = px.bar(
                org_stats.reset_index(),
                x='Promedio',
                y='organismo',
                orientation='h',
                title="Top 20 Organismos por Sueldo Promedio",
                labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'organismo': 'Organismo'},
                color='Promedio',
                color_continuous_scale='Greens'
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    
    with tab3:
        if not df_filtered.empty:
            # Histograma de distribución
            fig_hist = px.histogram(
                df_filtered,
                x='sueldo_bruto',
                nbins=50,
                title="Distribución de Sueldos Brutos",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'}
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Distribución por categorías
            categoria_counts = df_filtered['categoria_sueldo'].value_counts()
            fig_pie = px.pie(
                values=categoria_counts.values,
                names=categoria_counts.index,
                title="Distribución por Categorías de Sueldo"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
    
    with tab4:
        if not df_filtered.empty:
            # Top sueldos
            top_sueldos = df_filtered.nlargest(20, 'sueldo_bruto')
            
            fig = px.bar(
                top_sueldos,
                x='sueldo_bruto',
                y='organismo',
                orientation='h',
                title="Top 20 Sueldos Más Altos",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'organismo': 'Organismo'},
                color='sueldo_bruto',
                color_continuous_scale='Reds',
                hover_data=['cargo', 'estamento', 'grado']
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            
            # Tabla detallada
            st.subheader("📋 Detalle de Top Sueldos")
            display_cols = ['organismo', 'nombre', 'cargo', 'grado', 'estamento', 'sueldo_bruto']
            available_cols = [col for col in display_cols if col in top_sueldos.columns]
            st.dataframe(
                top_sueldos[available_cols].reset_index(drop=True),
                use_container_width=True
            )
    
    # Información adicional
    st.header("ℹ️ Información del Dataset")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Última Actualización")
        if CSV_PATH.exists():
            mod_time = datetime.fromtimestamp(CSV_PATH.stat().st_mtime)
            st.info(f"Datos actualizados: {mod_time.strftime('%d/%m/%Y %H:%M')}")
        else:
            st.warning("No se pudo determinar la fecha de actualización")
    
    with col2:
        st.subheader("📊 Estadísticas del Dataset")
        st.info(f"""
        - **Total de registros**: {len(df):,}
        - **Registros filtrados**: {len(df_filtered):,}
        - **Organismos únicos**: {df['organismo'].nunique():,}
        - **Estamentos únicos**: {df['estamento'].nunique():,}
        """)

if __name__ == '__main__':
    main()