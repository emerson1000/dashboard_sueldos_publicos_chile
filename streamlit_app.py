#!/usr/bin/env python3
"""
Dashboard de Transparencia Salarial - Streamlit Cloud Version
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Configurar página
st.set_page_config(
    page_title="Dashboard de Transparencia Salarial",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #666;
        margin-bottom: 3rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1f77b4;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

def load_data():
    """Carga los datos desde los archivos consolidados (Parquet preferido)."""
    try:
        # Intentar cargar datos finales en Parquet primero (más eficiente)
        data_file = Path("data/processed/sueldos_consolidado_final_small.parquet")
        if data_file.exists():
            df = pd.read_parquet(data_file)
            return df
        
        # Fallback a CSV
        data_file = Path("data/processed/sueldos_consolidado_final_small.csv")
        if data_file.exists():
            df = pd.read_csv(data_file)
            return df
        
        # Fallback a datos reales consolidados
        data_file = Path("data/processed/sueldos_reales_consolidado.csv")
        if data_file.exists():
            df = pd.read_csv(data_file)
            return df
        else:
            # Fallback a datos consolidados
            data_file = Path("data/processed/sueldos_consolidado.csv")
            if data_file.exists():
                df = pd.read_csv(data_file)
                return df
            else:
                st.error("No se encontraron datos. Por favor, ejecuta el ETL primero.")
                return pd.DataFrame()
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return pd.DataFrame()

def clean_data(df):
    """Limpia y prepara los datos para visualización."""
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
    
    # Crear categorías de sueldo
    df['categoria_sueldo'] = pd.cut(
        df['sueldo_bruto'], 
        bins=[0, 500000, 1000000, 2000000, 5000000, float('inf')],
        labels=['Muy Bajo', 'Bajo', 'Medio', 'Alto', 'Muy Alto']
    )
    
    return df

def create_summary_metrics(df):
    """Crea métricas resumen."""
    if df.empty:
        return {}
    
    return {
        'total_registros': len(df),
        'promedio_sueldo': df['sueldo_bruto'].mean(),
        'mediana_sueldo': df['sueldo_bruto'].median(),
        'organismos_unicos': df['organismo'].nunique(),
        'estamentos_unicos': df['estamento'].nunique()
    }

def create_equity_metrics(df):
    """Crea métricas de equidad."""
    if df.empty:
        return {}
    
    # Ratio máximo/mínimo por estamento
    estamento_stats = df.groupby('estamento')['sueldo_bruto'].agg(['min', 'max']).reset_index()
    estamento_stats['ratio'] = estamento_stats['max'] / estamento_stats['min']
    max_ratio = estamento_stats['ratio'].max()
    
    # Diferencia máxima
    max_diff = df['sueldo_bruto'].max() - df['sueldo_bruto'].min()
    
    # Coeficiente de Gini simplificado
    sorted_salaries = df['sueldo_bruto'].sort_values()
    n = len(sorted_salaries)
    gini = (2 * sum((i + 1) * salary for i, salary in enumerate(sorted_salaries))) / (n * sum(sorted_salaries)) - (n + 1) / n
    
    return {
        'max_min_ratio': max_ratio,
        'max_min_diff': max_diff,
        'gini_coefficient': gini
    }

def main():
    """Función principal del dashboard."""
    
    # Header principal
    st.markdown('<h1 class="main-header">🏛️ Dashboard de Transparencia Salarial</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Análisis de remuneraciones del sector público chileno</p>', unsafe_allow_html=True)
    
    # Cargar datos
    df = load_data()
    
    if df.empty:
        st.error("No se pudieron cargar los datos. Verifica que el ETL se haya ejecutado correctamente.")
        return
    
    # Limpiar datos
    df = clean_data(df)
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros")
    
    # Filtro por organismo
    organismos = ['Todos'] + sorted(df['organismo'].unique().tolist())
    organismo_seleccionado = st.sidebar.selectbox("Organismo", organismos)
    
    if organismo_seleccionado != 'Todos':
        df = df[df['organismo'] == organismo_seleccionado]
    
    # Filtro por estamento
    estamentos = ['Todos'] + sorted(df['estamento'].unique().tolist())
    estamento_seleccionado = st.sidebar.selectbox("Estamento", estamentos)
    
    if estamento_seleccionado != 'Todos':
        df = df[df['estamento'] == estamento_seleccionado]
    
    # Filtro por rango de sueldo
    if not df.empty:
        min_sueldo = df['sueldo_bruto'].min()
        max_sueldo = df['sueldo_bruto'].max()
        
        if min_sueldo != max_sueldo:
            rango_sueldo = st.sidebar.slider(
                "Rango de Sueldo",
                min_value=int(min_sueldo),
                max_value=int(max_sueldo),
                value=(int(min_sueldo), int(max_sueldo)),
                format="$%d"
            )
            df = df[(df['sueldo_bruto'] >= rango_sueldo[0]) & (df['sueldo_bruto'] <= rango_sueldo[1])]
    
    # Métricas principales
    if not df.empty:
        metrics = create_summary_metrics(df)
        equity_metrics = create_equity_metrics(df)
        
        # Mostrar métricas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📊 Total Registros",
                value=f"{metrics['total_registros']:,}",
                delta=None
            )
        
        with col2:
            st.metric(
                label="💰 Promedio Sueldo",
                value=f"${metrics['promedio_sueldo']:,.0f}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="📈 Mediana Sueldo",
                value=f"${metrics['mediana_sueldo']:,.0f}",
                delta=None
            )
        
        with col4:
            st.metric(
                label="🏛️ Organismos",
                value=f"{metrics['organismos_unicos']}",
                delta=None
            )
        
        # Métricas de equidad
        st.subheader("⚖️ Métricas de Equidad")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="📊 Ratio Max/Min",
                value=f"{equity_metrics['max_min_ratio']:.1f}x",
                delta=None
            )
        
        with col2:
            st.metric(
                label="📈 Diferencia Max-Min",
                value=f"${equity_metrics['max_min_diff']:,.0f}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="📉 Coeficiente Gini",
                value=f"{equity_metrics['gini_coefficient']:.3f}",
                delta=None
            )
        
        # Visualizaciones
        st.subheader("📊 Visualizaciones")
        
        # Tabs para diferentes análisis
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Por Estamento", "🏛️ Por Organismo", "📊 Distribución", "🔍 Top Sueldos"])
        
        with tab1:
            if 'estamento' in df.columns:
                estamento_promedio = df.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
                fig = px.bar(
                    x=estamento_promedio.values,
                    y=estamento_promedio.index,
                    orientation='h',
                    title="Promedio de Sueldos por Estamento",
                    labels={'x': 'Sueldo Promedio ($)', 'y': 'Estamento'},
                    color=estamento_promedio.values,
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            if 'organismo' in df.columns:
                organismo_promedio = df.groupby('organismo')['sueldo_bruto'].mean().sort_values(ascending=False).head(10)
                fig = px.bar(
                    x=organismo_promedio.index,
                    y=organismo_promedio.values,
                    title="Top 10 Organismos por Sueldo Promedio",
                    labels={'x': 'Organismo', 'y': 'Sueldo Promedio ($)'},
                    color=organismo_promedio.values,
                    color_continuous_scale='Greens'
                )
                fig.update_layout(xaxis_tickangle=45, height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            fig = px.histogram(
                df,
                x='sueldo_bruto',
                nbins=30,
                title="Distribución de Sueldos",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'},
                color_discrete_sequence=['#1f77b4']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            top_sueldos = df.nlargest(10, 'sueldo_bruto')[['organismo', 'estamento', 'sueldo_bruto']]
            fig = px.bar(
                top_sueldos,
                x='sueldo_bruto',
                y='organismo',
                orientation='h',
                title="Top 10 Sueldos Más Altos",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'organismo': 'Organismo'},
                color='sueldo_bruto',
                color_continuous_scale='Reds'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Información del dataset
        st.subheader("ℹ️ Información del Dataset")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"""
            **Datos incluidos:**
            - {metrics['total_registros']:,} registros de funcionarios
            - {metrics['organismos_unicos']} organismos diferentes
            - {metrics['estamentos_unicos']} estamentos únicos
            - Fuentes: SII, Ministerios, Transparencia Activa
            """)
        
        with col2:
            st.info(f"""
            **Última actualización:**
            - Datos extraídos automáticamente
            - Procesados con ETL robusto
            - Validados y limpiados
            - Base de datos SQLite
            """)
    
    else:
        st.warning("No hay datos que coincidan con los filtros seleccionados.")

if __name__ == "__main__":
    main()
