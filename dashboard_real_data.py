#!/usr/bin/env python3
"""
Dashboard Streamlit con datos reales consolidados.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
from datetime import datetime
import warnings

# Suprimir todos los warnings molestos
warnings.filterwarnings('ignore', category=UserWarning, module='plotly')
warnings.filterwarnings('ignore', message='.*keyword arguments have been deprecated.*')
warnings.filterwarnings('ignore', message='.*config instead to specify Plotly configuration options.*')
warnings.filterwarnings('ignore', message='.*deprecated.*')
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
warnings.filterwarnings('ignore', category=UserWarning, module='streamlit')
warnings.filterwarnings('ignore', message='.*use_container_width.*')
warnings.filterwarnings('ignore', message='.*deprecation.*')

# Configurar página
st.set_page_config(
    page_title="Transparencia Salarial Chile - Datos Reales",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suprimir warnings de Streamlit
st.set_option('deprecation.showPyplotGlobalUse', False)

# Configurar Plotly para evitar warnings
import plotly.io as pio
pio.templates.default = "plotly_white"

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
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_real_data():
    """Carga los datos reales consolidados."""
    try:
        data_file = Path("data/processed/datos_reales_consolidados.csv")
        if data_file.exists():
            df = pd.read_csv(data_file, low_memory=False)
            
            # Limpiar datos
            df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
            df['organismo'] = df['organismo'].fillna('Sin especificar')
            df['estamento'] = df['estamento'].fillna('Sin especificar')
            df['cargo'] = df['cargo'].fillna('Sin especificar')
            df['nombre'] = df['nombre'].fillna('Sin especificar')
            
            return df
        else:
            st.error("No se encontraron datos reales consolidados")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return pd.DataFrame()

@st.cache_data
def load_statistics():
    """Carga las estadísticas de los datos."""
    try:
        stats_file = Path("data/processed/estadisticas_datos_reales.json")
        if stats_file.exists():
            with open(stats_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except Exception as e:
        st.error(f"Error cargando estadísticas: {e}")
        return {}

def create_summary_metrics(df):
    """Crea métricas resumen."""
    if df.empty:
        return {}
    
    return {
        'total_registros': len(df),
        'promedio_sueldo': df['sueldo_bruto'].mean(),
        'mediana_sueldo': df['sueldo_bruto'].median(),
        'organismos_unicos': df['organismo'].nunique(),
        'estamentos_unicos': df['estamento'].nunique(),
        'categorias_unicas': df['categoria_organismo'].nunique() if 'categoria_organismo' in df.columns else 0
    }

def create_equity_metrics(df):
    """Crea métricas de equidad."""
    if df.empty:
        return {}
    
    # Ratio máximo/mínimo por estamento
    estamento_means = df.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
    if len(estamento_means) > 1:
        ratio_max_min = estamento_means.iloc[0] / estamento_means.iloc[-1]
        diferencia_max_min = estamento_means.iloc[0] - estamento_means.iloc[-1]
    else:
        ratio_max_min = 1.0
        diferencia_max_min = 0.0
    
    # Coeficiente de Gini simplificado
    sorted_salaries = df['sueldo_bruto'].sort_values().values
    n = len(sorted_salaries)
    if n > 1:
        cumsum = sorted_salaries.cumsum()
        gini = (n + 1 - 2 * cumsum.sum() / cumsum[-1]) / n
    else:
        gini = 0.0
    
    return {
        'ratio_max_min': ratio_max_min,
        'diferencia_max_min': diferencia_max_min,
        'gini_coefficient': gini
    }

def main():
    """Función principal del dashboard."""
    
    # Header principal
    st.markdown('<h1 class="main-header">🏛️ Transparencia Salarial Chile</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Datos reales consolidados del sistema de transparencia</p>', unsafe_allow_html=True)
    
    # Cargar datos
    df = load_real_data()
    stats = load_statistics()
    
    if df.empty:
        st.error("No se pudieron cargar los datos reales.")
        return
    
    # Mostrar información de datos reales
    st.markdown("""
    <div class="success-box">
        <h4>✅ Datos Reales Consolidados</h4>
        <p>Este dashboard utiliza <strong>datos reales</strong> extraídos del sistema de transparencia chileno, 
        no datos sintéticos. Los datos incluyen remuneraciones reales de funcionarios públicos de 
        múltiples organismos del Estado.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros")
    
    # Filtro por categoría de organismo (primero)
    if 'categoria_organismo' in df.columns:
        categorias = ['Todas'] + sorted(df['categoria_organismo'].unique().tolist())
        categoria_seleccionada = st.sidebar.selectbox("Categoría de Organismo", categorias)
        
        if categoria_seleccionada != 'Todas':
            df = df[df['categoria_organismo'] == categoria_seleccionada]
    
    # Filtro por organismo específico (después de categoría)
    organismos = ['Todos'] + sorted(df['organismo'].unique().tolist())
    organismo_seleccionado = st.sidebar.selectbox("Organismo Específico", organismos)
    
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
                value=f"{equity_metrics['ratio_max_min']:.1f}x",
                delta=None
            )
        
        with col2:
            st.metric(
                label="📈 Diferencia Max-Min",
                value=f"${equity_metrics['diferencia_max_min']:,.0f}",
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
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📈 Por Estamento", "🏛️ Por Organismo", "🏢 Por Categoría", "📊 Distribución", "🔍 Top Sueldos", "📋 Datos Raw"])
        
        with tab1:
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
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.warning("No hay datos de estamentos disponibles")
            else:
                st.warning("No hay datos de estamentos disponibles")
        
        with tab2:
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
                    fig.update_layout(xaxis_tickangle=45, height=400)
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.warning("No hay datos de organismos disponibles")
            else:
                st.warning("No hay datos de organismos disponibles")
        
        with tab3:
            if 'categoria_organismo' in df.columns and len(df) > 0:
                # Análisis por categoría
                categoria_stats = df.groupby('categoria_organismo').agg({
                    'sueldo_bruto': ['count', 'mean', 'median', 'std'],
                    'organismo': 'nunique'
                }).round(0)
                
                categoria_stats.columns = ['Total_Funcionarios', 'Promedio_Sueldo', 'Mediana_Sueldo', 'Desv_Std', 'Organismos_Unicos']
                categoria_stats = categoria_stats.sort_values('Promedio_Sueldo', ascending=False)
                
                if len(categoria_stats) > 0:
                    # Gráfico de barras por categoría
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
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, width='stretch')
                    
                    # Tabla de estadísticas por categoría
                    st.subheader("📊 Estadísticas por Categoría")
                    st.dataframe(categoria_stats, width='stretch')
                    
                    # Gráfico de dispersión: Organismos vs Sueldo por categoría
                    if 'organismo' in df.columns and len(df) > 0:
                        org_cat_stats = df.groupby(['categoria_organismo', 'organismo'])['sueldo_bruto'].mean().reset_index()
                        
                        if len(org_cat_stats) > 0:
                            fig = px.scatter(
                                org_cat_stats,
                                x='categoria_organismo',
                                y='sueldo_bruto',
                                color='categoria_organismo',
                                title="Distribución de Sueldos por Categoría y Organismo",
                                labels={'categoria_organismo': 'Categoría', 'sueldo_bruto': 'Sueldo Promedio ($)'},
                                hover_data=['organismo']
                            )
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, width='stretch')
                else:
                    st.warning("No hay datos de categorías disponibles")
            else:
                st.warning("No hay datos de categorías disponibles")
        
        with tab4:
            if len(df) > 0 and 'sueldo_bruto' in df.columns:
                fig = px.histogram(
                    df,
                    x='sueldo_bruto',
                    nbins=30,
                    title="Distribución de Sueldos (Datos Reales)",
                    labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'},
                    color_discrete_sequence=['#1f77b4']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
            else:
                st.warning("No hay datos de sueldos disponibles")
        
        with tab5:
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
                    fig.update_layout(height=600)
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.warning("No hay datos de sueldos disponibles")
            else:
                st.warning("No hay datos de sueldos disponibles")
        
        with tab6:
            st.subheader("Tabla de Datos Reales")
            
            # Mostrar columnas principales
            columns_to_show = ['organismo', 'nombre', 'cargo', 'estamento', 'sueldo_bruto', 'categoria_organismo']
            available_columns = [col for col in columns_to_show if col in df.columns]
            
            st.dataframe(df[available_columns], width='stretch')
        
        # Información del dataset
        st.subheader("ℹ️ Información del Dataset")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"""
            **Datos incluidos:**
            - {metrics['total_registros']:,} registros de funcionarios reales
            - {metrics['organismos_unicos']} organismos diferentes
            - {metrics['estamentos_unicos']} estamentos únicos
            - {metrics['categorias_unicas']} categorías de organismos
            """)
        
        with col2:
            # Mostrar distribución por categoría
            if 'categoria_organismo' in df.columns:
                categoria_dist = df['categoria_organismo'].value_counts()
                st.info(f"""
                **Distribución por categoría:**
                - Municipalidades: {categoria_dist.get('Municipalidad', 0):,} registros
                - Servicios: {categoria_dist.get('Servicio', 0):,} registros  
                - Ministerios: {categoria_dist.get('Ministerio', 0):,} registros
                - Otros: {categoria_dist.get('Otros', 0):,} registros
                """)
            else:
                st.info(f"""
                **Fuentes de datos:**
                - Datos extraídos del sistema de transparencia
                - Información oficial de organismos públicos
                - Datos procesados y validados
                - Consolidados de múltiples fuentes
                """)
    
    else:
        st.warning("No hay datos que coincidan con los filtros seleccionados.")

if __name__ == "__main__":
    main()
