#!/usr/bin/env python3
"""
Streamlit app principal para Streamlit Cloud.
Dashboard de transparencia salarial con datos reales.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import warnings
from pathlib import Path
import sqlite3

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Sueldos Públicos Chile",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suprimir warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Configurar Plotly para evitar warnings
import plotly.io as pio
pio.templates.default = "plotly_white"

def load_data():
    """Cargar datos desde archivos CSV"""
    try:
        # Intentar cargar desde CSV consolidado
        csv_file = Path("data/consolidated/sueldos_consolidados.csv")
        if csv_file.exists():
            df = pd.read_csv(csv_file)
            return df
        
        # Si no existe, crear datos de ejemplo
        st.warning("No se encontraron datos consolidados. Mostrando datos de ejemplo.")
        return create_sample_data()
        
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return create_sample_data()

def create_sample_data():
    """Crear datos de ejemplo para demostración"""
    data = {
        'organismo': ['Municipalidad Santiago', 'SII', 'Ministerio Hacienda', 'DIPRES', 'Municipalidad Providencia'] * 20,
        'categoria_organismo': ['Municipalidad', 'Servicio', 'Ministerio', 'Servicio', 'Municipalidad'] * 20,
        'estamento': ['Directivo', 'Profesional', 'Técnico', 'Administrativo', 'Auxiliar'] * 20,
        'sueldo_bruto': [8000000, 6500000, 7200000, 5800000, 4500000] * 20,
        'nombre': ['Juan Pérez', 'María González', 'Carlos Silva', 'Ana Martínez', 'Luis Rodríguez'] * 20,
        'cargo': ['Director', 'Analista', 'Jefe', 'Asistente', 'Secretario'] * 20
    }
    return pd.DataFrame(data)

def clean_data(df):
    """Limpiar y procesar datos"""
    if df.empty:
        return df
    
    # Convertir sueldo_bruto a numérico
    if 'sueldo_bruto' in df.columns:
        df['sueldo_bruto'] = pd.to_numeric(df['sueldo_bruto'], errors='coerce')
        df = df.dropna(subset=['sueldo_bruto'])
    
    # Limpiar categorías
    if 'categoria_organismo' in df.columns:
        df['categoria_organismo'] = df['categoria_organismo'].fillna('Otros')
    
    return df

def main():
    """Función principal del dashboard"""
    
    # CSS personalizado
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
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
    </style>
    """, unsafe_allow_html=True)
    
    # Título principal
    st.markdown('<h1 class="main-header">🏛️ Dashboard Sueldos Públicos Chile</h1>', unsafe_allow_html=True)
    st.markdown("**Análisis de remuneraciones del sector público chileno con datos reales**")
    
    # Cargar datos
    with st.spinner("Cargando datos..."):
        df = load_data()
        df = clean_data(df)
    
    if df.empty:
        st.error("No se pudieron cargar los datos.")
        return
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros")
    
    # Filtro por categoría de organismo
    if 'categoria_organismo' in df.columns:
        categorias = ['Todas'] + sorted(df['categoria_organismo'].unique().tolist())
        categoria_seleccionada = st.sidebar.selectbox("Categoría de Organismo", categorias)
        
        if categoria_seleccionada != 'Todas':
            df = df[df['categoria_organismo'] == categoria_seleccionada]
    
    # Filtro por organismo específico
    if 'organismo' in df.columns:
        organismos = ['Todos'] + sorted(df['organismo'].unique().tolist())
        organismo_seleccionado = st.sidebar.selectbox("Organismo Específico", organismos)
        
        if organismo_seleccionado != 'Todos':
            df = df[df['organismo'] == organismo_seleccionado]
    
    # Filtro por estamento
    if 'estamento' in df.columns:
        estamentos = ['Todos'] + sorted(df['estamento'].unique().tolist())
        estamento_seleccionado = st.sidebar.selectbox("Estamento", estamentos)
        
        if estamento_seleccionado != 'Todos':
            df = df[df['estamento'] == estamento_seleccionado]
    
    # Filtro por rango de sueldo
    if 'sueldo_bruto' in df.columns and len(df) > 0:
        min_sueldo = int(df['sueldo_bruto'].min())
        max_sueldo = int(df['sueldo_bruto'].max())
        
        if min_sueldo != max_sueldo:
            rango_sueldo = st.sidebar.slider(
                "Rango de Sueldo",
                min_value=min_sueldo,
                max_value=max_sueldo,
                value=(min_sueldo, max_sueldo),
                format="$%d"
            )
            df = df[(df['sueldo_bruto'] >= rango_sueldo[0]) & (df['sueldo_bruto'] <= rango_sueldo[1])]
    
    # Métricas principales
    if len(df) > 0:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Registros", f"{len(df):,}")
        
        with col2:
            promedio = df['sueldo_bruto'].mean()
            st.metric("💰 Promedio Sueldo", f"${promedio:,.0f}")
        
        with col3:
            mediana = df['sueldo_bruto'].median()
            st.metric("📈 Mediana Sueldo", f"${mediana:,.0f}")
        
        with col4:
            organismos_unicos = df['organismo'].nunique() if 'organismo' in df.columns else 0
            st.metric("🏛️ Organismos", f"{organismos_unicos}")
        
        # Tabs para diferentes análisis
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Por Estamento", "🏛️ Por Organismo", "🏢 Por Categoría", "📋 Datos Raw"])
        
        with tab1:
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
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.warning("No hay datos de estamentos disponibles")
            else:
                st.warning("No hay datos de estamentos disponibles")
        
        with tab2:
            if 'organismo' in df.columns and len(df) > 0:
                organismo_promedio = df.groupby('organismo')['sueldo_bruto'].mean().sort_values(ascending=False).head(10)
                if len(organismo_promedio) > 0:
                    fig = px.bar(
                        x=organismo_promedio.index,
                        y=organismo_promedio.values,
                        title="Top 10 Organismos por Sueldo Promedio",
                        labels={'x': 'Organismo', 'y': 'Sueldo Promedio ($)'},
                        color=organismo_promedio.values,
                        color_continuous_scale='Greens'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.warning("No hay datos de organismos disponibles")
            else:
                st.warning("No hay datos de organismos disponibles")
        
        with tab3:
            if 'categoria_organismo' in df.columns and len(df) > 0:
                categoria_stats = df.groupby('categoria_organismo').agg({
                    'sueldo_bruto': ['count', 'mean', 'median'],
                    'organismo': 'nunique'
                }).round(0)
                
                categoria_stats.columns = ['Total_Funcionarios', 'Promedio_Sueldo', 'Mediana_Sueldo', 'Organismos_Unicos']
                categoria_stats = categoria_stats.sort_values('Promedio_Sueldo', ascending=False)
                
                if len(categoria_stats) > 0:
                    fig = px.bar(
                        categoria_stats.reset_index(),
                        x='categoria_organismo',
                        y='Promedio_Sueldo',
                        title="Sueldo Promedio por Categoría de Organismo",
                        labels={'categoria_organismo': 'Categoría', 'Promedio_Sueldo': 'Sueldo Promedio ($)'},
                        color='Promedio_Sueldo',
                        color_continuous_scale='Viridis',
                        hover_data=['Total_Funcionarios', 'Mediana_Sueldo', 'Organismos_Unicos']
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, width='stretch')
                    
                    # Tabla de estadísticas
                    st.subheader("Estadísticas por Categoría")
                    st.dataframe(categoria_stats, width='stretch')
                else:
                    st.warning("No hay datos de categorías disponibles")
            else:
                st.warning("No hay datos de categorías disponibles")
        
        with tab4:
            st.subheader("Datos Raw")
            st.dataframe(df, width='stretch')
            
            # Información del dataset
            st.subheader("Información del Dataset")
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Total de registros:** {len(df):,}")
                st.write(f"**Columnas:** {len(df.columns)}")
                st.write(f"**Memoria utilizada:** {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            with col2:
                if 'categoria_organismo' in df.columns:
                    st.write("**Distribución por categoría:**")
                    categoria_dist = df['categoria_organismo'].value_counts()
                    for cat, count in categoria_dist.items():
                        st.write(f"- {cat}: {count:,} ({count/len(df)*100:.1f}%)")
    
    else:
        st.warning("No hay datos que coincidan con los filtros seleccionados")

if __name__ == "__main__":
    main()