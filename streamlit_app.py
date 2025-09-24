#!/usr/bin/env python3
"""
Streamlit app principal para Streamlit Cloud.
Dashboard de transparencia salarial con datos reales.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import warnings
from pathlib import Path

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Sueldos Públicos Chile",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suprimir warnings
warnings.filterwarnings("ignore")

# Configurar Plotly para evitar warnings
import plotly.io as pio
pio.templates.default = "plotly_white"

def load_data():
    """Cargar datos desde archivos CSV"""
    try:
        # Intentar cargar desde diferentes archivos de datos reales
        data_files = [
            Path("data/processed/datos_reales_consolidados.csv"),
            Path("data/processed/sueldos_reales_consolidado.csv"),
            Path("data/processed/sueldos_consolidado_final_small.csv"),
            Path("data/processed/datos_extraidos_final.csv"),
            Path("data/raw/consolidado/2025-09/todos_los_datos.csv")
        ]
        
        for csv_file in data_files:
            if csv_file.exists():
                st.success(f"✅ Cargando datos reales desde: {csv_file.name}")
                df = pd.read_csv(csv_file)
                if len(df) > 0:
            return df
        
        # Si no se encuentran datos reales, crear datos de ejemplo
        st.warning("⚠️ No se encontraron datos consolidados. Mostrando datos de ejemplo.")
        return create_sample_data()
        
    except Exception as e:
        st.error(f"❌ Error cargando datos: {e}")
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
        # Filtrar sueldos razonables (más permisivo)
        df = df[(df['sueldo_bruto'] >= 100000) & (df['sueldo_bruto'] <= 10000000)]
    
    # Limpiar categorías
    if 'categoria_organismo' in df.columns:
        df['categoria_organismo'] = df['categoria_organismo'].fillna('Otros')
    
    # Limpiar organismos
    if 'organismo' in df.columns:
    df['organismo'] = df['organismo'].fillna('Sin especificar')
    
    # Limpiar estamentos
    if 'estamento' in df.columns:
    df['estamento'] = df['estamento'].fillna('Sin especificar')
    
    # Limpiar nombres y cargos
    if 'nombre' in df.columns:
        df['nombre'] = df['nombre'].fillna('Sin especificar')
    if 'cargo' in df.columns:
        df['cargo'] = df['cargo'].fillna('Sin especificar')
    
    # Seleccionar solo las columnas relevantes para mostrar
    relevant_columns = ['organismo', 'categoria_organismo', 'estamento', 'cargo', 'nombre', 'sueldo_bruto']
    available_columns = [col for col in relevant_columns if col in df.columns]
    
    if available_columns:
        df = df[available_columns]
    
    return df

def calculate_gini(salaries):
    """Calcular el coeficiente de Gini para medir desigualdad salarial"""
    if len(salaries) == 0:
        return 0
    
    # Ordenar salarios
    sorted_salaries = np.sort(salaries)
    n = len(sorted_salaries)
    
    # Calcular Gini usando la fórmula estándar
    cumsum = np.cumsum(sorted_salaries)
    return (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n

def main():
    """Función principal del dashboard"""
    
    # CSS personalizado
    st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
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
        st.markdown("### 📊 Métricas Principales")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">📊 {len(df):,}</div>
                <div class="metric-label">Total Registros</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            promedio = df['sueldo_bruto'].mean()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">💰 ${promedio:,.0f}</div>
                <div class="metric-label">Promedio Sueldo</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            mediana = df['sueldo_bruto'].median()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">📈 ${mediana:,.0f}</div>
                <div class="metric-label">Mediana Sueldo</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            organismos_unicos = df['organismo'].nunique() if 'organismo' in df.columns else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">🏛️ {organismos_unicos}</div>
                <div class="metric-label">Organismos</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Métricas adicionales
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            sueldo_max = df['sueldo_bruto'].max()
            st.metric("🔝 Sueldo Máximo", f"${sueldo_max:,.0f}")
        
        with col6:
            sueldo_min = df['sueldo_bruto'].min()
            st.metric("🔻 Sueldo Mínimo", f"${sueldo_min:,.0f}")
        
        with col7:
            ratio_max_min = sueldo_max / sueldo_min if sueldo_min > 0 else 0
            st.metric("⚖️ Ratio Max/Min", f"{ratio_max_min:.1f}x")
        
        with col8:
            gini_coefficient = calculate_gini(df['sueldo_bruto'].values)
            st.metric("📊 Índice de Gini", f"{gini_coefficient:.3f}")
        
        # Tabs para diferentes análisis
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Por Estamento", "🏛️ Por Organismo", "🏢 Por Categoría", "📊 Análisis de Desigualdad", "📋 Datos Raw"])
        
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
            st.subheader("📊 Análisis de Desigualdad")
            
            # Métricas de desigualdad
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🔍 Métricas de Desigualdad")
                
                # Índice de Gini
                gini_coefficient = calculate_gini(df['sueldo_bruto'].values)
                st.metric("Índice de Gini", f"{gini_coefficient:.3f}")
                
                # Interpretación del Gini
                if gini_coefficient < 0.3:
                    st.success("📈 **Baja desigualdad** - Distribución relativamente equitativa")
                elif gini_coefficient < 0.5:
                    st.warning("📊 **Desigualdad moderada** - Distribución con diferencias notables")
                else:
                    st.error("📉 **Alta desigualdad** - Distribución muy desigual")
                
                # Percentiles
                p90_p10_ratio = df['sueldo_bruto'].quantile(0.9) / df['sueldo_bruto'].quantile(0.1)
                st.metric("Ratio P90/P10", f"{p90_p10_ratio:.1f}x")
                
                # Coeficiente de variación
                cv = df['sueldo_bruto'].std() / df['sueldo_bruto'].mean()
                st.metric("Coeficiente de Variación", f"{cv:.2f}")
            
            with col2:
                st.subheader("📈 Distribución de Sueldos")
                
                # Histograma de distribución
                fig_hist = px.histogram(
                    df, 
                x='sueldo_bruto',
                    nbins=50,
                    title="Distribución de Sueldos",
                    labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'}
                )
                fig_hist.update_layout(height=400)
                st.plotly_chart(fig_hist, width='stretch')
            
            # Análisis por categorías
            if 'categoria_organismo' in df.columns:
                st.subheader("🏢 Desigualdad por Categoría")
                
                categoria_gini = df.groupby('categoria_organismo')['sueldo_bruto'].apply(calculate_gini).sort_values(ascending=False)
                
                fig_gini = px.bar(
                    x=categoria_gini.index,
                    y=categoria_gini.values,
                    title="Índice de Gini por Categoría de Organismo",
                    labels={'x': 'Categoría', 'y': 'Índice de Gini'},
                    color=categoria_gini.values,
                color_continuous_scale='Reds'
            )
                fig_gini.update_layout(height=400)
                st.plotly_chart(fig_gini, width='stretch')
                
                # Tabla de desigualdad por categoría
                st.subheader("📋 Tabla de Desigualdad por Categoría")
                desigualdad_stats = df.groupby('categoria_organismo').agg({
                    'sueldo_bruto': ['count', 'mean', 'std', 'min', 'max']
                }).round(0)
                desigualdad_stats.columns = ['Funcionarios', 'Promedio', 'Desv_Std', 'Minimo', 'Maximo']
                desigualdad_stats['Gini'] = df.groupby('categoria_organismo')['sueldo_bruto'].apply(calculate_gini).round(3)
                desigualdad_stats['CV'] = (desigualdad_stats['Desv_Std'] / desigualdad_stats['Promedio']).round(2)
                desigualdad_stats = desigualdad_stats.sort_values('Gini', ascending=False)
                
                st.dataframe(desigualdad_stats, width='stretch')
        
        with tab5:
            st.subheader("📋 Datos Raw")
            
            # Mostrar solo las primeras 1000 filas para mejor rendimiento
            df_display = df.head(1000)
            st.dataframe(df_display, width='stretch')
            
            if len(df) > 1000:
                st.info(f"Mostrando las primeras 1,000 filas de {len(df):,} registros totales.")
        
        # Información del dataset
            st.subheader("📊 Información del Dataset")
        col1, col2 = st.columns(2)
        
        with col1:
                st.write(f"**Total de registros:** {len(df):,}")
                st.write(f"**Columnas mostradas:** {len(df.columns)}")
                st.write(f"**Memoria utilizada:** {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
                
                # Estadísticas de desigualdad
                gini_coefficient = calculate_gini(df['sueldo_bruto'].values)
                st.write(f"**Índice de Gini:** {gini_coefficient:.3f}")
                
                if gini_coefficient < 0.3:
                    st.write("📈 **Baja desigualdad** (Gini < 0.3)")
                elif gini_coefficient < 0.5:
                    st.write("📊 **Desigualdad moderada** (Gini 0.3-0.5)")
                else:
                    st.write("📉 **Alta desigualdad** (Gini > 0.5)")
        
        with col2:
                if 'categoria_organismo' in df.columns:
                    st.write("**Distribución por categoría:**")
                    categoria_dist = df['categoria_organismo'].value_counts()
                    for cat, count in categoria_dist.items():
                        st.write(f"- {cat}: {count:,} ({count/len(df)*100:.1f}%)")
                
                if 'estamento' in df.columns:
                    st.write("**Distribución por estamento:**")
                    estamento_dist = df['estamento'].value_counts()
                    for est, count in estamento_dist.items():
                        st.write(f"- {est}: {count:,} ({count/len(df)*100:.1f}%)")
    
    else:
        st.warning("No hay datos que coincidan con los filtros seleccionados")

if __name__ == "__main__":
    main()
