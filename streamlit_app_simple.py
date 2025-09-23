#!/usr/bin/env python3
"""
Dashboard de Transparencia Salarial - Versión Simple para Streamlit Cloud
"""

import streamlit as st
import pandas as pd
from pathlib import Path

# Configurar página
st.set_page_config(
    page_title="Dashboard de Transparencia Salarial",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_data():
    """Carga los datos desde CSV."""
    try:
        # Intentar cargar datos reales primero
        data_file = Path("data/processed/sueldos_reales_consolidado.csv")
        if data_file.exists():
            df = pd.read_csv(data_file)
            return df
        
        # Fallback a datos consolidados
        data_file = Path("data/processed/sueldos_consolidado.csv")
        if data_file.exists():
            df = pd.read_csv(data_file)
            return df
        
        st.error("No se encontraron datos.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return pd.DataFrame()

def clean_data(df):
    """Limpia y prepara los datos."""
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
    
    return df

def main():
    """Función principal del dashboard."""
    
    # Header principal
    st.title("🏛️ Dashboard de Transparencia Salarial")
    st.subheader("Análisis de remuneraciones del sector público chileno")
    
    # Cargar datos
    df = load_data()
    
    if df.empty:
        st.error("No se pudieron cargar los datos.")
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
    
    # Métricas principales
    if not df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Registros", f"{len(df):,}")
        
        with col2:
            st.metric("💰 Promedio Sueldo", f"${df['sueldo_bruto'].mean():,.0f}")
        
        with col3:
            st.metric("📈 Mediana Sueldo", f"${df['sueldo_bruto'].median():,.0f}")
        
        with col4:
            st.metric("🏛️ Organismos", f"{df['organismo'].nunique()}")
        
        # Visualizaciones simples
        st.subheader("📊 Visualizaciones")
        
        # Tabla de datos
        st.subheader("📋 Datos")
        st.dataframe(df.head(100))
        
        # Estadísticas por estamento
        if 'estamento' in df.columns:
            st.subheader("📈 Promedio por Estamento")
            estamento_stats = df.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
            st.bar_chart(estamento_stats)
        
        # Estadísticas por organismo
        if 'organismo' in df.columns:
            st.subheader("🏛️ Top 10 Organismos")
            organismo_stats = df.groupby('organismo')['sueldo_bruto'].mean().sort_values(ascending=False).head(10)
            st.bar_chart(organismo_stats)
        
        # Distribución de sueldos
        st.subheader("📊 Distribución de Sueldos")
        st.histogram(df['sueldo_bruto'], bins=30)
        
        # Información del dataset
        st.subheader("ℹ️ Información del Dataset")
        st.info(f"""
        **Datos incluidos:**
        - {len(df):,} registros de funcionarios
        - {df['organismo'].nunique()} organismos diferentes
        - {df['estamento'].nunique()} estamentos únicos
        - Fuentes: SII, Ministerios, Transparencia Activa
        """)
    
    else:
        st.warning("No hay datos que coincidan con los filtros seleccionados.")

if __name__ == "__main__":
    main()
