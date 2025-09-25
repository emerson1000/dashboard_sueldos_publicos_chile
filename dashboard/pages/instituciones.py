#!/usr/bin/env python3
"""
Análisis detallado de remuneraciones por institución del sector público chileno.

Proporciona análisis comparativos, estadísticas descriptivas y visualizaciones
específicas para cada institución del sector público.
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

def calculate_institution_stats(df, organismo):
    """Calcula estadísticas para una institución específica."""
    inst_data = df[df['organismo'] == organismo]
    
    if inst_data.empty:
        return {}
    
    stats = {
        'total_registros': len(inst_data),
        'estamentos_unicos': inst_data['estamento'].nunique(),
        'grados_unicos': inst_data['grado'].nunique(),
        'promedio_sueldo': inst_data['sueldo_bruto'].mean(),
        'mediana_sueldo': inst_data['sueldo_bruto'].median(),
        'min_sueldo': inst_data['sueldo_bruto'].min(),
        'max_sueldo': inst_data['sueldo_bruto'].max(),
        'desv_std': inst_data['sueldo_bruto'].std(),
        'percentil_25': inst_data['sueldo_bruto'].quantile(0.25),
        'percentil_75': inst_data['sueldo_bruto'].quantile(0.75),
        'iqr': inst_data['sueldo_bruto'].quantile(0.75) - inst_data['sueldo_bruto'].quantile(0.25),
        'coef_variacion': inst_data['sueldo_bruto'].std() / inst_data['sueldo_bruto'].mean() if inst_data['sueldo_bruto'].mean() > 0 else 0
    }
    
    return stats

def calculate_equity_metrics(df, organismo):
    """Calcula métricas de equidad para una institución."""
    inst_data = df[df['organismo'] == organismo]
    
    if inst_data.empty or 'estamento' not in inst_data.columns:
        return {}
    
    equity_metrics = {}
    
    # Ratio entre estamentos dentro de la institución
    estamento_means = inst_data.groupby('estamento')['sueldo_bruto'].mean().sort_values(ascending=False)
    if len(estamento_means) > 1:
        equity_metrics['ratio_max_min_estamento'] = estamento_means.iloc[0] / estamento_means.iloc[-1]
        equity_metrics['diferencia_max_min_estamento'] = estamento_means.iloc[0] - estamento_means.iloc[-1]
    
    # Gini coefficient (simplificado)
    sorted_salaries = np.sort(inst_data['sueldo_bruto'].dropna())
    n = len(sorted_salaries)
    if n > 1:
        cumsum = np.cumsum(sorted_salaries)
        equity_metrics['gini_coefficient'] = (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n
    
    return equity_metrics

def main():
    st.set_page_config(page_title="Análisis por Institución", layout="wide")
    
    st.title("🏢 Análisis por Institución")
    st.markdown("### Comparación detallada de remuneraciones por institución del sector público")
    
    df = load_data()
    
    if df.empty:
        st.error("🚨 No hay datos disponibles.")
        return
    
    if 'organismo' not in df.columns:
        st.error("❌ El conjunto de datos no tiene columna 'organismo'.")
        return
    
    instituciones = sorted(df['organismo'].dropna().unique())
    if not instituciones:
        st.error("❌ No se encontraron instituciones en los datos.")
        return
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros")
    
    # Selección de institución
    selected_institucion = st.sidebar.selectbox(
        "Seleccionar institución para análisis detallado",
        instituciones,
        index=0
    )
    
    # Filtro por estamento (opcional)
    estamentos_institucion = sorted(df[df['organismo'] == selected_institucion]['estamento'].unique())
    estamentos_seleccionados = st.sidebar.multiselect(
        "Filtrar por estamentos (opcional)",
        estamentos_institucion,
        default=estamentos_institucion
    )
    
    # Filtro por rango de sueldo
    institucion_data = df[df['organismo'] == selected_institucion]
    if not institucion_data.empty:
        min_sueldo, max_sueldo = st.sidebar.slider(
            "Rango de sueldo bruto",
            min_value=int(institucion_data['sueldo_bruto'].min()),
            max_value=int(institucion_data['sueldo_bruto'].max()),
            value=(int(institucion_data['sueldo_bruto'].min()), int(institucion_data['sueldo_bruto'].max())),
            format="$%d"
        )
    
    # Aplicar filtros
    df_filtered = df[df['organismo'] == selected_institucion].copy()
    
    if estamentos_seleccionados:
        df_filtered = df_filtered[df_filtered['estamento'].isin(estamentos_seleccionados)]
    
    if not institucion_data.empty:
        df_filtered = df_filtered[
            (df_filtered['sueldo_bruto'] >= min_sueldo) & 
            (df_filtered['sueldo_bruto'] <= max_sueldo)
        ]
    
    # Estadísticas de la institución seleccionada
    stats = calculate_institution_stats(df_filtered, selected_institucion)
    equity_metrics = calculate_equity_metrics(df_filtered, selected_institucion)
    
    if stats:
        st.header(f"📊 Estadísticas de la Institución: {selected_institucion}")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Funcionarios", f"{stats['total_registros']:,}")
        
        with col2:
            st.metric("Promedio Sueldo", f"${stats['promedio_sueldo']:,.0f}")
        
        with col3:
            st.metric("Mediana Sueldo", f"${stats['mediana_sueldo']:,.0f}")
        
        with col4:
            st.metric("Estamentos", f"{stats['estamentos_unicos']:,}")
        
        # Estadísticas adicionales
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Sueldo Mínimo", f"${stats['min_sueldo']:,.0f}")
        
        with col2:
            st.metric("Sueldo Máximo", f"${stats['max_sueldo']:,.0f}")
        
        with col3:
            st.metric("Desv. Estándar", f"${stats['desv_std']:,.0f}")
    
    # Métricas de equidad
    if equity_metrics:
        st.subheader("⚖️ Métricas de Equidad Interna")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'ratio_max_min_estamento' in equity_metrics:
                st.metric("Ratio Max/Min Estamentos", f"{equity_metrics['ratio_max_min_estamento']:.2f}x")
        
        with col2:
            if 'diferencia_max_min_estamento' in equity_metrics:
                st.metric("Diferencia Max-Min", f"${equity_metrics['diferencia_max_min_estamento']:,.0f}")
        
        with col3:
            if 'gini_coefficient' in equity_metrics:
                st.metric("Coeficiente Gini", f"{equity_metrics['gini_coefficient']:.3f}")
    
    # Comparación entre instituciones
    st.header("📈 Comparación entre Instituciones")
    
    # Top instituciones por promedio
    inst_comparison = df.groupby('organismo')['sueldo_bruto'].agg(['mean', 'median', 'count']).round(0)
    inst_comparison.columns = ['Promedio', 'Mediana', 'Cantidad']
    inst_comparison = inst_comparison[inst_comparison['Cantidad'] >= 5]  # Solo instituciones con al menos 5 registros
    inst_comparison = inst_comparison.sort_values('Promedio', ascending=True).tail(20)
    
    fig = px.bar(
        inst_comparison.reset_index(),
        x='Promedio',
        y='organismo',
        orientation='h',
        title="Top 20 Instituciones por Sueldo Promedio",
        labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'organismo': 'Institución'},
        color='Promedio',
        color_continuous_scale='Greens',
        hover_data=['Mediana', 'Cantidad']
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    
    # Análisis detallado de la institución seleccionada
    if not df_filtered.empty:
        st.header(f"🔍 Análisis Detallado: {selected_institucion}")
        
        # Tabs para diferentes análisis
        tab1, tab2, tab3, tab4 = st.tabs(["🏛️ Por Estamento", "📊 Distribución", "🔝 Top Sueldos", "📋 Datos Completos"])
        
        with tab1:
            # Promedio por estamento dentro de la institución
            estamento_stats = df_filtered.groupby('estamento').agg({
                'sueldo_bruto': ['mean', 'median', 'count']
            }).round(0)
            estamento_stats.columns = ['Promedio', 'Mediana', 'Cantidad']
            estamento_stats = estamento_stats.sort_values('Promedio', ascending=True)
            
            if not estamento_stats.empty:
                fig = px.bar(
                    estamento_stats.reset_index(),
                    x='Promedio',
                    y='estamento',
                    orientation='h',
                    title=f"Promedio de Sueldos por Estamento - {selected_institucion}",
                    labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'estamento': 'Estamento'},
                    color='Promedio',
                    color_continuous_scale='Blues',
                    hover_data=['Mediana', 'Cantidad']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
                
                # Box plot por estamento
                fig_box = px.box(
                    df_filtered,
                    x='estamento',
                    y='sueldo_bruto',
                    title=f"Distribución de Sueldos por Estamento - {selected_institucion}",
                    labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'estamento': 'Estamento'}
                )
                fig_box.update_layout(height=400)
                st.plotly_chart(fig_box, use_container_width=True)
            else:
                st.info("No hay suficientes datos para mostrar el análisis por estamento.")
        
        with tab2:
            # Histograma de distribución
            fig_hist = px.histogram(
                df_filtered,
                x='sueldo_bruto',
                nbins=30,
                title=f"Distribución de Sueldos - {selected_institucion}",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'}
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True, config={"responsive": True})
            
            # Estadísticas descriptivas
            st.subheader("📋 Estadísticas Descriptivas")
            desc_stats = df_filtered['sueldo_bruto'].describe()
            st.dataframe(desc_stats.to_frame().round(0), use_container_width=True)
        
        with tab3:
            # Top sueldos de la institución
            top_sueldos = df_filtered.nlargest(20, 'sueldo_bruto')
            
            fig = px.bar(
                top_sueldos,
                x='sueldo_bruto',
                y='estamento',
                orientation='h',
                title=f"Top 20 Sueldos Más Altos - {selected_institucion}",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'estamento': 'Estamento'},
                color='sueldo_bruto',
                color_continuous_scale='Reds',
                hover_data=['cargo', 'grado']
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            
            # Tabla detallada
            st.subheader("📋 Detalle de Top Sueldos")
            display_cols = ['nombre', 'cargo', 'grado', 'estamento', 'sueldo_bruto']
            available_cols = [col for col in display_cols if col in top_sueldos.columns]
            st.dataframe(
                top_sueldos[available_cols].reset_index(drop=True),
                use_container_width=True
            )
        
        with tab4:
            # Tabla completa de la institución
            st.subheader("📋 Datos Completos de la Institución")
            display_cols = ['nombre', 'cargo', 'grado', 'estamento', 'sueldo_bruto']
            available_cols = [col for col in display_cols if col in df_filtered.columns]
            
            # Agregar filtros adicionales para la tabla
            col1, col2 = st.columns(2)
            
            with col1:
                sort_by = st.selectbox("Ordenar por", ['sueldo_bruto', 'nombre', 'cargo', 'estamento'])
            
            with col2:
                ascending = st.selectbox("Orden", ['Descendente', 'Ascendente']) == 'Ascendente'
            
            df_sorted = df_filtered.sort_values(sort_by, ascending=ascending)
            
            st.dataframe(
                df_sorted[available_cols].reset_index(drop=True),
                use_container_width=True,
                height=600
            )
    
    # Resumen ejecutivo
    st.header("📝 Resumen Ejecutivo")
    
    if stats:
        # Comparar con el promedio general
        promedio_general = df['sueldo_bruto'].mean()
        diferencia_promedio = stats['promedio_sueldo'] - promedio_general
        porcentaje_diferencia = (diferencia_promedio / promedio_general) * 100
        
        st.info(f"""
        **Institución {selected_institucion}:**
        - Representa {stats['total_registros']:,} funcionarios ({stats['total_registros']/len(df)*100:.1f}% del total)
        - Sueldo promedio: ${stats['promedio_sueldo']:,.0f} ({diferencia_promedio:+,.0f} vs promedio general)
        - Sueldo mediano: ${stats['mediana_sueldo']:,.0f}
        - Rango salarial: ${stats['min_sueldo']:,.0f} - ${stats['max_sueldo']:,.0f}
        - Dispersión salarial: ${stats['desv_std']:,.0f} (coeficiente de variación: {stats['coef_variacion']:.2f})
        - Estamentos representados: {stats['estamentos_unicos']:,}
        """)

if __name__ == '__main__':
    main()