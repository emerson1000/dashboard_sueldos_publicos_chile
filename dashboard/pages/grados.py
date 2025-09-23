#!/usr/bin/env python3
"""
Análisis detallado de remuneraciones por grado del sector público chileno.

Proporciona análisis comparativos, estadísticas descriptivas y visualizaciones
específicas para cada grado del sector público.
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

def calculate_grado_stats(df, grado):
    """Calcula estadísticas para un grado específico."""
    grado_data = df[df['grado'] == grado]
    
    if grado_data.empty:
        return {}
    
    stats = {
        'total_registros': len(grado_data),
        'organismos_unicos': grado_data['organismo'].nunique(),
        'estamentos_unicos': grado_data['estamento'].nunique(),
        'promedio_sueldo': grado_data['sueldo_bruto'].mean(),
        'mediana_sueldo': grado_data['sueldo_bruto'].median(),
        'min_sueldo': grado_data['sueldo_bruto'].min(),
        'max_sueldo': grado_data['sueldo_bruto'].max(),
        'desv_std': grado_data['sueldo_bruto'].std(),
        'percentil_25': grado_data['sueldo_bruto'].quantile(0.25),
        'percentil_75': grado_data['sueldo_bruto'].quantile(0.75),
        'iqr': grado_data['sueldo_bruto'].quantile(0.75) - grado_data['sueldo_bruto'].quantile(0.25),
        'coef_variacion': grado_data['sueldo_bruto'].std() / grado_data['sueldo_bruto'].mean() if grado_data['sueldo_bruto'].mean() > 0 else 0
    }
    
    return stats

def calculate_grado_equity_metrics(df, grado):
    """Calcula métricas de equidad para un grado específico."""
    grado_data = df[df['grado'] == grado]
    
    if grado_data.empty or 'organismo' not in grado_data.columns:
        return {}
    
    equity_metrics = {}
    
    # Ratio entre organismos dentro del grado
    org_means = grado_data.groupby('organismo')['sueldo_bruto'].mean().sort_values(ascending=False)
    if len(org_means) > 1:
        equity_metrics['ratio_max_min_organismo'] = org_means.iloc[0] / org_means.iloc[-1]
        equity_metrics['diferencia_max_min_organismo'] = org_means.iloc[0] - org_means.iloc[-1]
    
    # Gini coefficient (simplificado)
    sorted_salaries = np.sort(grado_data['sueldo_bruto'].dropna())
    n = len(sorted_salaries)
    if n > 1:
        cumsum = np.cumsum(sorted_salaries)
        equity_metrics['gini_coefficient'] = (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n
    
    return equity_metrics

def main():
    st.set_page_config(page_title="Análisis por Grado", layout="wide")
    
    st.title("📊 Análisis por Grado")
    st.markdown("### Comparación detallada de remuneraciones por grado del sector público")
    
    df = load_data()
    
    if df.empty:
        st.error("🚨 No hay datos disponibles.")
        return
    
    if 'grado' not in df.columns:
        st.error("❌ El conjunto de datos no tiene columna 'grado'.")
        return
    
    grados = sorted(df['grado'].dropna().unique())
    if not grados:
        st.error("❌ No se encontraron grados en los datos.")
        return
    
    # Sidebar con filtros
    st.sidebar.header("🔍 Filtros")
    
    # Selección de grado
    selected_grado = st.sidebar.selectbox(
        "Seleccionar grado para análisis detallado",
        grados,
        index=0
    )
    
    # Filtro por organismo (opcional)
    organismos_grado = sorted(df[df['grado'] == selected_grado]['organismo'].unique())
    organismos_seleccionados = st.sidebar.multiselect(
        "Filtrar por organismos (opcional)",
        organismos_grado,
        default=organismos_grado
    )
    
    # Filtro por estamento (opcional)
    estamentos_grado = sorted(df[df['grado'] == selected_grado]['estamento'].unique())
    estamentos_seleccionados = st.sidebar.multiselect(
        "Filtrar por estamentos (opcional)",
        estamentos_grado,
        default=estamentos_grado
    )
    
    # Filtro por rango de sueldo
    grado_data = df[df['grado'] == selected_grado]
    if not grado_data.empty:
        min_sueldo, max_sueldo = st.sidebar.slider(
            "Rango de sueldo bruto",
            min_value=int(grado_data['sueldo_bruto'].min()),
            max_value=int(grado_data['sueldo_bruto'].max()),
            value=(int(grado_data['sueldo_bruto'].min()), int(grado_data['sueldo_bruto'].max())),
            format="$%d"
        )
    
    # Aplicar filtros
    df_filtered = df[df['grado'] == selected_grado].copy()
    
    if organismos_seleccionados:
        df_filtered = df_filtered[df_filtered['organismo'].isin(organismos_seleccionados)]
    
    if estamentos_seleccionados:
        df_filtered = df_filtered[df_filtered['estamento'].isin(estamentos_seleccionados)]
    
    if not grado_data.empty:
        df_filtered = df_filtered[
            (df_filtered['sueldo_bruto'] >= min_sueldo) & 
            (df_filtered['sueldo_bruto'] <= max_sueldo)
        ]
    
    # Estadísticas del grado seleccionado
    stats = calculate_grado_stats(df_filtered, selected_grado)
    equity_metrics = calculate_grado_equity_metrics(df_filtered, selected_grado)
    
    if stats:
        st.header(f"📊 Estadísticas del Grado: {selected_grado}")
        
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
    
    # Métricas de equidad
    if equity_metrics:
        st.subheader("⚖️ Métricas de Equidad por Grado")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'ratio_max_min_organismo' in equity_metrics:
                st.metric("Ratio Max/Min Organismos", f"{equity_metrics['ratio_max_min_organismo']:.2f}x")
        
        with col2:
            if 'diferencia_max_min_organismo' in equity_metrics:
                st.metric("Diferencia Max-Min", f"${equity_metrics['diferencia_max_min_organismo']:,.0f}")
        
        with col3:
            if 'gini_coefficient' in equity_metrics:
                st.metric("Coeficiente Gini", f"{equity_metrics['gini_coefficient']:.3f}")
    
    # Comparación entre grados
    st.header("📈 Comparación entre Grados")
    
    # Gráfico de barras comparativo
    grado_comparison = df.groupby('grado')['sueldo_bruto'].agg(['mean', 'median', 'count']).round(0)
    grado_comparison.columns = ['Promedio', 'Mediana', 'Cantidad']
    grado_comparison = grado_comparison[grado_comparison['Cantidad'] >= 3]  # Solo grados con al menos 3 registros
    grado_comparison = grado_comparison.sort_values('Promedio', ascending=True)
    
    fig = px.bar(
        grado_comparison.reset_index(),
        x='Promedio',
        y='grado',
        orientation='h',
        title="Comparación de Sueldos Promedio por Grado",
        labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'grado': 'Grado'},
        color='Promedio',
        color_continuous_scale='Purples',
        hover_data=['Mediana', 'Cantidad']
    )
    fig.update_layout(height=max(400, len(grado_comparison) * 25))
    st.plotly_chart(fig, use_container_width=True)
    
    # Box plot comparativo
    fig_box = px.box(
        df,
        x='grado',
        y='sueldo_bruto',
        title="Distribución de Sueldos por Grado",
        labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'grado': 'Grado'}
    )
    fig_box.update_layout(height=400)
    st.plotly_chart(fig_box, use_container_width=True)
    
    # Análisis detallado del grado seleccionado
    if not df_filtered.empty:
        st.header(f"🔍 Análisis Detallado: Grado {selected_grado}")
        
        # Tabs para diferentes análisis
        tab1, tab2, tab3, tab4 = st.tabs(["🏢 Por Organismo", "🏛️ Por Estamento", "📊 Distribución", "🔝 Top Sueldos"])
        
        with tab1:
            # Promedio por organismo
            org_stats = df_filtered.groupby('organismo').agg({
                'sueldo_bruto': ['mean', 'median', 'count']
            }).round(0)
            org_stats.columns = ['Promedio', 'Mediana', 'Cantidad']
            org_stats = org_stats[org_stats['Cantidad'] >= 2]  # Solo organismos con al menos 2 registros
            org_stats = org_stats.sort_values('Promedio', ascending=True)
            
            if not org_stats.empty:
                fig = px.bar(
                    org_stats.reset_index(),
                    x='Promedio',
                    y='organismo',
                    orientation='h',
                    title=f"Promedio de Sueldos por Organismo - Grado {selected_grado}",
                    labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'organismo': 'Organismo'},
                    color='Promedio',
                    color_continuous_scale='Greens',
                    hover_data=['Mediana', 'Cantidad']
                )
                fig.update_layout(height=max(400, len(org_stats) * 20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay suficientes datos para mostrar el análisis por organismo.")
        
        with tab2:
            # Promedio por estamento
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
                    title=f"Promedio de Sueldos por Estamento - Grado {selected_grado}",
                    labels={'Promedio': 'Sueldo Bruto Promedio ($)', 'estamento': 'Estamento'},
                    color='Promedio',
                    color_continuous_scale='Blues',
                    hover_data=['Mediana', 'Cantidad']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay suficientes datos para mostrar el análisis por estamento.")
        
        with tab3:
            # Histograma de distribución
            fig_hist = px.histogram(
                df_filtered,
                x='sueldo_bruto',
                nbins=30,
                title=f"Distribución de Sueldos - Grado {selected_grado}",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'}
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Estadísticas descriptivas
            st.subheader("📋 Estadísticas Descriptivas")
            desc_stats = df_filtered['sueldo_bruto'].describe()
            st.dataframe(desc_stats.to_frame().round(0), use_container_width=True)
        
        with tab4:
            # Top sueldos del grado
            top_sueldos = df_filtered.nlargest(20, 'sueldo_bruto')
            
            fig = px.bar(
                top_sueldos,
                x='sueldo_bruto',
                y='organismo',
                orientation='h',
                title=f"Top 20 Sueldos Más Altos - Grado {selected_grado}",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'organismo': 'Organismo'},
                color='sueldo_bruto',
                color_continuous_scale='Reds',
                hover_data=['cargo', 'estamento']
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            # Tabla detallada
            st.subheader("📋 Detalle de Top Sueldos")
            display_cols = ['organismo', 'nombre', 'cargo', 'estamento', 'sueldo_bruto']
            available_cols = [col for col in display_cols if col in top_sueldos.columns]
            st.dataframe(
                top_sueldos[available_cols].reset_index(drop=True),
                use_container_width=True
            )
    
    # Resumen ejecutivo
    st.header("📝 Resumen Ejecutivo")
    
    if stats:
        # Comparar con el promedio general
        promedio_general = df['sueldo_bruto'].mean()
        diferencia_promedio = stats['promedio_sueldo'] - promedio_general
        porcentaje_diferencia = (diferencia_promedio / promedio_general) * 100
        
        st.info(f"""
        **Grado {selected_grado}:**
        - Representa {stats['total_registros']:,} funcionarios ({stats['total_registros']/len(df)*100:.1f}% del total)
        - Sueldo promedio: ${stats['promedio_sueldo']:,.0f} ({diferencia_promedio:+,.0f} vs promedio general)
        - Sueldo mediano: ${stats['mediana_sueldo']:,.0f}
        - Rango salarial: ${stats['min_sueldo']:,.0f} - ${stats['max_sueldo']:,.0f}
        - Dispersión salarial: ${stats['desv_std']:,.0f} (coeficiente de variación: {stats['coef_variacion']:.2f})
        - Organismos representados: {stats['organismos_unicos']:,}
        - Estamentos representados: {stats['estamentos_unicos']:,}
        """)

if __name__ == '__main__':
    main()