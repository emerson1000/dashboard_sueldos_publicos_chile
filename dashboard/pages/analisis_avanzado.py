#!/usr/bin/env python3
"""
Análisis avanzados de remuneraciones del sector público chileno.

Incluye análisis de outliers, comparaciones temporales, análisis de equidad,
correlaciones y otros análisis estadísticos avanzados.
"""

import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
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

def detect_outliers_iqr(df, column='sueldo_bruto'):
    """Detecta outliers usando el método IQR."""
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    
    return outliers, lower_bound, upper_bound

def detect_outliers_zscore(df, column='sueldo_bruto', threshold=3):
    """Detecta outliers usando el método Z-Score."""
    z_scores = np.abs(stats.zscore(df[column].dropna()))
    outlier_indices = df[column].dropna().index[z_scores > threshold]
    outliers = df.loc[outlier_indices]
    
    return outliers

def calculate_gini_coefficient(values):
    """Calcula el coeficiente de Gini."""
    values = np.sort(values)
    n = len(values)
    cumsum = np.cumsum(values)
    return (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n if cumsum[-1] > 0 else 0

def perform_clustering_analysis(df):
    """Realiza análisis de clustering en los datos."""
    # Preparar datos para clustering
    numeric_cols = ['sueldo_bruto']
    categorical_cols = ['organismo', 'estamento', 'grado']
    
    # Crear variables dummy para variables categóricas
    df_cluster = df[numeric_cols].copy()
    
    for col in categorical_cols:
        if col in df.columns:
            dummies = pd.get_dummies(df[col], prefix=col)
            df_cluster = pd.concat([df_cluster, dummies], axis=1)
    
    # Normalizar datos
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_cluster)
    
    # Aplicar K-means
    kmeans = KMeans(n_clusters=5, random_state=42)
    clusters = kmeans.fit_predict(df_scaled)
    
    # Agregar clusters al DataFrame original
    df_with_clusters = df.copy()
    df_with_clusters['cluster'] = clusters
    
    return df_with_clusters, kmeans

def main():
    st.set_page_config(page_title="Análisis Avanzado", layout="wide")
    
    st.title("🔬 Análisis Avanzado")
    st.markdown("### Análisis estadísticos avanzados y técnicas de machine learning")
    
    df = load_data()
    
    if df.empty:
        st.error("🚨 No hay datos disponibles.")
        return
    
    # Sidebar con opciones de análisis
    st.sidebar.header("🔍 Opciones de Análisis")
    
    analysis_type = st.sidebar.selectbox(
        "Seleccionar tipo de análisis",
        [
            "📊 Detección de Outliers",
            "📈 Análisis de Correlaciones",
            "🎯 Clustering",
            "⚖️ Análisis de Equidad",
            "📉 Análisis de Distribución",
            "🔍 Análisis Exploratorio"
        ]
    )
    
    # Filtros generales
    st.sidebar.subheader("🔧 Filtros")
    
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
    
    # Aplicar filtros
    df_filtered = df.copy()
    
    if organismos_seleccionados:
        df_filtered = df_filtered[df_filtered['organismo'].isin(organismos_seleccionados)]
    
    if estamentos_seleccionados:
        df_filtered = df_filtered[df_filtered['estamento'].isin(estamentos_seleccionados)]
    
    # Análisis según el tipo seleccionado
    if analysis_type == "📊 Detección de Outliers":
        st.header("📊 Detección de Outliers")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Método IQR")
            outliers_iqr, lower_bound, upper_bound = detect_outliers_iqr(df_filtered)
            
            st.metric("Total Outliers (IQR)", len(outliers_iqr))
            st.metric("Porcentaje de Outliers", f"{len(outliers_iqr)/len(df_filtered)*100:.2f}%")
            st.metric("Límite Inferior", f"${lower_bound:,.0f}")
            st.metric("Límite Superior", f"${upper_bound:,.0f}")
            
            if not outliers_iqr.empty:
                st.subheader("Top 10 Outliers (IQR)")
                display_cols = ['organismo', 'nombre', 'cargo', 'estamento', 'sueldo_bruto']
                available_cols = [col for col in display_cols if col in outliers_iqr.columns]
                st.dataframe(
                    outliers_iqr[available_cols].nlargest(10, 'sueldo_bruto').reset_index(drop=True),
                    use_container_width=True
                )
        
        with col2:
            st.subheader("Método Z-Score")
            outliers_zscore = detect_outliers_zscore(df_filtered)
            
            st.metric("Total Outliers (Z-Score)", len(outliers_zscore))
            st.metric("Porcentaje de Outliers", f"{len(outliers_zscore)/len(df_filtered)*100:.2f}%")
            
            if not outliers_zscore.empty:
                st.subheader("Top 10 Outliers (Z-Score)")
                display_cols = ['organismo', 'nombre', 'cargo', 'estamento', 'sueldo_bruto']
                available_cols = [col for col in display_cols if col in outliers_zscore.columns]
                st.dataframe(
                    outliers_zscore[available_cols].nlargest(10, 'sueldo_bruto').reset_index(drop=True),
                    use_container_width=True
                )
        
        # Visualización de outliers
        fig = px.box(
            df_filtered,
            y='sueldo_bruto',
            title="Distribución de Sueldos con Outliers",
            labels={'sueldo_bruto': 'Sueldo Bruto ($)'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    
    elif analysis_type == "📈 Análisis de Correlaciones":
        st.header("📈 Análisis de Correlaciones")
        
        # Crear matriz de correlación para variables numéricas
        numeric_data = df_filtered.select_dtypes(include=[np.number])
        
        if len(numeric_data.columns) > 1:
            correlation_matrix = numeric_data.corr()
            
            fig = px.imshow(
                correlation_matrix,
                title="Matriz de Correlación",
                color_continuous_scale='RdBu',
                aspect="auto"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            
            # Mostrar correlaciones más altas
            st.subheader("Correlaciones Más Altas")
            corr_pairs = []
            for i in range(len(correlation_matrix.columns)):
                for j in range(i+1, len(correlation_matrix.columns)):
                    corr_pairs.append({
                        'Variable 1': correlation_matrix.columns[i],
                        'Variable 2': correlation_matrix.columns[j],
                        'Correlación': correlation_matrix.iloc[i, j]
                    })
            
            corr_df = pd.DataFrame(corr_pairs)
            corr_df = corr_df.reindex(corr_df['Correlación'].abs().sort_values(ascending=False).index)
            st.dataframe(corr_df.head(10), use_container_width=True)
        else:
            st.info("No hay suficientes variables numéricas para análisis de correlación")
    
    elif analysis_type == "🎯 Clustering":
        st.header("🎯 Análisis de Clustering")
        
        if len(df_filtered) > 100:  # Necesitamos suficientes datos para clustering
            df_with_clusters, kmeans_model = perform_clustering_analysis(df_filtered)
            
            # Estadísticas por cluster
            cluster_stats = df_with_clusters.groupby('cluster').agg({
                'sueldo_bruto': ['count', 'mean', 'median', 'std'],
                'organismo': 'nunique',
                'estamento': 'nunique'
            }).round(0)
            
            cluster_stats.columns = ['Cantidad', 'Promedio', 'Mediana', 'Desv. Std', 'Organismos', 'Estamentos']
            
            st.subheader("Estadísticas por Cluster")
            st.dataframe(cluster_stats, use_container_width=True)
            
            # Visualización de clusters
            fig = px.scatter(
                df_with_clusters,
                x='organismo',
                y='sueldo_bruto',
                color='cluster',
                title="Clusters de Sueldos por Organismo",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'organismo': 'Organismo'},
                hover_data=['estamento', 'grado']
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
            
            # Distribución por cluster
            fig2 = px.histogram(
                df_with_clusters,
                x='sueldo_bruto',
                color='cluster',
                title="Distribución de Sueldos por Cluster",
                labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'},
                nbins=30
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True, config={"responsive": True})
            
        else:
            st.info("Se necesitan al menos 100 registros para realizar clustering")
    
    elif analysis_type == "⚖️ Análisis de Equidad":
        st.header("⚖️ Análisis de Equidad")
        
        # Coeficiente de Gini general
        gini_general = calculate_gini_coefficient(df_filtered['sueldo_bruto'].dropna())
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Coeficiente de Gini General", f"{gini_general:.3f}")
        
        with col2:
            # Ratio percentil 90/10
            p90 = df_filtered['sueldo_bruto'].quantile(0.9)
            p10 = df_filtered['sueldo_bruto'].quantile(0.1)
            ratio_90_10 = p90 / p10
            st.metric("Ratio P90/P10", f"{ratio_90_10:.2f}")
        
        with col3:
            # Ratio percentil 75/25
            p75 = df_filtered['sueldo_bruto'].quantile(0.75)
            p25 = df_filtered['sueldo_bruto'].quantile(0.25)
            ratio_75_25 = p75 / p25
            st.metric("Ratio P75/P25", f"{ratio_75_25:.2f}")
        
        # Análisis de equidad por estamento
        if 'estamento' in df_filtered.columns:
            st.subheader("Equidad por Estamento")
            
            estamento_gini = df_filtered.groupby('estamento')['sueldo_bruto'].apply(
                lambda x: calculate_gini_coefficient(x.dropna())
            ).sort_values(ascending=False)
            
            fig = px.bar(
                x=estamento_gini.index,
                y=estamento_gini.values,
                title="Coeficiente de Gini por Estamento",
                labels={'x': 'Estamento', 'y': 'Coeficiente de Gini'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
        
        # Curva de Lorenz
        st.subheader("Curva de Lorenz")
        
        # Ordenar sueldos
        sorted_salaries = np.sort(df_filtered['sueldo_bruto'].dropna())
        n = len(sorted_salaries)
        
        # Calcular puntos de la curva de Lorenz
        cumulative_people = np.arange(1, n + 1) / n
        cumulative_income = np.cumsum(sorted_salaries) / np.sum(sorted_salaries)
        
        # Crear gráfico
        fig = go.Figure()
        
        # Curva de Lorenz
        fig.add_trace(go.Scatter(
            x=cumulative_people,
            y=cumulative_income,
            mode='lines',
            name='Curva de Lorenz',
            line=dict(color='blue', width=2)
        ))
        
        # Línea de igualdad perfecta
        fig.add_trace(go.Scatter(
            x=[0, 1],
            y=[0, 1],
            mode='lines',
            name='Igualdad Perfecta',
            line=dict(color='red', dash='dash')
        ))
        
        fig.update_layout(
            title="Curva de Lorenz",
            xaxis_title="Proporción Acumulada de Personas",
            yaxis_title="Proporción Acumulada de Ingresos",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    
    elif analysis_type == "📉 Análisis de Distribución":
        st.header("📉 Análisis de Distribución")
        
        # Estadísticas descriptivas
        st.subheader("Estadísticas Descriptivas")
        desc_stats = df_filtered['sueldo_bruto'].describe()
        st.dataframe(desc_stats.to_frame().round(0), use_container_width=True)
        
        # Prueba de normalidad
        st.subheader("Prueba de Normalidad")
        
        # Shapiro-Wilk test (para muestras pequeñas)
        if len(df_filtered) <= 5000:
            stat, p_value = stats.shapiro(df_filtered['sueldo_bruto'].dropna())
            test_name = "Shapiro-Wilk"
        else:
            # Kolmogorov-Smirnov test (para muestras grandes)
            stat, p_value = stats.kstest(df_filtered['sueldo_bruto'].dropna(), 'norm')
            test_name = "Kolmogorov-Smirnov"
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Estadístico", f"{stat:.4f}")
        
        with col2:
            st.metric("P-valor", f"{p_value:.4f}")
        
        if p_value < 0.05:
            st.warning(f"Los datos NO siguen una distribución normal (p < 0.05)")
        else:
            st.success(f"Los datos siguen una distribución normal (p >= 0.05)")
        
        # Histograma con curva normal
        fig = px.histogram(
            df_filtered,
            x='sueldo_bruto',
            nbins=50,
            title="Distribución de Sueldos con Curva Normal",
            labels={'sueldo_bruto': 'Sueldo Bruto ($)', 'count': 'Frecuencia'}
        )
        
        # Agregar curva normal
        mean = df_filtered['sueldo_bruto'].mean()
        std = df_filtered['sueldo_bruto'].std()
        x_range = np.linspace(df_filtered['sueldo_bruto'].min(), df_filtered['sueldo_bruto'].max(), 100)
        y_normal = stats.norm.pdf(x_range, mean, std) * len(df_filtered) * (df_filtered['sueldo_bruto'].max() - df_filtered['sueldo_bruto'].min()) / 50
        
        fig.add_trace(go.Scatter(
            x=x_range,
            y=y_normal,
            mode='lines',
            name='Distribución Normal',
            line=dict(color='red', dash='dash')
        ))
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    
    elif analysis_type == "🔍 Análisis Exploratorio":
        st.header("🔍 Análisis Exploratorio de Datos")
        
        # Resumen general
        st.subheader("Resumen General")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", f"{len(df_filtered):,}")
        
        with col2:
            st.metric("Organismos Únicos", f"{df_filtered['organismo'].nunique():,}")
        
        with col3:
            st.metric("Estamentos Únicos", f"{df_filtered['estamento'].nunique():,}")
        
        with col4:
            st.metric("Grados Únicos", f"{df_filtered['grado'].nunique():,}")
        
        # Distribución por fuente
        if 'fuente' in df_filtered.columns:
            st.subheader("Distribución por Fuente")
            fuente_counts = df_filtered['fuente'].value_counts()
            
            fig = px.pie(
                values=fuente_counts.values,
                names=fuente_counts.index,
                title="Distribución de Registros por Fuente"
            )
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
        
        # Top organismos por cantidad de registros
        st.subheader("Top 10 Organismos por Cantidad de Registros")
        org_counts = df_filtered['organismo'].value_counts().head(10)
        
        fig = px.bar(
            x=org_counts.values,
            y=org_counts.index,
            orientation='h',
            title="Top 10 Organismos por Cantidad de Registros",
            labels={'x': 'Cantidad de Registros', 'y': 'Organismo'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
        
        # Análisis de completitud de datos
        st.subheader("Análisis de Completitud de Datos")
        
        completeness = {}
        for col in df_filtered.columns:
            completeness[col] = (df_filtered[col].notna().sum() / len(df_filtered)) * 100
        
        completeness_df = pd.DataFrame(list(completeness.items()), columns=['Columna', 'Completitud (%)'])
        completeness_df = completeness_df.sort_values('Completitud (%)', ascending=True)
        
        fig = px.bar(
            completeness_df,
            x='Completitud (%)',
            y='Columna',
            orientation='h',
            title="Completitud de Datos por Columna",
            labels={'Completitud (%)': 'Completitud (%)', 'Columna': 'Columna'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True})

if __name__ == '__main__':
    main()
