# 🏛️ Dashboard de Transparencia Salarial Chile

## 📊 **Sistema Completo con Datos Reales**

Dashboard interactivo que visualiza **19,224 registros reales** de remuneraciones del sector público chileno, extraídos de fuentes oficiales de transparencia.

### 🌐 **Acceso en Vivo**
**[🚀 Ver Dashboard en Streamlit Cloud](https://dashboard-sueldos-publicos-chile.streamlit.app/)**

## ✅ **Características Principales**

### 📈 **Datos Reales Consolidados**
- **19,224 registros** de funcionarios públicos reales
- **349 organismos** únicos (municipalidades, servicios, ministerios)
- **8 estamentos** diferentes
- **98.1%** de registros con nombres reales

### 🏢 **Distribución por Categoría**
- **Municipalidades**: 15,671 registros (347 organismos)
- **Servicios**: 3,187 registros (SII)
- **Ministerios**: 366 registros (Ministerio del Trabajo)

### 💰 **Sueldos Promedio por Categoría**
- **Servicios**: $3,185,301 (promedio)
- **Municipalidades**: $706,223 (promedio)
- **Ministerios**: $402,206 (promedio)

## 🔍 **Fuentes de Datos Oficiales**

### ✅ **Datos 100% Reales y Oficiales**
- **datos.gob.cl**: API oficial del gobierno (15,671 registros)
- **Servicio de Impuestos Internos**: Transparencia activa (3,187 registros)
- **Ministerio del Trabajo**: Portal oficial (366 registros)

### 📋 **Ejemplos de URLs Oficiales**
- `remuneracion-concejal-2021.csv`
- `sueldos-salud.csv`
- `sueldosdaem2018.csv`
- `https://www.mintrab.gob.cl/transparencia/remuneraciones.html`

## 🎯 **Funcionalidades del Dashboard**

### 🔍 **Filtros Jerárquicos**
1. **Categoría de Organismo** (Municipalidad/Servicio/Ministerio)
2. **Organismo Específico** (dentro de la categoría)
3. **Estamento** (Directivo, Auxiliar, Profesional, etc.)
4. **Rango de Sueldo**

### 📊 **Visualizaciones Interactivas**
- **Por Estamento**: Promedio de sueldos por categoría laboral
- **Por Organismo**: Top 20 organismos por sueldo promedio
- **Por Categoría**: Análisis específico municipalidades vs servicios vs ministerios
- **Distribución**: Histograma de sueldos
- **Top Sueldos**: Los 20 sueldos más altos
- **Datos Raw**: Tabla completa con filtros

### ⚖️ **Métricas de Equidad**
- Ratio máximo/mínimo entre estamentos
- Diferencia absoluta entre sueldos extremos
- Coeficiente de Gini simplificado

## 🛠️ **Tecnologías Utilizadas**

- **Frontend**: Streamlit con Plotly
- **Backend**: Python con pandas, requests, BeautifulSoup
- **Base de Datos**: SQLite para tracking y cache
- **Procesamiento**: ThreadPoolExecutor para extracción paralela
- **Visualización**: Plotly Express con gráficos interactivos

## 🚀 **Instalación y Uso Local**

### 📋 **Requisitos**
```bash
pip install streamlit pandas plotly numpy requests beautifulsoup4
```

### 🏃 **Ejecución**
```bash
# Dashboard principal
streamlit run dashboard_real_data.py

# Sistema de extracción
python run_real_extraction.py --step full

# Verificación de datos
python verify_data_sources.py
```

## 📁 **Estructura del Proyecto**

```
transparencia_sueldos/
├── dashboard_real_data.py          # Dashboard principal
├── streamlit_app.py                # Punto de entrada para Streamlit Cloud
├── etl/                           # Sistema de extracción
│   ├── extract_transparencia_activa_robusto.py
│   ├── consolidate_real_data.py
│   └── get_real_transparencia_urls.py
├── data/processed/                # Datos consolidados
│   ├── datos_reales_consolidados.csv
│   └── estadisticas_datos_reales.json
├── .streamlit/                    # Configuración Streamlit
└── requirements.txt               # Dependencias
```

## 🔧 **Sistema de Extracción Robusto**

### ⚡ **Características Técnicas**
- **Manejo de timeouts**: 30s por request con reintentos automáticos
- **Procesamiento paralelo**: 8 workers simultáneos
- **Monitoreo en tiempo real**: Logging detallado y tracking en SQLite
- **Validación automática**: Limpieza y estandarización de datos
- **Manejo de errores**: Backoff exponencial y recuperación automática

### 📊 **Capacidad de Escalamiento**
- Preparado para procesar **900+ fuentes** de transparencia activa
- Sistema de batching para procesamiento eficiente
- Cache inteligente para evitar re-extracciones

## ⚖️ **Aspectos Legales y Éticos**

### ✅ **Cumplimiento Legal**
- Datos públicos de transparencia activa
- Fuentes oficiales del gobierno chileno
- Cumple con la Ley de Transparencia chilena
- Datos ya publicados en portales oficiales

### 🔒 **Privacidad**
- Solo datos ya públicos por ley
- Sin información personal sensible
- Datos de funcionarios públicos sujetos a transparencia

## 📈 **Próximas Mejoras**

- [ ] Procesamiento de PDFs con OCR
- [ ] API REST para acceso programático
- [ ] Sistema de alertas automáticas
- [ ] Análisis predictivo con ML
- [ ] Expansión a todos los organismos públicos

## 🤝 **Contribuciones**

Este proyecto está abierto a contribuciones. Para reportar problemas o sugerir mejoras, por favor crea un issue en GitHub.

## 📞 **Contacto**

- **Repositorio**: [GitHub](https://github.com/emerson1000/dashboard_sueldos_publicos_chile)
- **Dashboard**: [Streamlit Cloud](https://dashboard-sueldos-publicos-chile.streamlit.app/)

---

**🏛️ Sistema de Transparencia Salarial Chile - Promoviendo la transparencia y equidad en el sector público**