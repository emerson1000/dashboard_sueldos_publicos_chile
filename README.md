# 🏛️ Transparencia Salarial Chile

Sistema avanzado de análisis y visualización de remuneraciones del sector público chileno. Este proyecto implementa un pipeline ETL robusto y un dashboard interactivo para consolidar y analizar datos de múltiples fuentes gubernamentales, promoviendo la transparencia y el acceso a información salarial del sector público.

## 🌟 Características Principales

### 📊 Dashboard Avanzado
- **Interfaz moderna** con visualizaciones interactivas usando Plotly
- **Métricas de equidad** incluyendo coeficiente de Gini y ratios salariales
- **Análisis por múltiples dimensiones**: estamento, grado, institución
- **Filtros dinámicos** para exploración detallada de datos
- **Análisis avanzados** con detección de outliers y clustering

### 🔄 Pipeline ETL Robusto
- **Extracción automatizada** desde DIPRES, SII y Contraloría
- **Procesamiento inteligente** con limpieza y validación de datos
- **Mapeo automático** de columnas entre diferentes fuentes
- **Sistema de logging** completo con monitoreo de salud
- **Manejo de errores** y recuperación automática

### 🚀 Despliegue en Producción
- **Containerización** completa con Docker
- **Automatización** con cron y monitoreo continuo
- **Health checks** y alertas por email
- **Logging estructurado** con rotación automática
- **Configuración flexible** mediante variables de entorno

## 📁 Estructura del Proyecto

```
transparencia_sueldos/
├── 📊 dashboard/                 # Aplicación Streamlit
│   ├── app.py                   # Dashboard principal
│   └── pages/                   # Páginas especializadas
│       ├── estamentos.py        # Análisis por estamento
│       ├── grados.py            # Análisis por grado
│       ├── instituciones.py     # Análisis por institución
│       └── analisis_avanzado.py # Análisis estadísticos avanzados
├── 🔄 etl/                      # Pipeline de datos
│   ├── extract_*.py            # Scripts de extracción
│   ├── transform.py            # Transformación y limpieza
│   ├── load.py                 # Carga a base de datos
│   └── monitor.py              # Sistema de monitoreo
├── ⏰ cron/                     # Automatización
│   └── update.sh               # Script de actualización
├── 📁 data/                     # Datos
│   ├── raw/                    # Datos originales
│   ├── processed/              # Datos procesados
│   └── sueldos.db              # Base de datos SQLite
├── 📋 logs/                     # Logs del sistema
├── 🐳 docker-compose.yml        # Orquestación de contenedores
├── 🐳 Dockerfile               # Imagen de contenedor
└── 📄 requirements.txt          # Dependencias Python
```

## 🚀 Instalación y Configuración

### Opción 1: Instalación Local

1. **Clonar el repositorio**
   ```bash
   git clone <repository-url>
   cd transparencia_sueldos
   ```

2. **Crear entorno virtual**
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variables de entorno (opcional)**
   ```bash
   cp env.example .env
   # Editar .env con tus configuraciones
   ```

5. **Ejecutar pipeline ETL**
   ```bash
   # Ejecución manual
   python etl/extract_dipres.py
   python etl/extract_sii.py
   python etl/extract_contraloria.py
   python etl/transform.py
   python etl/load.py
   
   # O ejecución automatizada
   bash cron/update.sh
   ```

6. **Lanzar dashboard**
   ```bash
   streamlit run dashboard/app.py
   ```

### Opción 2: Despliegue con Docker

1. **Configurar variables de entorno**
   ```bash
   cp env.example .env
   # Editar .env con tus configuraciones
   ```

2. **Construir y ejecutar**
   ```bash
   docker-compose up --build
   ```

3. **Acceder al dashboard**
   - Dashboard: http://localhost:8501
   - Monitoreo (opcional): http://localhost:9090

## 📊 Uso del Dashboard

### Página Principal
- **Métricas globales** del sector público
- **Visualizaciones interactivas** por estamento y organismo
- **Filtros dinámicos** para análisis específicos
- **Top sueldos** y análisis de distribución

### Páginas Especializadas

#### 🏛️ Análisis por Estamento
- Comparación entre diferentes estamentos
- Estadísticas detalladas por estamento
- Análisis de distribución salarial
- Métricas de equidad interna

#### 📊 Análisis por Grado
- Comparación salarial por grado
- Análisis de dispersión salarial
- Identificación de grados con mayor variabilidad
- Correlaciones entre grado y remuneración

#### 🏢 Análisis por Institución
- Comparación entre instituciones
- Análisis de equidad interna por institución
- Identificación de instituciones con mayor dispersión
- Ranking de instituciones por remuneración promedio

#### 🔬 Análisis Avanzado
- **Detección de outliers** usando métodos IQR y Z-Score
- **Análisis de correlaciones** entre variables
- **Clustering** para identificar patrones salariales
- **Análisis de equidad** con curva de Lorenz y coeficiente de Gini
- **Análisis de distribución** con pruebas de normalidad
- **Análisis exploratorio** de completitud de datos

## 🔧 Configuración Avanzada

### Variables de Entorno

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `SMTP_SERVER` | Servidor SMTP para alertas | - |
| `FROM_EMAIL` | Email remitente | - |
| `TO_EMAIL` | Email destinatario | - |
| `EMAIL_PASSWORD` | Contraseña del email | - |
| `LOG_LEVEL` | Nivel de logging | INFO |
| `DATA_FRESHNESS_THRESHOLD` | Umbral de frescura (horas) | 24 |

### Monitoreo y Alertas

El sistema incluye un sistema de monitoreo que verifica:
- **Frescura de datos** (última actualización)
- **Calidad de datos** (completitud y validez)
- **Tamaños de archivos** (detección de problemas)
- **Distribución de datos** (verificación de fuentes)

### Automatización

El sistema se ejecuta automáticamente:
- **Diariamente a las 2:00 AM** (configurable)
- **Con monitoreo continuo** de la salud del sistema
- **Con alertas por email** en caso de problemas
- **Con rotación automática** de logs

## 📈 Métricas y KPIs

### Métricas de Equidad
- **Coeficiente de Gini**: Medida de desigualdad salarial
- **Ratio P90/P10**: Comparación entre percentiles extremos
- **Ratio P75/P25**: Comparación entre cuartiles
- **Diferencia Max-Min**: Dispersión absoluta

### Métricas de Calidad
- **Completitud de datos**: Porcentaje de campos completos
- **Validez de sueldos**: Porcentaje de sueldos válidos
- **Diversidad de fuentes**: Número de fuentes activas
- **Frescura de datos**: Tiempo desde última actualización

## 🛠️ Desarrollo y Contribución

### Estructura de Código
- **Modular**: Cada componente es independiente
- **Documentado**: Código con docstrings completos
- **Testeable**: Estructura preparada para testing
- **Configurable**: Parámetros externos configurables

### Agregar Nuevas Fuentes
1. Crear script en `etl/extract_nueva_fuente.py`
2. Agregar mapeo en `etl/transform.py`
3. Actualizar `cron/update.sh`
4. Documentar la nueva fuente

### Agregar Nuevos Análisis
1. Crear página en `dashboard/pages/`
2. Implementar funciones de análisis
3. Agregar visualizaciones con Plotly
4. Documentar el nuevo análisis

## 🔒 Seguridad y Privacidad

- **Datos públicos**: Solo procesa información pública
- **Sin datos personales**: No almacena información personal sensible
- **Acceso controlado**: Configuración de acceso por variables de entorno
- **Logging seguro**: Logs sin información sensible

## 📞 Soporte y Contacto

Para reportar problemas o sugerir mejoras:
- Crear un issue en el repositorio
- Documentar el problema con logs relevantes
- Incluir información del entorno (OS, Python version, etc.)

## 📄 Licencia

Este proyecto está bajo licencia MIT. Ver archivo LICENSE para más detalles.

## 🙏 Agradecimientos

- **DIPRES**: Dirección de Presupuestos del Ministerio de Hacienda
- **SII**: Servicio de Impuestos Internos
- **Contraloría**: Contraloría General de la República
- **Comunidad Streamlit**: Por la excelente plataforma de dashboards
- **Comunidad Python**: Por las librerías de análisis de datos

---

**Nota**: Este proyecto es una herramienta de transparencia ciudadana. Los datos procesados son de dominio público y están disponibles en los sitios web oficiales de las instituciones mencionadas.