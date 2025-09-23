# 📖 Guía de Usuario - Transparencia Salarial Chile

## 🚀 Inicio Rápido

### Para Usuarios Finales

1. **Acceder al Dashboard**
   - Abrir navegador web
   - Ir a `http://localhost:8501` (o la URL de tu servidor)
   - El dashboard se carga automáticamente

2. **Navegación Básica**
   - **Página Principal**: Vista general con métricas principales
   - **Análisis por Estamento**: Comparar diferentes estamentos
   - **Análisis por Grado**: Analizar remuneraciones por grado
   - **Análisis por Institución**: Comparar instituciones
   - **Análisis Avanzado**: Herramientas estadísticas avanzadas

### Para Administradores

1. **Ejecutar Actualización Manual**
   ```bash
   # Opción 1: Script automatizado
   bash cron/update.sh
   
   # Opción 2: Pasos individuales
   python etl/extract_dipres.py
   python etl/extract_sii.py
   python etl/extract_contraloria.py
   python etl/transform.py
   python etl/load.py
   ```

2. **Verificar Estado del Sistema**
   ```bash
   python etl/monitor.py
   ```

## 📊 Uso del Dashboard

### Página Principal

#### Métricas Principales
- **Total Registros**: Número total de funcionarios en la base de datos
- **Promedio Sueldo**: Sueldo promedio del sector público
- **Mediana Sueldo**: Sueldo mediano (valor central)
- **Organismos**: Número de instituciones diferentes

#### Métricas de Equidad
- **Ratio Max/Min Estamentos**: Comparación entre estamentos con mayor y menor sueldo
- **Diferencia Max-Min**: Diferencia absoluta entre sueldos extremos
- **Coeficiente Gini**: Medida de desigualdad (0 = perfecta igualdad, 1 = máxima desigualdad)

#### Filtros Disponibles
- **Organismos**: Seleccionar instituciones específicas
- **Estamentos**: Filtrar por tipo de estamento
- **Rango de Sueldo**: Definir límites mínimo y máximo

#### Visualizaciones
- **Por Estamento**: Gráfico de barras horizontal con promedios
- **Por Organismo**: Top 20 instituciones por sueldo promedio
- **Distribución**: Histograma de frecuencias salariales
- **Top Sueldos**: Los 20 sueldos más altos del sistema

### Análisis por Estamento

#### Selección de Estamento
1. Usar el selector en la barra lateral
2. Elegir el estamento de interés
3. Los filtros adicionales son opcionales

#### Información Mostrada
- **Estadísticas del Estamento**: Métricas específicas del estamento seleccionado
- **Comparación entre Estamentos**: Vista general de todos los estamentos
- **Análisis Detallado**: Tabs con diferentes perspectivas

#### Tabs de Análisis Detallado
- **Por Organismo**: Promedios por institución dentro del estamento
- **Distribución**: Histograma y estadísticas descriptivas
- **Top Sueldos**: Los sueldos más altos del estamento

### Análisis por Grado

#### Funcionalidades Similares
- Misma estructura que análisis por estamento
- Filtros específicos para grado seleccionado
- Comparaciones entre diferentes grados

#### Métricas Específicas
- **Equidad por Grado**: Coeficiente de Gini específico del grado
- **Dispersión**: Variabilidad salarial dentro del grado
- **Correlaciones**: Relación entre grado y remuneración

### Análisis por Institución

#### Selección de Institución
1. Elegir institución del selector
2. Aplicar filtros adicionales si es necesario
3. Explorar análisis detallado

#### Métricas de Equidad Interna
- **Ratio Max/Min Estamentos**: Equidad interna de la institución
- **Coeficiente Gini**: Desigualdad interna
- **Comparación con Promedio General**: Posición relativa

#### Análisis Detallado
- **Por Estamento**: Distribución interna por estamento
- **Distribución**: Histograma de sueldos de la institución
- **Top Sueldos**: Sueldos más altos de la institución
- **Datos Completos**: Tabla completa con opciones de ordenamiento

### Análisis Avanzado

#### Tipos de Análisis Disponibles

##### 📊 Detección de Outliers
- **Método IQR**: Detecta valores extremos usando rango intercuartílico
- **Método Z-Score**: Identifica outliers usando desviación estándar
- **Visualización**: Box plot mostrando distribución y outliers

##### 📈 Análisis de Correlaciones
- **Matriz de Correlación**: Relaciones entre variables numéricas
- **Correlaciones Más Altas**: Ranking de relaciones más fuertes
- **Interpretación**: Valores cercanos a 1 o -1 indican correlación fuerte

##### 🎯 Clustering
- **Agrupación Automática**: Identifica patrones salariales similares
- **5 Clusters**: Agrupación en 5 categorías diferentes
- **Visualización**: Scatter plot y histogramas por cluster

##### ⚖️ Análisis de Equidad
- **Coeficiente de Gini**: Medida global de desigualdad
- **Ratios Percentiles**: Comparaciones P90/P10 y P75/P25
- **Curva de Lorenz**: Visualización de distribución de ingresos
- **Equidad por Estamento**: Comparación de desigualdad interna

##### 📉 Análisis de Distribución
- **Estadísticas Descriptivas**: Resumen numérico completo
- **Prueba de Normalidad**: Verificación si los datos siguen distribución normal
- **Histograma con Curva Normal**: Comparación visual con distribución teórica

##### 🔍 Análisis Exploratorio
- **Resumen General**: Métricas básicas del dataset
- **Distribución por Fuente**: Proporción de datos por origen
- **Top Organismos**: Instituciones con más registros
- **Completitud de Datos**: Porcentaje de campos completos por columna

## 🔧 Configuración y Personalización

### Variables de Entorno Importantes

```bash
# Configuración de Email (para alertas)
SMTP_SERVER=smtp.gmail.com
FROM_EMAIL=tu-email@gmail.com
TO_EMAIL=admin@tudominio.com
EMAIL_PASSWORD=tu-password

# Configuración del Dashboard
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Configuración de Monitoreo
HEALTH_CHECK_INTERVAL=3600
DATA_FRESHNESS_THRESHOLD=24
```

### Personalización de Visualizaciones

#### Colores y Temas
- Modificar `dashboard/.streamlit/config.toml`
- Cambiar colores primarios y secundarios
- Ajustar fuente y estilo

#### Filtros por Defecto
- Modificar valores por defecto en cada página
- Ajustar número de organismos mostrados inicialmente
- Configurar rangos de sueldo por defecto

## 🚨 Solución de Problemas

### Problemas Comunes

#### Dashboard No Carga
1. Verificar que el puerto 8501 esté disponible
2. Revisar logs en `logs/` directory
3. Verificar que la base de datos existe: `data/sueldos.db`

#### Datos No Actualizados
1. Ejecutar pipeline ETL manualmente
2. Verificar conectividad con fuentes de datos
3. Revisar logs de extracción

#### Errores de Visualización
1. Verificar que hay datos suficientes
2. Ajustar filtros para incluir más datos
3. Revisar logs del dashboard

### Comandos de Diagnóstico

```bash
# Verificar estado del sistema
python etl/monitor.py

# Verificar datos en la base
sqlite3 data/sueldos.db "SELECT COUNT(*) FROM sueldos;"

# Verificar logs recientes
tail -f logs/etl_run_*.log

# Verificar espacio en disco
df -h data/
```

## 📞 Soporte

### Información para Reportar Problemas
- **Versión del sistema**: Fecha de última actualización
- **Sistema operativo**: Windows/Linux/Mac
- **Navegador**: Chrome/Firefox/Safari
- **Logs relevantes**: Copiar mensajes de error
- **Pasos para reproducir**: Descripción detallada

### Recursos Adicionales
- **Documentación técnica**: README.md
- **Logs del sistema**: Directorio `logs/`
- **Configuración**: Archivo `env.example`
- **Docker**: `docker-compose.yml`

---

**Nota**: Esta guía cubre las funcionalidades principales. Para información técnica detallada, consultar el README.md principal.
