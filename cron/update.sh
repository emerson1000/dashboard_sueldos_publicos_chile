#!/bin/bash
# Ejecuta el pipeline ETL completo con monitoreo y logging mejorado.

set -e

# Configuración
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/etl_run_$TIMESTAMP.log"

# Crear directorio de logs si no existe
mkdir -p "$LOG_DIR"

# Función para logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Función para manejar errores
handle_error() {
    log "ERROR: Pipeline falló en el paso: $1"
    log "Ver logs en: $LOG_FILE"
    exit 1
}

# Función para verificar si un comando fue exitoso
check_success() {
    if [ $? -eq 0 ]; then
        log "SUCCESS: $1 completado exitosamente"
    else
        handle_error "$1"
    fi
}

log "=== INICIANDO PIPELINE ETL ==="
log "Directorio del proyecto: $PROJECT_DIR"
log "Archivo de log: $LOG_FILE"

# Cambiar al directorio del proyecto
cd "$PROJECT_DIR"

# Paso 1: Extracción DIPRES
log "Paso 1: Extrayendo datos de DIPRES..."
python etl/extract_dipres.py >> "$LOG_FILE" 2>&1
check_success "Extracción DIPRES"

# Paso 2: Extracción SII
log "Paso 2: Extrayendo datos de SII..."
python etl/extract_sii.py >> "$LOG_FILE" 2>&1
check_success "Extracción SII"

# Paso 3: Extracción Contraloría
log "Paso 3: Extrayendo datos de Contraloría..."
python etl/extract_contraloria.py >> "$LOG_FILE" 2>&1
check_success "Extracción Contraloría"

# Paso 4: Transformación
log "Paso 4: Transformando datos..."
python etl/transform.py >> "$LOG_FILE" 2>&1
check_success "Transformación"

# Paso 5: Carga
log "Paso 5: Cargando datos a la base de datos..."
python etl/load.py >> "$LOG_FILE" 2>&1
check_success "Carga"

# Paso 6: Monitoreo
log "Paso 6: Ejecutando verificaciones de salud..."
python etl/monitor.py >> "$LOG_FILE" 2>&1
check_success "Monitoreo"

# Resumen final
log "=== PIPELINE ETL COMPLETADO EXITOSAMENTE ==="
log "Timestamp: $TIMESTAMP"
log "Log guardado en: $LOG_FILE"

# Limpiar logs antiguos (mantener solo los últimos 10)
log "Limpiando logs antiguos..."
cd "$LOG_DIR"
ls -t etl_run_*.log | tail -n +11 | xargs -r rm -f

log "Pipeline ETL finalizado exitosamente"