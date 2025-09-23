FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Instala dependencias del sistema necesarias para OCR y conversión de PDF
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        poppler-utils \
        tesseract-ocr \
        libtesseract-dev \
        tesseract-ocr-spa \
        curl \
        wget \
        cron \
        logrotate \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Crear usuario no-root para seguridad
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copiar archivos de configuración primero (para mejor cache de Docker)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY . /app

# Crear directorios necesarios
RUN mkdir -p /app/data/raw /app/data/processed /app/logs && \
    chown -R appuser:appuser /app

# Configurar cron para automatización
RUN echo "0 2 * * * /app/cron/update.sh >> /app/logs/cron.log 2>&1" | crontab - && \
    chown appuser:appuser /var/spool/cron/crontabs/appuser

# Configurar logrotate para los logs
RUN echo "/app/logs/*.log {\n  daily\n  missingok\n  rotate 7\n  compress\n  delaycompress\n  notifempty\n  create 644 appuser appuser\n}" > /etc/logrotate.d/app-logs

# Cambiar a usuario no-root
USER appuser

EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Por defecto ejecuta el dashboard en el contenedor. El scheduler se define en docker-compose
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.headless=true", "--server.address=0.0.0.0"]