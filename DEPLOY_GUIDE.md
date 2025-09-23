# 🚀 Guía de Deploy - Dashboard de Transparencia Salarial

## Opción 1: Streamlit Community Cloud (RECOMENDADO - GRATIS)

### Pasos:

1. **Crear cuenta en GitHub** (si no tienes):
   - Ve a https://github.com
   - Crea una cuenta gratuita

2. **Subir tu proyecto a GitHub**:
   ```bash
   git init
   git add .
   git commit -m "Dashboard de transparencia salarial"
   git branch -M main
   git remote add origin https://github.com/tu_usuario/transparencia_sueldos.git
   git push -u origin main
   ```

3. **Conectar con Streamlit Cloud**:
   - Ve a https://share.streamlit.io
   - Inicia sesión con tu cuenta de GitHub
   - Haz clic en "New app"
   - Selecciona tu repositorio: `tu_usuario/transparencia_sueldos`
   - Branch: `main`
   - Main file path: `dashboard/app.py`
   - Haz clic en "Deploy!"

4. **Configurar variables de entorno** (opcional):
   - En la página de tu app en Streamlit Cloud
   - Ve a "Settings" > "Secrets"
   - Copia el contenido de `.streamlit/secrets.toml`

### ✅ Ventajas:
- **Completamente gratuito**
- **Deploy automático** desde GitHub
- **URL pública** permanente
- **Actualizaciones automáticas** cuando haces push

### ⚠️ Limitaciones:
- **Solo funciona con datos estáticos** (no puede actualizar datos automáticamente)
- **Límite de uso** (1000 horas/mes gratis)

---

## Opción 2: Heroku (GRATIS con limitaciones)

### Pasos:

1. **Instalar Heroku CLI**:
   - Descarga desde https://devcenter.heroku.com/articles/heroku-cli

2. **Crear Procfile**:
   ```
   web: streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0
   ```

3. **Deploy**:
   ```bash
   heroku login
   heroku create tu-app-transparencia
   git push heroku main
   ```

### ✅ Ventajas:
- **Gratuito** (con limitaciones)
- **Puede ejecutar scripts** de actualización

### ⚠️ Limitaciones:
- **Se duerme** después de 30 min de inactividad
- **Límite de horas** gratuitas

---

## Opción 3: Railway (RECOMENDADO para producción)

### Pasos:

1. **Crear cuenta en Railway**:
   - Ve a https://railway.app
   - Conecta tu cuenta de GitHub

2. **Crear nuevo proyecto**:
   - Selecciona tu repositorio
   - Railway detectará automáticamente que es Python

3. **Configurar variables**:
   - `PORT`: 8501
   - `PYTHON_VERSION`: 3.11

4. **Deploy automático**:
   - Railway hará deploy automáticamente

### ✅ Ventajas:
- **$5/mes** por servicio activo
- **Siempre activo**
- **Puede ejecutar cron jobs**
- **Base de datos incluida**

---

## Opción 4: VPS (Servidor Virtual)

### Opciones populares:
- **DigitalOcean**: $6/mes
- **Linode**: $5/mes  
- **Vultr**: $3.50/mes
- **AWS EC2**: Variable

### Pasos generales:
1. Crear instancia Ubuntu
2. Instalar Docker
3. Clonar tu repositorio
4. Ejecutar con Docker Compose

---

## 🎯 Recomendación Final

**Para empezar**: Usa **Streamlit Community Cloud** (gratis)
**Para producción**: Usa **Railway** ($5/mes) si necesitas actualizaciones automáticas

## 📝 Notas Importantes

1. **Datos estáticos**: Streamlit Cloud solo puede mostrar datos que ya están en tu repositorio
2. **Actualizaciones**: Para datos en tiempo real, necesitas un servicio que permita ejecutar scripts
3. **Base de datos**: Considera usar una base de datos en la nube (PostgreSQL, MySQL) para datos dinámicos
4. **Dominio personalizado**: Todas las opciones permiten conectar un dominio personalizado

## 🔄 Automatización de Datos

Si quieres que los datos se actualicen automáticamente:

1. **Railway** + **GitHub Actions** para cron jobs
2. **VPS** con cron jobs nativos
3. **Streamlit Cloud** + **GitHub Actions** para actualizaciones programadas
