# 🌐 Deploy del Dashboard de Transparencia Salarial

## 🚀 Deploy Rápido con Streamlit Cloud

### 1. Preparar el repositorio
```bash
# Asegúrate de estar en el directorio del proyecto
cd transparencia_sueldos

# Inicializar git si no está inicializado
git init

# Agregar todos los archivos
git add .

# Hacer commit inicial
git commit -m "Dashboard de transparencia salarial - versión inicial"
```

### 2. Subir a GitHub
```bash
# Crear repositorio en GitHub primero, luego:
git remote add origin https://github.com/TU_USUARIO/transparencia_sueldos.git
git branch -M main
git push -u origin main
```

### 3. Deploy en Streamlit Cloud
1. Ve a https://share.streamlit.io
2. Inicia sesión con tu cuenta de GitHub
3. Haz clic en "New app"
4. Configuración:
   - **Repository**: `TU_USUARIO/transparencia_sueldos`
   - **Branch**: `main`
   - **Main file path**: `dashboard/app.py`
5. Haz clic en "Deploy!"

### 4. ¡Listo! 🎉
Tu dashboard estará disponible en: `https://TU_USUARIO-transparencia-sueldos-dashboard-app-xxxxxx.streamlit.app`

---

## 🔄 Actualización Automática de Datos

### Opción A: GitHub Actions (Recomendado)
El archivo `.github/workflows/update-data.yml` ya está configurado para:
- Actualizar datos automáticamente el primer día de cada mes
- Ejecutar manualmente cuando necesites

### Opción B: Actualización Manual
```bash
# Ejecutar localmente y hacer push
python etl/extract_dipres.py
python etl/extract_sii.py
python etl/extract_contraloria.py
python etl/transform.py
python etl/load.py

git add data/
git commit -m "Actualización de datos"
git push
```

---

## 🎯 Características del Deploy

### ✅ Lo que funciona:
- **Dashboard completo** con todas las visualizaciones
- **Datos estáticos** cargados desde el repositorio
- **Análisis avanzados** y métricas de equidad
- **Interfaz responsive** para móviles y desktop
- **Actualizaciones automáticas** con GitHub Actions

### 📊 Datos incluidos:
- **47 registros** de sueldos públicos
- **6 estamentos** diferentes
- **Múltiples fuentes** (DIPRES, SII, Contraloría)
- **Análisis estadísticos** completos

### 🔧 Configuración incluida:
- **Streamlit config** optimizado para producción
- **Secrets management** para variables sensibles
- **GitHub Actions** para automatización
- **Documentación completa** de deploy

---

## 🆘 Solución de Problemas

### Error: "No module named 'streamlit'"
- Verifica que `requirements.txt` esté en la raíz del proyecto
- Asegúrate de que todas las dependencias estén listadas

### Error: "File not found"
- Verifica que `dashboard/app.py` exista
- Asegúrate de que la estructura de carpetas sea correcta

### Error: "Database not found"
- Los datos deben estar en `data/sueldos.db`
- Ejecuta el ETL localmente antes del deploy

### Dashboard no se actualiza
- Verifica que GitHub Actions esté habilitado en tu repositorio
- Revisa los logs en la pestaña "Actions" de GitHub

---

## 📈 Próximos Pasos

1. **Deploy inicial** con Streamlit Cloud
2. **Configurar dominio personalizado** (opcional)
3. **Monitorear uso** y métricas
4. **Considerar upgrade** a Railway si necesitas más funcionalidades

---

## 🎉 ¡Felicidades!

Tu dashboard de transparencia salarial ahora será accesible públicamente y se actualizará automáticamente cada mes. 

**¡La información transparente será realmente transparente!** 🏛️✨
