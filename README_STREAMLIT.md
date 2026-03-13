# Despliegue en GitHub + Streamlit Cloud

## 1) Preparar repositorio local

```bash
git init
git add .
git commit -m "Inicializa proyecto con app web y Streamlit"
```

## 2) Crear repo en GitHub

1. Crea un repositorio nuevo (por ejemplo `diagramador-web`).
2. Conecta tu local:

```bash
git remote add origin https://github.com/TU_USUARIO/diagramador-web.git
git branch -M main
git push -u origin main
```

## 3) Publicar en Streamlit Community Cloud

1. Entra a [https://share.streamlit.io](https://share.streamlit.io)
2. `New app`
3. Selecciona tu repo y rama `main`
4. **Main file path**: `streamlit_app.py`
5. Deploy

## 4) Usuarios iniciales

- Admin: `ccantero` / `Juacko1993`
- Usuario normal: `dlopez` / `Lopez2803`

## 5) Consideraciones

- Streamlit Cloud usa almacenamiento efímero: archivos subidos y resultados pueden perderse al reiniciar.
- Si necesitas persistencia real (archivos permanentes), agrega almacenamiento externo (S3, GDrive, DB, etc.).
