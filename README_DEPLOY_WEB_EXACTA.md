# Desplegar la web exacta (Flask) gratis

Si quieres la **misma web actual** (mismo formato, pestañas, JS y funcionalidad), despliega `app.py` como servicio web Flask.

## Opción recomendada: Render (plan free)

## 1) Ir a Render

Entra a [https://render.com](https://render.com) y conecta tu cuenta de GitHub.

## 2) Crear Web Service desde tu repo

Selecciona el repo:

`Claudio1993cg/diagramador-v12-funcional-con-2-patios`

Render detectará `render.yaml` automáticamente.

## 3) Deploy

Render usará:

- Build: `pip install -r requirements.txt`
- Start: `gunicorn app:app --workers 1 --threads 8 --timeout 600 --bind 0.0.0.0:$PORT`

## 4) Probar login

- Admin: `ccantero` / `Juacko1993`
- Usuario: `dlopez` / `Lopez2803`

## Nota importante

El plan free puede “dormirse” por inactividad y tardar en despertar.  
Pero la interfaz será la misma de Flask, no una versión adaptada a Streamlit.
