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

## 5) Evitar cold starts (opcional pero recomendado)

El plan free de Render duerme el servicio tras ~15 min sin tráfico. Al despertar tarda ~1 minuto.

**Solución gratuita:** Usar UptimeRobot para hacer ping cada 5 minutos.

1. Entra a [https://uptimerobot.com](https://uptimerobot.com) y crea cuenta gratis.
2. **Add New Monitor**
3. Configura:
   - **Monitor Type:** HTTP(s)
   - **Friendly Name:** Diagramador Web
   - **URL:** `https://diagramador-v12-web.onrender.com/healthz`
   - **Monitoring Interval:** 5 minutes
4. Guarda. Con eso la app se mantiene despierta y responde rápido siempre.

## Nota importante

El plan free puede “dormirse” por inactividad y tardar en despertar.  
Pero la interfaz será la misma de Flask, no una versión adaptada a Streamlit.
