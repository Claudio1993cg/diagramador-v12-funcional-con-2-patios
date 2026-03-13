# Ejecución, análisis y qué modificar

## 1. Qué se ejecutó y qué se analizó

- **Ejecución:** Diagramador con `datos_salidas.xlsx` actual (en `diagramador_optimizado/`). Con el archivo mínimo de 4 viajes que hay en el repo: 4 viajes → 3 buses, 3 conductores, 5 iteraciones, Fase 3 sin reducción.
- **Salida:** `diagramador_optimizado/diagramador_optimizado/resultado_ejecucion.xlsx` (por cómo se resuelve la raíz al ejecutar desde la raíz del repo).
- **Análisis:** `analizar_resultado.py` sobre ese Excel. **Todas las reglas cumplidas**, incluido **FnS sin desconexión (gap ≤ 1 min)**.

---

## 2. Qué modificar (recomendaciones)

### 2.1 Datos de entrada (obligatorio para operación real)

- **Archivo:** `diagramador_optimizado/datos_salidas.xlsx`.
- **Situación:** Ahora es un archivo mínimo de 4 viajes (creado para pruebas). Para tu operación real hace falta el Excel con **todos los viajes** (p. ej. 1281).
- **Qué hacer:** Sustituir por tu archivo real o cargarlo desde la web para que se guarde ahí. No requiere cambios de código.

### 2.2 Ruta del resultado al ejecutar desde la raíz del repo (opcional)

- **Comportamiento:** Si ejecutas desde la raíz del proyecto y pasas `archivo_salida='diagramador_optimizado/resultado_ejecucion.xlsx'`, la raíz que usa el CLI es `diagramador_optimizado/`, así que el archivo se escribe en `diagramador_optimizado/diagramador_optimizado/resultado_ejecucion.xlsx`.
- **Qué hacer (si quieres el archivo en `diagramador_optimizado/`):**
  - Ejecutar sin argumentos (usa por defecto `resultado_diagramacion.xlsx` en `diagramador_optimizado/`), o
  - Pasar solo el nombre: `archivo_salida='resultado_ejecucion.xlsx'` para que quede en `diagramador_optimizado/resultado_ejecucion.xlsx`.

### 2.3 Analizar el archivo que tú generes

- **Por defecto:** `analizar_resultado.py` busca en varias rutas (entre ellas `diagramador_optimizado/resultado_diagramacion.xlsx` y `resultado_ejecucion.xlsx`).
- **Qué hacer:** Si guardas el resultado con otro nombre o en otra carpeta, pásalo como argumento:  
  `python analizar_resultado.py "ruta/al/archivo.xlsx"`

### 2.4 Nada crítico que modificar en la lógica

- Las correcciones (FnS al cierre real, InS antes del primer evento, orden por conductor, bus_id_inicial e inicio/fin de jornada, paradas largas como “Tiempo en depósito”, reasignación de comerciales) están aplicadas.
- El análisis sobre el resultado de esta ejecución **pasa todas las reglas**; en particular FnS ya no tiene gap > 1 min.

---

## 3. Resumen

| Acción | Prioridad | Dónde |
|--------|-----------|--------|
| Usar tu `datos_salidas.xlsx` completo para operación real | Alta | `diagramador_optimizado/datos_salidas.xlsx` o carga por web |
| Ajustar nombre/ruta de salida si no quieres carpeta duplicada | Baja | Al invocar `main(archivo_salida=...)` usar `resultado_ejecucion.xlsx` sin prefijo de carpeta |
| Pasar ruta al analizador si el resultado está en otro archivo | Baja | `python analizar_resultado.py "ruta/al/resultado.xlsx"` |
| Cambios en código / reglas de negocio | No necesario | — |

Tras poner tu Excel real de viajes y volver a ejecutar, conviene correr de nuevo `analizar_resultado.py` sobre el nuevo resultado como control de calidad.
