# Reglas de división de conductores por bus (corte en nodo de relevo)

## Resumen

La división de turnos de conductor se hace **por bus**: se toma el bloque de eventos del bus (Vacio, Comercial, Parada, …) y se corta **solo en nodos de relevo** (config: `puntos_relevo`, ej. PIE ANDINO, LOS TILOS), garantizando:

1. **Un solo conductor** si el bloque cabe en la jornada máxima (≤ 600 min) y termina en depósito o relevo.
2. **Varios conductores** si el bloque excede la jornada o termina en nodo no-relevo: cada corte es en un **punto de relevo**; el conductor que sale hace Desplazamiento al depósito y FnS; el que entra hace InS en depósito y Desplazamiento al relevo.

---

## Ejemplo (bus 62)

- **Bloque:** 05:00–22:16 (muchos comerciales y paradas).
- **Corte en PIE ANDINO** a las 12:59 (fin del último evento del primer conductor en el relevo).

### Conductor 1 (sale)

- **InS** en depósito (ej. 04:45–05:00).
- Vacio + comerciales/paradas hasta **12:59** en **PIE ANDINO**.
- **Desplazamiento** PIE ANDINO → Depósito Pie Andino: **12:59–13:00**.
- **FnS** en depósito a las **13:00** (turno ≈ 08:15).

### Conductor 2 (entra)

- **InS** en depósito: **12:59–13:14** (15 min).
- **Desplazamiento** Depósito → PIE ANDINO: **13:14–13:15**.
- Primer evento en el bus: el que **empieza a las 13:15** (no antes).
- Jornada hasta el último evento del bus (ej. 22:16), respetando jornada máxima (600 min).

Paradas/vacíos que ocurran **entre 12:59 y 13:15** (ventana de relevo) **no se asignan al conductor entrante**: su primer evento asignado es el primero que empieza cuando ya puede tomar el bus (≥ 13:15).

---

## Dónde se implementa

| Regla | Dónde |
|-------|--------|
| Cortes solo en depósito o `puntos_relevo` | `fase2_conductores.py`: `_dividir_bloque_en_turnos()` |
| Fin de turno en relevo = último evento + Desplazamiento (relevo→depósito) + FnS en depósito | Mismo: `fin_turno = ultimo_viaje["fin"] + t_desplaz_fin` |
| Inicio del siguiente = InS en depósito + Desplazamiento (depósito→relevo); primer evento ≥ ese instante | Mismo: avance de `idx_inicio` saltando eventos con `inicio < min_inicio_sig` |
| Creación de eventos InS, Desplazamiento, FnS en export | `core/engines/eventos_completos.py` y `io/exporters/excel_writer.py` |

---

## Configuración relevante

- **`puntos_relevo`** en `configuracion.json`: nodos donde se puede hacer relevo (deben tener desplazamiento habilitado al depósito).
- **`limite_jornada`**: 600 min (jornada máxima por conductor).
- **`tiempo_toma`**: 15 min (duración del InS en depósito).
