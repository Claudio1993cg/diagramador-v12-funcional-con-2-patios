# Análisis: Eventos duplicados en EventosCompletos y solución

## Problema reportado

1. **Mismo evento de BusEventos aparece varias veces en EventosCompletos** (varios conductores para el mismo evento de bus).
   - Ejemplo: Vacio 02:00–02:30 Depósito → LOS TILOS (bus 111) aparece para conductores 11, 146, 295 y 329.
2. **Cada evento de BusEventos debe asignarse una sola vez a un solo conductor** (1 evento bus = 1 fila EventosCompletos).
3. **No se deben crear eventos nuevos** (viajes/comerciales/vacíos/paradas) que no existan en BusEventos; solo asignar conductor a eventos ya existentes.

---

## Causa raíz

### 1. Clave de unificación incluye origen/destino

En `excel_writer.py`, `_obtener_clave_unificacion` define la clave así:

- **VACIO:** `(tipo, bus_id, ini_redondo, fin_redondo, origen_n, destino_n)`
- **PARADA:** `(tipo, bus_id, ini, fin, origen_n, destino_n)`

Si el mismo evento físico llega con pequeñas diferencias de texto (p. ej. "Deposito Pie Andino" vs "Deposito", o distinta normalización por conductor), se generan **claves distintas** y el mismo slot de bus se trata como varios eventos. Resultado: varias filas en EventosCompletos para el mismo evento de BusEventos.

### 2. Origen de los eventos

- **eventos_conductores** se construye en `core/engines/eventos_completos.py`: por cada evento en `eventos_bus` se asigna **un** conductor y se añade **un** evento (Vacio/Parada/Comercial). No se crean eventos nuevos; solo se asigna conductor.
- En la exportación se hace:
  1. Se rellenan **eventos_unificados** con los eventos de conductores (cada uno con su clave).
  2. Luego se recorren los **eventos de bus** y se fusionan por clave.

Si la clave depende de origen/destino, el mismo evento de bus puede tener distinta clave según quién lo traiga (o cómo esté normalizado) y terminar en varias entradas de `eventos_unificados` → varias filas en EventosCompletos.

### 3. Eventos “inventados”

Cualquier lógica que **añada** eventos (Vacio, Parada, Comercial) que no existan en `eventos_bus` rompe la regla “no crear eventos nuevos”. La regla deseada es: **solo existen en EventosCompletos los eventos que ya están en BusEventos**, más InS/FnS/Desplazamiento que son propios de conductores.

---

## Regla objetivo

- **BusEventos** = fuente única de verdad para Vacio, Parada, Comercial, Recarga.
- **EventosCompletos** = los mismos eventos, cada uno asignado a **un solo** conductor.
- **1 evento (bus, tipo, inicio, fin) → 1 fila en EventosCompletos con 1 conductor.**

---

## Solución (paso a paso)

### Paso 1: Una sola fila por (bus, tipo, inicio, fin)

- **VACIO:** clave de unificación **sin** origen/destino:  
  `(tipo, bus_id, ini_redondo, fin_redondo)`  
  Así, un mismo slot (bus 111, 02:00–02:30) siempre produce la misma clave y una sola fila.
- **PARADA:** clave **sin** origen/destino:  
  `(tipo, bus_id, ini, fin)`  
  Misma idea: una Parada por (bus, inicio, fin).

Con esto se evita que variaciones de texto en origen/destino generen filas duplicadas.

### Paso 2: Comercial ya es 1:1

- Comercial ya usa `(Comercial, viaje_id, bus)` → un viaje comercial = una fila. No se cambia.

### Paso 3: Al unificar, conservar un solo conductor

- Si al rellenar `eventos_unificados` aparece dos veces el mismo (bus, tipo, inicio, fin) con distinto conductor, al usar la nueva clave solo habrá **una** entrada; al sobrescribir, se mantiene un único conductor por evento (el que quede en la última asignación coherente con las reglas de asignación).

### Paso 4: No crear eventos que no estén en BusEventos

- Revisar que **no** se añadan Vacio/Parada/Comercial/Recarga que no existan en `eventos_bus`.
- Solo se pueden “crear” eventos que no son de bus: InS, FnS, Desplazamiento (desde turnos/conductores).

---

## Cambio realizado en código

En `excel_writer.py`, en `_obtener_clave_unificacion`:

- **VACIO:** clave = `(tipo_evento, bus_id, ini_redondo, fin_redondo)` (se quitan `origen_n` y `destino_n`).
- **PARADA:** clave = `(tipo_evento, bus_id, ini, fin)` (se quitan `origen_n` y `destino_n`).

Con esto se garantiza **una fila por (bus, tipo, inicio, fin)** en EventosCompletos y que cada evento de BusEventos se asigne **una sola vez** a **un** conductor.
