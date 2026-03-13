# Análisis experto: coherencia de conductores asignados

**Archivo:** `diagramador_optimizado/resultado_diagramacion.xlsx`  
**Enfoque:** Transporte y optimización — coherencia de la asignación de conductores a buses, turnos y eventos.

---

## 1. Resumen ejecutivo

El resultado contiene **incoherencias claras** en la asignación y presentación de conductores que explican por qué “no son coherentes” a nivel operativo:

| Hallazgo | Impacto | Cantidad |
|----------|--------|----------|
| **bus_id_inicial** no coincide con buses que conduce el conductor | TurnosConductores miente el “primer bus” | 2 conductores (257, 291) |
| Conductor en **varios buses** (relevos) | Esperado, pero debe ser consistente con TurnosBuses | 38 conductores |
| **TurnosBuses.detalle_conductores** vs **TurnosConductores.bus_id_inicial** | Un conductor figura en un bus pero su “inicial” es otro | 42 casos |
| **Huecos > 60 min** entre eventos consecutivos del mismo conductor | Posibles relevos, FnS mal colocado o eventos desordenados | 78 pares |
| **FnS muy posterior** al último evento real | Conductor “termina” horas después de su último trabajo | Ej.: conductor 106 (07:53 → 15:30) |
| **Orden en EventosCompletos**: FnS antes que Comercial del mismo conductor | Misma persona con FnS y luego Comercial en la hoja | Ej.: conductor 107 |
| **inicio_jornada / fin_jornada** (TurnosConductores) vs primer/último evento real | Resumen del turno no cuadra con la línea de tiempo | 91 inicio, 125 fin |
| **Paradas de horas** en depósito etiquetadas como “Parada” | No son paradas operativas | 9 (179–445 min) |

**Conclusión:** La asignación de conductores es **parcialmente incoherente** entre hojas (TurnosConductores, TurnosBuses, EventosCompletos) y dentro de EventosCompletos (orden, FnS tardío, huecos grandes). Corregir requiere alinear definiciones de “bus inicial”, ordenar siempre por (conductor, prioridad, inicio) al escribir EventosCompletos, fijar FnS al cierre real del turno y unificar criterios de inicio/fin de jornada.

---

## 2. Incoherencias detectadas (detalle)

### 2.1 bus_id_inicial que no aparece en los eventos del conductor

En **TurnosConductores** cada fila tiene `bus_id_inicial`. En **EventosCompletos** se ve en qué buses tiene eventos cada conductor.

- **Conductor 257:** `bus_id_inicial = 1`, pero en EventosCompletos solo tiene eventos en buses **47, 93, 98**. Nunca conduce el bus 1.
- **Conductor 291:** `bus_id_inicial = 1`, pero en EventosCompletos solo tiene eventos en buses **6, 10, 31**.

**Implicación:** El “primer bus” del turno no es el que realmente conduce primero según EventosCompletos. Rompe cualquier reporte o cuadro que use “bus inicial” como referencia.

**Recomendación:** Calcular `bus_id_inicial` a partir del **primer evento con bus** del conductor en la secuencia temporal (por ejemplo desde EventosCompletos o desde la misma fuente que genera la secuencia), no desde otra estructura que pueda estar desincronizada.

---

### 2.2 Conductores en más de un bus (relevos)

**38 conductores** tienen eventos en **más de un bus**. Ejemplos:

- Conductor 30: bus 14 (04:45–08:55), luego bus 25 (09:18–10:25).
- Conductor 41: bus 46 (05:09–07:38), luego bus 53 (08:00–10:11).
- Conductor 60: bus 75 (05:38–08:00), luego bus 81 (08:32–10:51).

En estos casos la **secuencia temporal** (cambio de bus con tiempo intermedio) es razonable. El problema no es tener dos buses, sino:

- Que **TurnosConductores.cambios_bus** y la lista de buses en EventosCompletos coincidan (hoy sí: los 38 tienen cambios_bus ≥ 1 o están bien reflejados).
- Que **TurnosBuses.detalle_conductores** y **TurnosConductores.bus_id_inicial** no se contradigan (véase 2.3).

---

### 2.3 TurnosBuses.detalle_conductores vs TurnosConductores.bus_id_inicial

En **TurnosBuses** cada bus tiene `detalle_conductores` (ej.: "33, 164, 223"). En **TurnosConductores** cada conductor tiene `bus_id_inicial`.

Hay **42 casos** en que un conductor aparece en el detalle del **bus X** pero su `bus_id_inicial` es **Y ≠ X**. Ejemplos:

- Conductor 223: está en bus 1, `bus_id_inicial` = 10.
- Conductor 205: está en bus 5, `bus_id_inicial` = 15.
- Conductor 291: está en bus 6, `bus_id_inicial` = 1.

Si el conductor hace relevo (primero bus 10, luego bus 1), es coherente que esté en el detalle del bus 1 y que `bus_id_inicial = 10`. En esos casos la incoherencia es solo **semántica/nombres**: “bus inicial” está bien, pero quien lee “detalle_conductores del bus 1” puede esperar que 223 tenga `bus_id_inicial = 1`. Para conductores que **no** hacen relevo en ese bus (como 257 y 291, que nunca conducen el bus 1), la incoherencia es **numérica**: el dato de “primer bus” es falso.

**Recomendación:** Unificar la definición de “bus inicial” (primer bus por tiempo en EventosCompletos) y, si se mantiene “detalle_conductores” por bus, documentar que puede incluir conductores cuyo `bus_id_inicial` es otro bus (relevo).

---

### 2.4 Huecos grandes entre eventos consecutivos (mismo conductor)

Hay **78 pares** de eventos consecutivos del mismo conductor con **hueco > 60 minutos** entre el fin de uno y el inicio del siguiente. Ejemplos:

- Conductor 9: 122 min entre dos eventos del **mismo bus (109)** — posible parada larga o error de secuencia.
- Conductor 13: 95 min entre evento y siguiente (uno con Bus=nan: InS/FnS).
- Conductor 106: **457 min** (7,6 h) entre último evento con bus (23) y evento con Bus=nan (FnS).
- Conductor 107: 240 min y 95 min entre eventos (aparece FnS en medio; ver 2.6).

Parte de los huecos son **InS/FnS** (Bus=nan) en medio de la secuencia o FnS muy tarde, lo que indica:

- FnS mal posicionado en el tiempo (2.5),
- o orden de filas incorrecto en EventosCompletos (2.6).

**Recomendación:** Revisar generación de InS/FnS y orden final al exportar para que no queden huecos artificiales ni FnS “flotando” horas después del último evento.

---

### 2.5 FnS muy posterior al último evento real

**Conductor 106:**

- InS 06:32–06:47.
- Único bloque de trabajo: Vacio + Comercial + Parada + Vacio en bus 23, **06:46–07:53**.
- FnS **15:30–15:30**.

El turno real termina a las **07:53** y el FnS figura a las **15:30** (7 h 37 min después). Operativamente el conductor no puede “cerrar servicio” a las 15:30 si su último evento es a las 07:53.

**Causa probable:** Cálculo de FnS usando un máximo global (p. ej. de otro conductor o de otro turno) o de `fin_jornada` no acotado al último evento de ese conductor.

**Recomendación:** Fijar FnS por conductor como **fin del último evento asignado a ese conductor** (con margen opcional de pocos minutos), y no usar un máximo ajeno al turno.

---

### 2.6 Orden en EventosCompletos: FnS antes de un Comercial del mismo conductor

**Conductor 107** en la hoja EventosCompletos (por orden de fila):

1. InS 03:00–03:15  
2. **FnS 11:03–11:03**  
3. Comercial bus 112, **07:15–09:28**

Cronológicamente el Comercial (07:15) es **anterior** al FnS (11:03). En la hoja aparece **FnS y luego Comercial**, por lo que quien lee por filas ve que el conductor “termina” y después “hace un viaje”, lo cual es incoherente.

**Causa probable:** Al construir la lista para EventosCompletos se mezclan dos bloques (p. ej. `eventos_filtrados_post_fns` y `eventos_sin_conductor` o listas por bus). El Comercial de 107 queda en otro bloque y se escribe después, aunque por tiempo debería ir antes del FnS. O el orden final por (conductor, prioridad, inicio, fin) no se aplica a toda la lista unificada antes de escribir.

**Recomendación:** Asegurar que **todos** los eventos que van a EventosCompletos (incluidos los que puedan venir de “sin conductor” o de otra fuente) se unifiquen en **una sola lista**, se ordenen por (conductor, prioridad InS=0 / otros=1 / FnS=2, inicio, fin) y **solo entonces** se escriban filas. No escribir por bloques separados que rompan el orden cronológico por conductor.

---

### 2.7 inicio_jornada y fin_jornada vs primer y último evento real

- **91 conductores:** `inicio_jornada` en TurnosConductores difiere en **más de 20 min** del inicio del primer evento real (Vacio/Comercial/Parada) en EventosCompletos.
- **125 conductores:** `fin_jornada` difiere en **más de 20 min** del fin del último evento real.

Ejemplos:

- Conductor 2: primer evento 06:00, `inicio_jornada` 00:14 (46 min antes).
- Conductor 1: último evento 09:18, `fin_jornada` 09:40 (22 min después).

Diferencias pequeñas (15–20 min) pueden ser InS/tioma de servicio. Diferencias de 40–70 min indican que **inicio_jornada** o **fin_jornada** no se están calculando desde la misma línea de tiempo que EventosCompletos (p. ej. se usan horarios de “bloque” o de “turno” que no coinciden con los eventos exportados).

**Recomendación:** Calcular **inicio_jornada** y **fin_jornada** a partir de la misma fuente que genera la secuencia por conductor (primer inicio y último fin de eventos no InS/FnS, o incluyendo InS/FnS de forma explícita), para que TurnosConductores y EventosCompletos cuenten la misma historia.

---

### 2.8 Paradas de muchas horas en depósito

**9 eventos** tipo “Parada” con Origen/Destino “Deposito Pie Andino” y duración entre **179 y 445 minutos**. No son paradas operativas; son tiempos en depósito sin servicio (o entre turnos).

**Recomendación:** No etiquetar como “Parada” gaps de más de 30–60 min; usar otro tipo (p. ej. “Tiempo en depósito” o “Sin asignación”) o no generar fila y dejar el gap visible en análisis.

---

## 3. Resumen de causas y acciones

| Causa probable | Dónde actuar | Acción |
|----------------|-------------|--------|
| `bus_id_inicial` desde estructura distinta a la secuencia real | Fase 2 / export / TurnosConductores | Calcular “primer bus” desde el primer evento con bus del conductor (orden temporal). |
| FnS = máximo global o de otro turno | Export (corrección FnS) | FnS por conductor = fin del último evento de **ese** conductor (+ margen opcional). |
| Orden EventosCompletos roto por escribir dos listas | Export (ensamblado EventosCompletos) | Una sola lista, ordenar por (conductor, prioridad, inicio, fin), luego escribir. |
| inicio_jornada / fin_jornada desde otro origen | Fase 2 / construcción de turnos | Unificar con la secuencia que alimenta EventosCompletos. |
| Paradas de horas en depósito | Generación de eventos / tipificación | Limitar duración de “Parada” o usar otro tipo para gaps largos en depósito. |
| Detalle conductores por bus vs bus_id_inicial | Documentación / cálculo | Documentar relevos; alinear bus_id_inicial con primer bus real. |

---

## 4. Conclusión

Los conductores asignados **no son coherentes** en su presentación y en varios datos clave: bus inicial falso en 2 casos, 42 desajustes entre “detalle por bus” y “bus inicial”, 78 huecos grandes entre eventos, FnS muy posterior al último evento (ej. conductor 106), orden incorrecto en EventosCompletos (FnS antes de Comercial, ej. conductor 107) y desfases entre inicio/fin de jornada y la línea de tiempo real en 91 y 125 conductores respectivamente.

Para que el resultado sea fiable a nivel operativo y de optimización hace falta:

1. **Unificar la fuente de verdad** para la secuencia por conductor (una sola lista ordenada por tiempo).  
2. **Derivar de esa secuencia** bus_id_inicial, inicio_jornada, fin_jornada y FnS.  
3. **Escribir EventosCompletos** en un único paso, después de un solo orden (conductor, prioridad, inicio, fin).  
4. **Ajustar tipificación** de paradas largas en depósito.

Con estos cambios, la asignación de conductores pasará a ser coherente entre hojas y en el tiempo.
