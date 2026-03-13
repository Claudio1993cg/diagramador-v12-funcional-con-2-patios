# Evaluación experto transporte: EventosCompletos

## Por qué los resultados “no tienen sentido”

Evaluación de la hoja **EventosCompletos** del export Excel desde el punto de vista operativo de transporte: coherencia temporal, asignación de conductores y consistencia de InS/FnS.

---

## 1. Orden cronológico por conductor

**Problema:** Dentro de un mismo conductor, las filas no siguen el orden temporal. Ejemplo: conductor 55 tiene **InS 06:15–06:30** y justo después un **Vacio 05:29–05:30**. Quien lee la hoja asume una línea de tiempo; ver “06:15” y luego “05:29” rompe esa lógica.

**Causa:** El orden se hacía por (conductor, prioridad InS=0 / otros=1 / FnS=2, inicio, fin). InS quedaba siempre primero por prioridad, pero su *inicio* podía ser **posterior** al inicio del primer evento real (p. ej. por usar `inicio_turno` en lugar del primer evento).

**Corrección aplicada:**  
- En el export se fuerza que **InS.inicio ≤ inicio del primer evento** del conductor (InS = min_inicio_otros − 15 min).  
- Así, al ordenar por (conductor, prioridad, inicio, fin), la fila InS queda primera y además **antes** en el tiempo que el resto, y la secuencia por conductor pasa a ser cronológicamente coherente.

---

## 2. InS después del primer movimiento

**Problema:** En varios casos el **Inicio de Servicio (InS)** tiene hora **posterior** al inicio del primer evento de trabajo (Vacio, Comercial, etc.). Operativamente, el conductor no puede “empezar servicio” después de haber iniciado ya un recorrido.

**Causa:** InS se calculaba a partir del `inicio` del turno o del primer viaje asignado al turno; si el conductor tenía eventos de otro bloque o de otra fuente con *inicio* más temprano, esos no se tenían en cuenta y el InS quedaba “tarde”.

**Corrección:** En la escritura a Excel se aplica la regla:  
`InS.inicio = max(0, min(inicio de todos los eventos del conductor) − 15)`.  
Así InS queda siempre **antes** del primer evento del conductor en la hoja.

---

## 3. Mismo conductor en dos buses a la vez (solapamiento)

**Problema:** Un mismo conductor aparece con eventos en **dos buses distintos** en el mismo tramo horario. Ejemplo: conductor 30 con bus 113 hasta 12:09 y con bus 25 en 09:18–10:25. Eso implica que una misma persona está asignada a dos vehículos a la vez, lo cual es inviable.

**Causa:** Error de **asignación en Fase 2** (construcción de turnos/bloques): un conductor recibe dos bloques que se solapan en tiempo o se le asignan tareas de dos buses en ventanas compatibles sin validar unicidad.

**Recomendación:**  
- Revisar la fase donde se asignan conductores a bloques/buses (diagramación o generación de turnos).  
- Añadir una validación: para cada conductor, comprobar que no existan dos eventos con `inicio`/`fin` solapados en buses distintos.  
- Si se detecta solapamiento, corregir la asignación (un solo bus por conductor en cada instante) o marcar el caso para revisión manual.

---

## 4. Paradas muy largas en depósito

**Problema:** Aparecen eventos tipo **Parada** en “Deposito Pie Andino → Deposito Pie Andino” con duraciones de **179, 325, 438 minutos**, etc. Operativamente no tiene sentido registrar una “parada” de varias horas en depósito; suele ser tiempo sin asignar o tiempo muerto mal etiquetado.

**Causa:** Probablemente se está rellenando con “Parada” todo el tiempo entre el fin de un bloque y el inicio del siguiente para ese conductor/bus, en lugar de modelar explícitamente “sin servicio” o “en depósito sin asignar”.

**Recomendación:**  
- Diferenciar “Parada” (corta, en ruta o terminal) de “Tiempo en depósito sin asignación”.  
- Limitar la duración máxima que se puede etiquetar como Parada (p. ej. 30–60 min); si el gap es mayor, usar otro tipo de evento o no generar fila y dejar el gap visible en análisis.  
- Revisar en el generador de eventos dónde se crean estas paradas largas y ajustar la lógica.

---

## 5. FnS muy posterior al último evento

**Problema:** Casos donde el **último evento** del conductor termina mucho antes que el **FnS** (ej.: último evento ~07:53 y FnS 15:30). Da la impresión de que el conductor “sigue en servicio” horas después de su último movimiento.

**Causa:** FnS se fija con la “corrección definitiva” como `max(max_fin_otros, max_inicio_otros)` por conductor. Si hay eventos duplicados, conductores mezclados o eventos fuera del bloque del conductor, ese máximo puede quedar inflado. También puede deberse a que el `fin` del turno en datos sea muy posterior al último evento real.

**Recomendación:**  
- Asegurar que la corrección de FnS se aplica solo a eventos que realmente pertenecen a ese conductor (misma agrupación que para InS).  
- Opcional: acotar FnS al máximo entre (último fin de evento del conductor, fin de jornada del turno) y no superar un margen (p. ej. 30–60 min) sobre el último fin, salvo que el turno lo justifique.

---

## Resumen de acciones

| # | Problema | Acción |
|---|----------|--------|
| 1 | Orden no cronológico por conductor | **Hecho:** Orden (conductor, prioridad, inicio, fin) + corrección de InS para que sea anterior al primer evento. |
| 2 | InS después del primer evento | **Hecho:** InS.inicio = max(0, min_inicio_otros − 15) en el export. |
| 3 | Conductor en dos buses a la vez | **Pendiente:** Validación en Fase 2 y corrección de asignación. |
| 4 | Paradas de horas en depósito | **Pendiente:** Revisar generación de eventos y tipificación Parada vs tiempo sin asignar. |
| 5 | FnS muy posterior al último evento | **Pendiente:** Revisar agrupación por conductor y opcional acotar FnS. |

Con las correcciones 1 y 2, la hoja EventosCompletos pasa a ser **cronológicamente coherente por conductor** y el InS deja de aparecer después del primer movimiento, lo que mejora la credibilidad operativa del reporte. Los puntos 3–5 requieren cambios en la construcción de turnos y en la generación de eventos (no solo en el export).

---

## Ejecución del diagramador y análisis del resultado

### Datos de la ejecución

- **Entrada:** Se creó un `datos_salidas.xlsx` mínimo en `diagramador_optimizado/` con 4 viajes comerciales (líneas 712 y 801, nodos LOS TILOS, PIE ANDINO, AMENGUAL), usando la `configuracion.json` existente.
- **Comando:** `python -m diagramador_optimizado.main` (salida en `resultado_ejecucion.xlsx`).
- **Fases:** 5 iteraciones (semillas 42, 1042, 2042, 3042, 4042); en todas: Fase 1 → 3 buses, 2 reutilizaciones; Fase 2 → 3 conductores, 4/4 viajes cubiertos; Fase 3 → 0 conductores reducidos.

### Resultado operativo (resumen)

- **Viajes:** 4 comerciales asignados.
- **Buses (bloques):** 3.
- **Conductores (turnos):** 3.
- **Eventos exportados:** Comercial 4, Vacio 6, Parada 2, InS 3, FnS 3 (y un evento RECARGA filtrado por quedar después del FnS).

### Análisis experto: hoja EventosCompletos

1. **Orden cronológico por conductor**  
   Por cada conductor, la secuencia en la hoja es coherente en el tiempo: **InS → Vacio/Comercial/Parada → FnS**.  
   - Conductor 1: InS 04:45–05:00 → Vacio 05:00–06:30 → Comercial 06:30–08:00 → Vacio 08:00–08:01 → FnS 08:01.  
   - Conductor 2: InS 05:15–05:30 → Vacio 05:30–06:00 → Comerciales y paradas en cadena → Vacio 10:15–10:16 → FnS 10:16.  
   Las correcciones de orden e InS se verifican en la salida.

2. **InS antes del primer evento**  
   InS queda siempre 15 minutos antes del primer movimiento (toma de servicio): Conductor 1 primer evento 05:00 → InS 04:45–05:00; Conductor 2 primer evento 05:30 → InS 05:15–05:30. Correcto para operación y normativa.

3. **FnS al cierre del turno**  
   FnS está inmediatamente después del último evento (08:01 y 10:16), sin huecos largos. No aparece FnS muy posterior al último evento en esta ejecución.

4. **Coherencia bus–conductor**  
   En la muestra, cada conductor aparece en un solo bus (1 y 2 respectivamente). No se observan solapamientos conductor–bus en los eventos exportados para esta corrida.

5. **Paradas**  
   Las paradas mostradas son cortas (15 min en PIE ANDINO y LOS TILOS), coherentes con esperas en terminal. No hay paradas de horas en depósito en este resultado.

6. **Optimización**  
   Con 4 viajes y 3 buses hay 2 reutilizaciones; con 3 turnos no hubo reducción en Fase 3 (mismo grupo de línea o restricciones que no permiten unir más). Para esta demanda pequeña el resultado es consistente y utilizable para operación.

### Conclusión de la ejecución

La ejecución del diagramador con datos mínimos **completa correctamente** las tres fases y genera un Excel con EventosCompletos **cronológicamente coherente**: InS primero, FnS último, InS antes del primer movimiento y FnS al cierre. La hoja es apta para uso operativo y auditoría en este escenario. Para escenarios grandes conviene seguir validando solapamientos (punto 3 del resumen) y paradas largas en depósito (punto 4).
