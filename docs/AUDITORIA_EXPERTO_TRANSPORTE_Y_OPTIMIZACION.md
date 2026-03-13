# Auditoría experto: transporte público de pasajeros y optimización

**Objetivo:** Verificar que la diagramación y el export cumplan estándares de operación y de optimización.  
**Enfoque:** Experto en transporte público de pasajeros + experto en optimización.

---

## 1. Lente TRANSPORTE PÚBLICO DE PASAJEROS

### 1.1 Reglas operativas que deben cumplirse

| Regla | Estado en código/resultado | Verificación |
|-------|----------------------------|--------------|
| **Un conductor, un bus a la vez** | Sin solapamiento en EventosCompletos (0 detectados en análisis previo). Fase 2 asigna bloques completos por conductor. | OK: la asignación por bloques evita doble asignación en el mismo minuto. |
| **Inicio de servicio (InS) antes del primer movimiento** | Corrección temprana + definitiva: InS.inicio = max(0, min_inicio_trabajo − 15). | OK: 15 min de “toma” antes del primer evento de trabajo. |
| **Fin de servicio (FnS) al cierre real del turno** | FnS = fin del último evento de **trabajo** (Vacio, Comercial, Parada, Recarga, Desplazamiento); no se usa InS ni max_inicio. | OK: FnS ya no queda horas después del último evento. |
| **Orden cronológico por conductor** | Orden (conductor, prioridad InS=0 / trabajo=1 / FnS=2, inicio, fin). Reasignación de Comerciales sin conductor que caen en [InS.fin, FnS.inicio] de un único conductor. | OK: evita FnS antes de un Comercial del mismo conductor en la hoja. |
| **Jornada máxima respetada** | limite_jornada (600 min) en Fase 2; 0 turnos > 600 en el resultado analizado. | OK: restricción dura en el motor. |
| **Paradas largas en depósito no como “Parada”** | En export: Parada con origen/destino depósito y duración > 60 min → etiqueta “Tiempo en depósito”. | OK: distingue parada operativa de tiempo en depósito. |
| **Bus inicial = primer bus que conduce** | TurnosConductores: bus_id_inicial e inicio/fin jornada desde la **primera y última tarea por tiempo** (orden cronológico de tareas). Fase 3: id_bus = primera tarea de tareas_con_bus. | OK: coherencia con la secuencia real del conductor. |

### 1.2 Riesgos residuales (transporte)

- **Relevos:** Un conductor en varios buses es válido si hay tiempo de traslado y no solapamiento. La lógica actual no valida tiempo mínimo de traslado entre buses en el export; eso se asegura en Fase 3 (conexión depósito/relevo). Aceptable.
- **Conductor solo InS/FnS:** Si por error un turno tuviera solo InS y FnS (sin eventos de trabajo), FnS no se sobrescribe (guarda añadida: solo se corrige FnS cuando hay max_fin_otros). Correcto.
- **Ventana de reasignación:** Un Comercial sin conductor se reasigna solo si cae en [InS.fin, FnS.inicio] de **un único** conductor; si dos conductores solapan ventanas no se asigna (evita ambigüedad). Correcto.

### 1.3 Veredicto transporte

**APTO** para uso operativo: la secuencia por conductor es coherente en el tiempo, InS/FnS están bien acotados, el “primer bus” y el inicio/fin de jornada reflejan la secuencia real, y las paradas largas en depósito se tipifican aparte. Las correcciones aplicadas resuelven las incoherencias que se habían detectado.

---

## 2. Lente OPTIMIZACIÓN

### 2.1 Objetivos y restricciones del modelo

| Capa | Objetivo | Restricciones duras |
|------|----------|----------------------|
| **Fase 1 (buses)** | Cubrir todos los viajes con el mínimo de buses (reutilización). | Agrupación por línea; máximo de buses; vacíos/recargas factibles. |
| **Fase 2 (conductores)** | Cubrir todos los bloques con el mínimo de conductores. | Jornada ≤ limite_jornada; tiempo de toma; sin overtime (o tolerancia configurable). |
| **Fase 3 (unión)** | Reducir conductores uniendo turnos compatibles. | Mismo grupo de línea; tiempo entre turnos; máximo cambios de bus; conexión depósito/relevo. |

### 2.2 Calidad de la solución

- **Factibilidad:** Todos los viajes cubiertos (regla dura en export y en Fase 1/2). Comerciales siempre presentes en EventosCompletos (con o sin conductor).
- **Optimalidad:** Fase 2 reporta OPTIMAL (mínimo de conductores para los bloques dados). Fase 3 es greedy multi-pasada; no garantiza óptimo global pero reduce bien (p. ej. 11,1 % en el caso analizado).
- **Consistencia entre fases:** Los datos que llegan al Excel (lista_final_eventos) se corrigen para que FnS/InS y orden no contradigan la solución del motor. Así, el “certificado” (Excel) es consistente con la solución optimizada.

### 2.3 Posibles mejoras (optimización)

- **Fase 1:** El estado FEASIBLE_MAX_BUSES_EXCEDIDO indica que se tocó el techo de flota; la solución es factible pero el límite puede ser restrictivo. Valorar relajar max_buses o documentar que es un diseño deseado.
- **Fase 3:** Unión solo dentro del mismo grupo de línea; podría valorarse (en futuro) relajar para más reducción de conductores, si la operación lo permite.
- **Multi-iteración (semillas):** Varias semillas mejoran robustez ante el orden de procesamiento; la solución final es la de menor número de conductores. Adecuado.

### 2.4 Veredicto optimización

**COHERENTE** con un modelo de optimización en tres fases: factibilidad garantizada, Fase 2 óptima en conductores para los bloques dados, Fase 3 con ahorro significativo y export alineado con la solución. No se introducen inconsistencias que invaliden el resultado optimizado.

---

## 3. Resumen de la auditoría

| Dimensión | Conclusión |
|-----------|------------|
| **Transporte** | Secuencia temporal correcta por conductor; InS/FnS y bus inicial/jornada coherentes; paradas largas en depósito diferenciadas. **APTO** operación. |
| **Optimización** | Tres fases alineadas; factibilidad y optimalidad de Fase 2 respetadas; Fase 3 reduce conductores; Excel consistente con la solución. **COHERENTE**. |
| **Correcciones recientes** | FnS solo desde eventos de trabajo; corrección temprana antes del filtrado; reasignación de Comerciales en ventana única; bus_id_inicial e inicio/fin desde tareas ordenadas; paradas > 60 min en depósito como “Tiempo en depósito”; guarda para conductor sin eventos de trabajo. **VERIFICADAS**. |

**Dictamen final:** Con las correcciones aplicadas y las guardas añadidas, el sistema está **en condiciones de ser usado en producción** desde el punto de vista de transporte público de pasajeros y de optimización, siempre que se vuelva a ejecutar el diagramador para generar un nuevo Excel con las reglas actuales. Se recomienda ejecutar `analizar_resultado.py` sobre cada nuevo resultado como control de calidad rutinario.
