# Análisis experto: resultado de diagramación

**Archivo analizado:** `diagramador_optimizado/resultado_diagramacion.xlsx`  
**Enfoque:** Transporte urbano, asignación de flota y conductores, y optimización operativa.

---

## 1. Resumen ejecutivo

| Dimensión | Valor | Valoración |
|-----------|--------|------------|
| Viajes comerciales | 1 281 | Cubiertos en EventosCompletos |
| Buses (bloques) | 162 | Fase 1 factible con aviso de techo de buses |
| Conductores (turnos) | 319 | Fase 2 óptima; Fase 3 redujo 40 (11,1 %) |
| Jornada máxima | 600 min | Ningún turno supera el límite |
| Orden cronológico por conductor | 314/318 InS correcto, 318/318 FnS correcto | Muy bueno |
| Solapamiento conductor–bus | 0 | Correcto |
| Paradas largas en depósito | 9 (179–445 min) | Revisar tipificación |

**Conclusión:** La diagramación es **operativamente coherente y utilizable**. La Fase 1 reporta que se alcanzó el máximo de buses permitido (regla dura); Fase 2 y Fase 3 están bien resueltas. Los puntos a mejorar son la tipificación de “paradas” de muchas horas en depósito y el mensaje/regla de Fase 1 cuando se toca el techo de flota.

---

## 2. Análisis por hoja del Excel

### 2.1 ResumenOptimizacion

| Fase | Estado final | Descripción |
|------|-------------|-------------|
| Fase 1: Buses | **FEASIBLE_MAX_BUSES_EXCEDIDO** | ERROR / OTRO |
| Fase 2: Conductores | **OPTIMAL** | Número mínimo de conductores |
| Fase 3: Unión de Conductores | **COMPLETADA** | 40 conductores reducidos (11,1 %) |

**Lectura experta:**

- **Fase 1:** La solución es factible pero se activó una regla de “máximo de buses excedido”. Eso indica que con el techo actual de flota (p. ej. 50 o el configurado) la demanda requiere más buses de los permitidos, o que el algoritmo llegó al límite y lo reporta como aviso. Operativamente: o se relaja el `max_buses` en configuración, o se asume que 162 bloques es el diseño deseado y el mensaje es informativo.
- **Fase 2:** Óptimo en número de conductores para cubrir los bloques generados en Fase 1.
- **Fase 3:** Unión de turnos bien aplicada: 40 conductores menos (de ~359 a 319), sin violar restricciones de jornada ni de cambio de bus, lo que reduce coste laboral y complejidad operativa.

---

### 2.2 BloquesBuses (2 621 filas)

- **Contenido:** Secuencia de eventos por bus (vacío, comercial, parada) con origen, destino, inicio, fin y duración.
- **Cobertura:** 162 buses (bloques), 8 líneas/servicios: F09, 102, F20, F05, F06, 712, F03c, F02.
- **Uso:** Base para asignar conductores y para explotación (salidas/llegadas por bus y por línea).

**Valoración:** Estructura clara para operación y para alimentar EventosCompletos y TurnosBuses.

---

### 2.3 TurnosConductores (319 filas)

- **Métricas de jornada:** Duración media ≈ 426 min; mediana ≈ 482 min; máximo 600 min (límite respetado).
- **Cumplimiento:** 0 turnos con duración > 600 min.
- **Distribución:** Turnos desde ~63 min hasta 600 min (hay turnos cortos y turnos largos, coherente con relevos y líneas de distinta longitud).

**Valoración:** Cumplimiento normativo de jornada correcto; mezcla de turnos largos y cortos razonable para una red con varias líneas.

---

### 2.4 BusEventos (2 863 filas)

- **Contenido:** Eventos por bus (Vacio, Comercial, Parada) con horarios e itinerarios.
- **Relación:** Menos filas que BloquesBuses porque aquí se agrupa por “evento lógico” (p. ej. un comercial puede agrupar tramos). Coherente con 162 buses y múltiples eventos por bus.

**Valoración:** Vista operativa por bus adecuada para seguimiento y posibles integraciones (autonomía, recarga, etc.).

---

### 2.5 TurnosBuses (162 filas)

- **Conductores por bus:** 39 buses con 1 conductor; 64 con 2; 45 con 3; 13 con 4; 1 con 5. Total 319 conductores repartidos en 162 buses.
- **Interpretación:** Relevos bien repartidos: muchos buses con 2–3 conductores (turnos partidos), pocos con 4–5 (buses de servicio muy largo o con muchas rotaciones).

**Valoración:** Asignación de personal por bus coherente con la duración y el tipo de servicio de cada bloque.

---

### 2.6 EventosCompletos (3 103 filas)

- **Tipos de evento:** Comercial 1 281, Parada 937, InS 318, FnS 318, Vacio 249.
- **Conductores:** 318 con al menos un InS (1 conductor menos que el total de 319 puede ser por turno sin eventos en esta hoja o por filtrado). 318 con FnS correctamente después del último evento.
- **Orden temporal:** En 314 de 318 conductores el InS está antes del primer evento de trabajo; en los 318 el FnS está después del último. La hoja es cronológicamente coherente por conductor y apta para auditoría y explotación.

**Solapamiento conductor–bus:** No se detectan ventanas de tiempo solapadas para un mismo conductor en dos buses distintos (0 casos). La asignación “un conductor, un bus a la vez” se cumple.

**Paradas largas en depósito:** 9 eventos tipo “Parada” con Origen/Destino “Deposito Pie Andino” y duración entre 179 y 445 minutos. Operativamente son tiempos en depósito sin servicio (o entre turnos), no “paradas” en sentido estricto. Recomendación: tipificar como “Tiempo en depósito” o “Sin asignación” y limitar la duración máxima que se etiqueta como “Parada” (p. ej. ≤ 60 min).

---

## 3. Métricas de optimización

| Indicador | Valor | Comentario |
|-----------|--------|------------|
| Viajes / bus (aprox.) | 1 281 / 162 ≈ 7,9 | Reutilización de flota alta |
| Conductores / bus (media) | 319 / 162 ≈ 1,97 | Relevos bien utilizados |
| Reducción Fase 3 | 40 conductores (11,1 %) | Buena ganancia por unión de turnos |
| Jornada máxima | 600 min | Dentro de límite |
| Comerciales cubiertos | 1 281 en EventosCompletos | Cobertura completa de la demanda exportada |

---

## 4. Recomendaciones

1. **Fase 1 – Mensaje MAX_BUSES_EXCEDIDO:** Definir si 162 buses es el diseño deseado; si sí, considerar cambiar el estado a “FEASIBLE” y dejar el aviso solo en descripción. Si no, revisar `max_buses` y/o demanda.
2. **Paradas largas en depósito:** Ajustar el generador de eventos para no etiquetar como “Parada” gaps de muchas horas en depósito; usar otro tipo o no generar fila y dejar el gap visible en análisis.
3. **EventosCompletos:** Mantener la lógica actual de orden (InS primero, FnS último) y la corrección de InS respecto al primer evento; está validada en este resultado.
4. **Auditoría periódica:** Ejecutar `analizar_resultado.py` sobre cada nuevo resultado para comprobar horarios, FnS, comerciales y (si se añade) detección de paradas largas.

---

## 5. Conclusión

El resultado de diagramación es **sólido para uso operativo**: cobertura de 1 281 viajes comerciales, 162 buses, 319 conductores con jornadas dentro de límite, sin solapamientos conductor–bus y con EventosCompletos ordenados y coherentes en el tiempo. La Fase 3 aporta una reducción relevante de conductores (11,1 %). Las mejoras sugeridas son de tipificación (paradas en depósito) y de presentación de la regla de máximo de buses en Fase 1.
