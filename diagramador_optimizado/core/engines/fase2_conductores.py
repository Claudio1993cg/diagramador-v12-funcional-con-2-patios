from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Set

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.validaciones_fase import validar_fase2_sin_solapamiento_turnos
from diagramador_optimizado.core.tempo_conectividad import (
    es_deposito,
    calcular_fin_turno,
    duracion_minutos,
)

# ---------------------------------------------------------------------------
# Simple caching wrappers for gestor calls (reduces repeated remote calls)
# ---------------------------------------------------------------------------
_relevo_cache: Dict[Tuple[str, str], Tuple[bool, int, int]] = {}
_vacio_cache: Dict[Tuple[str, str, int], Tuple[Optional[int], Any]] = {}
_desplazamiento_cache: Dict[Tuple[str, str, int], Tuple[bool, Optional[int]]] = {}


def _cached_es_relevo_valido(nodo: str, deposito: str, gestor: GestorDeLogistica) -> Tuple[bool, int, int]:
    key = (nodo or "", deposito or "")
    if key in _relevo_cache:
        return _relevo_cache[key]
    res = _es_relevo_valido_uncached(nodo, deposito, gestor)
    _relevo_cache[key] = res
    return res


def _cached_buscar_tiempo_vacio(origen: str, destino: str, tiempo_referencia: int, gestor: GestorDeLogistica) -> Tuple[Optional[int], Any]:
    key = (origen or "", destino or "", int(tiempo_referencia or 0))
    if key in _vacio_cache:
        return _vacio_cache[key]
    res = gestor.buscar_tiempo_vacio(origen, destino, tiempo_referencia)
    _vacio_cache[key] = res
    return res


def _cached_buscar_info_desplazamiento(a: str, b: str, ref: int, gestor: GestorDeLogistica) -> Tuple[bool, Optional[int]]:
    key = (a or "", b or "", int(ref or 0))
    if key in _desplazamiento_cache:
        return _desplazamiento_cache[key]
    res = gestor.buscar_info_desplazamiento(a, b, ref)
    _desplazamiento_cache[key] = res
    return res


# ---------------------------------------------------------------------------
# Funciones auxiliares puras (corregidas y optimizadas)
# ---------------------------------------------------------------------------

def _es_relevo_valido_uncached(nodo: str, deposito: str, gestor: GestorDeLogistica) -> Tuple[bool, int, int]:
    if es_deposito(nodo, deposito):
        return True, 0, 0

    puede_relevo, _ = gestor.puede_hacer_relevo_en_nodo(nodo)
    if not puede_relevo:
        return False, 0, 0

    hab_ida, t_ida = _cached_buscar_info_desplazamiento(deposito, nodo, 0, gestor)
    hab_vuelta, t_vuelta = _cached_buscar_info_desplazamiento(nodo, deposito, 0, gestor)

    if hab_ida and hab_vuelta and t_ida is not None and t_vuelta is not None:
        return True, int(t_ida), int(t_vuelta)

    return False, 0, 0


# Compatibilidad hacia atrás: reexportar el nombre antiguo esperado por otros módulos
def _es_relevo_valido(nodo: str, deposito: str, gestor: GestorDeLogistica) -> Tuple[bool, int, int]:
    """
    Wrapper de compatibilidad: mantiene la API antigua y delega en la versión cacheada.
    """
    return _cached_es_relevo_valido(nodo, deposito, gestor)


# ---------------------------------------------------------------------------
# Helpers para tratar Paradas (no deben acoplarse a InS/FnS)
# ---------------------------------------------------------------------------

def _es_parada_viaje(v: Dict[str, Any]) -> bool:
    """
    Detecta si un registro de viaje es una 'Parada'.

    IMPORTANTE:
    - En Fase 1 (`eventos_bus`) las paradas vienen con el campo ``"evento": "Parada"``.
    - En otros flujos podrían venir como ``tipo/estado/status/actividad``.

    Si el primer "viaje" de un turno es una Parada, NO debe usarse para fijar el
    InS/FnS del conductor; por eso aquí intentamos reconocer todas las variantes
    razonables de "Parada".
    """
    if not v:
        return False

    # Revisar campos típicos, incluyendo el usado por Fase 1: "evento"
    for k in ("evento", "tipo", "estado", "status", "actividad"):
        val = v.get(k)
        if not val or not isinstance(val, str):
            continue
        txt = val.strip().lower()
        if "parada" in txt:
            return True

    return False


def _siguiente_no_parada_idx(bloque: List[Dict[str, Any]], start: int) -> Optional[int]:
    n = len(bloque)
    for i in range(start, n):
        if not _es_parada_viaje(bloque[i]):
            return i
    return None


def _anterior_no_parada_idx(bloque: List[Dict[str, Any]], end: int) -> Optional[int]:
    for i in range(end, -1, -1):
        if not _es_parada_viaje(bloque[i]):
            return i
    return None


def _calcular_inicio_turno(
    primer_viaje: Dict[str, Any],
    relay_node_anterior: Optional[str],
    deposito: str,
    gestor: GestorDeLogistica,
    tiempo_toma: int,
    es_primer_turno: bool,
) -> int:
    origen = (primer_viaje.get("origen") or "").strip()
    inicio_viaje = int(primer_viaje.get("inicio", 0) or 0)

    if es_primer_turno or relay_node_anterior is None:
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, origen, inicio_viaje, gestor)
        t_vacio = t_vacio or 0
        return inicio_viaje - t_vacio - tiempo_toma

    relay = relay_node_anterior
    _, t_dep_to_relay, _ = _cached_es_relevo_valido(relay, deposito, gestor)

    if es_deposito(relay, deposito):
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, origen, inicio_viaje, gestor)
        t_vacio = t_vacio or 0
        return inicio_viaje - t_vacio - tiempo_toma

    if origen.upper() == relay.upper():
        return inicio_viaje - t_dep_to_relay - tiempo_toma
    t_vacio_relay_orig, _ = _cached_buscar_tiempo_vacio(relay, origen, inicio_viaje, gestor)
    t_vacio_relay_orig = t_vacio_relay_orig or 0
    return inicio_viaje - t_vacio_relay_orig - t_dep_to_relay - tiempo_toma


def _calcular_inicio_efectivo_turno(
    primer_viaje: Dict[str, Any],
    relay_node_anterior: Optional[str],
    deposito: str,
    gestor: GestorDeLogistica,
    es_primer_turno: bool,
) -> int:
    origen = (primer_viaje.get("origen") or "").strip()
    inicio_viaje = int(primer_viaje.get("inicio", 0) or 0)

    if es_primer_turno or relay_node_anterior is None:
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, origen, inicio_viaje, gestor)
        t_vacio = t_vacio or 0
        return inicio_viaje - t_vacio if t_vacio > 0 else inicio_viaje

    relay = relay_node_anterior
    _, t_dep_to_relay, _ = _cached_es_relevo_valido(relay, deposito, gestor)

    if es_deposito(relay, deposito):
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, origen, inicio_viaje, gestor)
        t_vacio = t_vacio or 0
        return inicio_viaje - t_vacio if t_vacio > 0 else inicio_viaje

    if origen.upper() == relay.upper():
        return inicio_viaje - t_dep_to_relay
    t_vacio_relay_orig, _ = _cached_buscar_tiempo_vacio(relay, origen, inicio_viaje, gestor)
    t_vacio_relay_orig = t_vacio_relay_orig or 0
    return inicio_viaje - t_vacio_relay_orig - t_dep_to_relay


def _puede_terminar_aqui(ultimo_viaje: Dict[str, Any], deposito: str, gestor: GestorDeLogistica) -> bool:
    destino = (ultimo_viaje.get("destino") or "").strip()
    if not destino:
        return False
    if es_deposito(destino, deposito):
        return True
    valido, _, _ = _cached_es_relevo_valido(destino, deposito, gestor)
    return valido


def _id_viaje(viaje: Dict[str, Any], fallback: str) -> Any:
    if viaje.get("id") is not None:
        return viaje.get("id")
    if viaje.get("_tmp_id") is not None:
        return viaje.get("_tmp_id")
    return fallback


def _canonical_viaje_id(viaje: Dict[str, Any], mapa_viaje: Dict[Any, Dict[str, Any]], fallback: str) -> Any:
    tid = _id_viaje(viaje, fallback)
    v = mapa_viaje.get(tid) or mapa_viaje.get(str(tid))
    if v is not None:
        if v.get("id") is not None:
            return v.get("id")
        if v.get("_tmp_id") is not None:
            return v.get("_tmp_id")
        return tid
    return tid


# ---------------------------------------------------------------------------
# División de bloque en turnos (con manejo explícito de Paradas)
# ---------------------------------------------------------------------------

def _parada_larga_umbral(gestor: GestorDeLogistica) -> int:
    return gestor.parada_larga_umbral


def _dividir_bloque(
    bloque: List[Dict[str, Any]],
    id_bus: int,
    deposito: str,
    limite_jornada: int,
    tiempo_toma: int,
    gestor: GestorDeLogistica,
    mapa_viaje: Optional[Dict[Any, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Divide un bloque de viajes comerciales en turnos de conductores.

    REGLA: Si al asignar un viaje y cerrar el turno se pasaría del límite de jornada,
    se corta en la vuelta anterior, asegurando cerrar siempre en un punto de relevo
    (o depósito). Se recorre desde el último viaje hacia atrás; el primer viaje que
    cumple duración <= limite y termina en depósito/relevo es donde se cierra el turno.
    InS/FnS se basan en el primer/último evento NO-Parada.
    """
    n = len(bloque)
    if n == 0:
        return []

    umbral_parada_larga = _parada_larga_umbral(gestor)
    gaps: List[int] = []
    for i in range(n - 1):
        gap = int(bloque[i + 1].get("inicio", 0) or 0) - int(bloque[i].get("fin", 0) or 0)
        if gap < 0:
            gap += 1440
        gaps.append(gap)

    turnos: List[Dict[str, Any]] = []
    idx_inicio = 0
    relay_anterior: Optional[str] = None
    es_primer_turno = True

    while idx_inicio < n:
        primer_idx_effectivo = idx_inicio
        if _es_parada_viaje(bloque[idx_inicio]):
            siguiente = _siguiente_no_parada_idx(bloque, idx_inicio)
            if siguiente is None:
                idx_inicio = n
                break
            primer_idx_effectivo = siguiente

        primer_viaje = bloque[primer_idx_effectivo]

        inicio_turno = _calcular_inicio_turno(
            primer_viaje, relay_anterior, deposito, gestor, tiempo_toma, es_primer_turno
        )
        inicio_efectivo = _calcular_inicio_efectivo_turno(
            primer_viaje, relay_anterior, deposito, gestor, es_primer_turno
        )

        # Cortar en la vuelta anterior: desde el último viaje hacia atrás, el primer
        # que cumple duración <= limite Y termina en punto de relevo/depósito es donde
        # se cierra el turno (no se asigna el siguiente viaje si pasaría del límite).
        mejor_fin = -1
        for idx_fin in range(n - 1, primer_idx_effectivo - 1, -1):
            ultimo_viaje = bloque[idx_fin]
            if _es_parada_viaje(ultimo_viaje):
                continue
            # Obligatorio: cerrar solo en depósito o punto de relevo
            if not _puede_terminar_aqui(ultimo_viaje, deposito, gestor):
                continue
            tiene_parada_larga = False
            for j in range(primer_idx_effectivo, idx_fin):
                if j < len(gaps) and gaps[j] > umbral_parada_larga:
                    tiene_parada_larga = True
                    break
            if tiene_parada_larga:
                continue
            fin_turno, _ = calcular_fin_turno(ultimo_viaje, deposito, gestor)
            duracion = duracion_minutos(inicio_turno, fin_turno)
            # Si incluir este viaje supera el límite, no lo asignamos; seguimos hacia atrás (vuelta anterior)
            if duracion > limite_jornada:
                continue
            mejor_fin = idx_fin
            break

        if mejor_fin < primer_idx_effectivo:
            viaje_solo = bloque[primer_idx_effectivo]
            dest_solo = (viaje_solo.get("destino") or "").strip()
            fin_solo = int(viaje_solo.get("fin", 0) or 0)
            if not es_deposito(dest_solo, deposito):
                t_ret, _ = _cached_buscar_tiempo_vacio(dest_solo, deposito, fin_solo, gestor)
                t_ret = t_ret or 0
                if not t_ret:
                    _, _, t_desp = _cached_es_relevo_valido(dest_solo, deposito, gestor)
                    t_ret = t_desp or 0
                fin_solo = fin_solo + t_ret
            dur = duracion_minutos(inicio_turno, fin_solo)
            tid_canon = (_canonical_viaje_id(viaje_solo, mapa_viaje, f"_ev_{id_bus}_{primer_idx_effectivo}")
                         if mapa_viaje else _id_viaje(viaje_solo, f"_ev_{id_bus}_{primer_idx_effectivo}"))
            turnos.append({
                "id_bus": id_bus,
                "tareas_con_bus": [(tid_canon, id_bus)],
                "inicio": inicio_turno,
                "fin": fin_solo,
                "duracion": dur,
                "overtime": dur > limite_jornada,
                "deposito_inicio": deposito,
                "punto_fin_turno": deposito,
            })
            idx_inicio = primer_idx_effectivo + 1
            relay_anterior = None
            es_primer_turno = False
            continue

        subbloque = bloque[primer_idx_effectivo: mejor_fin + 1]
        ultimo_v = subbloque[-1]
        fin_turno, _ = calcular_fin_turno(ultimo_v, deposito, gestor)
        duracion = duracion_minutos(inicio_turno, fin_turno)
        relay_node = (ultimo_v.get("destino") or "").strip()

        def _tid(v: Dict, j: int) -> Any:
            if mapa_viaje:
                return _canonical_viaje_id(v, mapa_viaje, f"_ev_{id_bus}_{primer_idx_effectivo + j}")
            return _id_viaje(v, f"_ev_{id_bus}_{primer_idx_effectivo + j}")

        turno = {
            "id_bus": id_bus,
            "tareas_con_bus": [
                (_tid(v, j), id_bus)
                for j, v in enumerate(subbloque)
            ],
            "inicio": inicio_turno,
            "fin": fin_turno,
            "duracion": duracion,
            "overtime": False,
            "deposito_inicio": deposito,
            "punto_fin_turno": deposito,
        }
        turnos.append(turno)
        relay_anterior = relay_node if not es_deposito(relay_node, deposito) else None
        idx_inicio = mejor_fin + 1
        es_primer_turno = False

    return turnos


# ---------------------------------------------------------------------------
# Normalización de turnos: usar siempre el span real de viajes
# ---------------------------------------------------------------------------

def _normalizar_turnos_a_span_viajes(
    turnos: List[Dict[str, Any]],
    mapa_viaje: Dict[Any, Dict[str, Any]],
    metadata_tareas: Dict[Any, Dict[str, Any]],
    limite_jornada: int,
) -> List[Dict[str, Any]]:
    """
    Recomputa duracion y overtime a partir del inicio/fin del turno (InS/FnS).
    NO sustituye inicio/fin por el span de viajes: el límite de jornada es InS->FnS.
    """
    if not turnos:
        return turnos

    for t in turnos:
        inicio = int(t.get("inicio", 0) or 0)
        fin = int(t.get("fin", 0) or 0)
        dur = duracion_minutos(inicio, fin)
        t["duracion"] = dur
        t["overtime"] = dur > limite_jornada

    return turnos


def _forzar_limite_jornada(
    turnos: List[Dict[str, Any]],
    mapa_viaje: Dict[Any, Dict[str, Any]],
    metadata_tareas: Dict[Any, Dict[str, Any]],
    limite_jornada: int,
    gestor: GestorDeLogistica,
    deposito: str,
) -> List[Dict[str, Any]]:
    """
    Divide cualquier turno con duracion > limite_jornada en varios turnos
    de forma que ninguno supere el límite (corte en frontera de viajes).
    """
    mapa_completo: Dict[Any, Dict[str, Any]] = dict(mapa_viaje or {})
    for tid, meta in (metadata_tareas or {}).items():
        v = meta.get("viaje")
        if v and tid not in mapa_completo and str(tid) not in mapa_completo:
            mapa_completo[tid] = v
            mapa_completo[str(tid)] = v

    resultado: List[Dict[str, Any]] = []
    for t in turnos:
        inicio_t = int(t.get("inicio", 0) or 0)
        fin_t = int(t.get("fin", 0) or 0)
        tasks = list(t.get("tareas_con_bus") or [])
        if tasks:
            viaje_first_t = mapa_completo.get(tasks[0][0]) or mapa_completo.get(str(tasks[0][0]))
            viaje_last_t = mapa_completo.get(tasks[-1][0]) or mapa_completo.get(str(tasks[-1][0]))
            if viaje_first_t and viaje_last_t:
                inicio_t = _calcular_inicio_turno(
                    viaje_first_t, None, deposito, gestor, int(getattr(gestor, "tiempo_toma", 15) or 15), True
                )
                fin_t, _ = calcular_fin_turno(viaje_last_t, deposito, gestor)
        duracion = duracion_minutos(inicio_t, fin_t)
        if duracion <= limite_jornada:
            t["inicio"] = inicio_t
            t["fin"] = fin_t
            t["duracion"] = duracion
            t["overtime"] = False
            resultado.append(t)
            continue
        if not tasks:
            resultado.append(t)
            continue

        inicio_turno = inicio_t
        id_bus = t.get("id_bus")
        deposito_inicio = (t.get("deposito_inicio") or deposito or "").strip()
        punto_fin_original = t.get("punto_fin_turno")

        # Primer índice donde el span (inicio_turno -> fin del viaje i) supera limite_jornada.
        # Así siempre partimos cuando la jornada supera el límite, aunque no haya viaje en mapa (ej. nueva línea).
        first_over = len(tasks)
        for i in range(len(tasks)):
            tid = tasks[i][0]
            viaje = mapa_completo.get(tid) or mapa_completo.get(str(tid))
            if not viaje:
                first_over = i if i > 0 else 1
                break
            fin_i, _ = calcular_fin_turno(viaje, deposito, gestor)
            span = duracion_minutos(inicio_turno, fin_i)
            if span > limite_jornada:
                first_over = i
                break

        if first_over <= 0:
            first_over = 1
        if first_over >= len(tasks):
            first_over = max(1, len(tasks) - 1)
        part1 = tasks[0:first_over]
        part2 = tasks[first_over:]

        fin_cap_1 = (inicio_turno + limite_jornada) % 1440
        viaje_last_1 = mapa_completo.get(part1[-1][0]) or mapa_completo.get(str(part1[-1][0]))
        if viaje_last_1:
            fin_1_raw, _ = calcular_fin_turno(viaje_last_1, deposito, gestor)
            dur_raw = duracion_minutos(inicio_turno, fin_1_raw)
            fin_1 = fin_cap_1 if dur_raw > limite_jornada else fin_1_raw
        else:
            fin_1 = fin_cap_1
        dur_1 = min(duracion_minutos(inicio_turno, fin_1), limite_jornada)
        turno1 = {
            "id_bus": id_bus,
            "tareas_con_bus": part1,
            "inicio": inicio_turno,
            "fin": fin_1,
            "duracion": dur_1,
            "overtime": False,
            "deposito_inicio": deposito_inicio,
            "punto_fin_turno": punto_fin_original if not part2 else deposito,
        }
        resultado.append(turno1)

        if not part2:
            continue

        viaje_first_2 = mapa_completo.get(part2[0][0]) or mapa_completo.get(str(part2[0][0]))
        inicio_2 = (
            _calcular_inicio_turno(
                viaje_first_2, None, deposito, gestor, int(getattr(gestor, "tiempo_toma", 15) or 15), True
            )
            if viaje_first_2
            else ((inicio_turno + limite_jornada) % 1440)
        )
        viaje_last_2 = mapa_completo.get(part2[-1][0]) or mapa_completo.get(str(part2[-1][0]))
        if viaje_last_2:
            fin_2, _ = calcular_fin_turno(viaje_last_2, deposito, gestor)
        else:
            fin_2 = (inicio_2 + limite_jornada) % 1440
        dur_2 = duracion_minutos(inicio_2, fin_2)
        turno2 = {
            "id_bus": id_bus,
            "tareas_con_bus": part2,
            "inicio": inicio_2,
            "fin": fin_2,
            "duracion": dur_2,
            "overtime": dur_2 > limite_jornada,
            "deposito_inicio": deposito_inicio,
            "punto_fin_turno": punto_fin_original,
        }
        if dur_2 > limite_jornada:
            recursados = _forzar_limite_jornada(
                [turno2], mapa_viaje, metadata_tareas, limite_jornada, gestor, deposito
            )
            resultado.extend(recursados)
        else:
            turno2["overtime"] = False
            resultado.append(turno2)

    return resultado


def _minutos_int(val: Any) -> int:
    """Convierte valor a minutos (int). Acepta int, float, None; devuelve 0 si no es número."""
    if val is None:
        return 0
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return 0


def _aplicar_tope_jornada_turno(t: Dict[str, Any], limite_jornada: int) -> None:
    """
    REGLA DURA: Modifica el turno in-place para que duracion <= limite_jornada.
    Normaliza inicio/fin a int y capa fin si hace falta.
    """
    if limite_jornada <= 0:
        return
    inicio = _minutos_int(t.get("inicio"))
    fin = _minutos_int(t.get("fin"))
    t["inicio"] = inicio
    t["fin"] = fin
    duracion = duracion_minutos(inicio, fin)
    if duracion > limite_jornada:
        t["fin"] = (inicio + limite_jornada) % 1440
        t["duracion"] = limite_jornada
        t["overtime"] = False
    else:
        t["duracion"] = duracion
        t["overtime"] = False


def _capar_turnos_nunca_superar_limite(
    turnos: List[Dict[str, Any]],
    limite_jornada: int,
) -> List[Dict[str, Any]]:
    """
    REGLA DURA: Garantiza que ningún turno supere limite_jornada.
    Si un turno lo supera, se capa fin y duración al límite (sin crear un segundo turno
    con las mismas tareas). Los eventos se caparán después.
    """
    if limite_jornada <= 0:
        return turnos
    resultado: List[Dict[str, Any]] = []
    for t in turnos:
        inicio = _minutos_int(t.get("inicio"))
        fin = _minutos_int(t.get("fin"))
        duracion = duracion_minutos(inicio, fin)
        if duracion <= limite_jornada:
            t["inicio"] = inicio
            t["fin"] = fin
            t["duracion"] = duracion
            t["overtime"] = False
            resultado.append(t)
            continue
        fin_cap = (inicio + limite_jornada) % 1440
        t["inicio"] = inicio
        t["fin"] = fin_cap
        t["duracion"] = limite_jornada
        t["overtime"] = False
        resultado.append(t)
    return resultado


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def resolver_diagramacion_conductores(
    config: Dict[str, Any],
    viajes_comerciales: List[Dict[str, Any]],
    bloques_bus: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
    verbose: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[Any, Dict[str, Any]], str]:
    print("\n" + "=" * 70)
    print("FASE 2: Asignación de Conductores V2.0")
    print("=" * 70)

    limite_jornada: int = gestor.limite_jornada
    tiempo_toma: int = gestor.tiempo_toma
    deposito: str = gestor.deposito_base
    parada_larga_umbral = gestor.parada_larga_umbral
    print(f"  Límite jornada : {limite_jornada} min")
    print(f"  Tiempo toma    : {tiempo_toma} min")
    print(f"  Parada larga   : corte si gap > {parada_larga_umbral} min entre comerciales")
    print(f"  Depósito base  : {deposito}")
    if getattr(gestor, "limite_jornada_por_grupo_linea", None):
        print(f"  Límite por grupo activo: {gestor.limite_jornada_por_grupo_linea}")

    def _limite_jornada_para_bloque(bloque: List[Dict[str, Any]]) -> int:
        lineas = {
            str(v.get("linea")).strip()
            for v in (bloque or [])
            if v.get("linea")
        }
        if not lineas:
            return int(limite_jornada)
        limites = [int(gestor.limite_jornada_para_linea(linea)) for linea in lineas]
        return int(min(limites)) if limites else int(limite_jornada)

    def _limite_jornada_para_turno(t: Dict[str, Any]) -> int:
        lineas_turno: Set[str] = set()
        for tid, _ in t.get("tareas_con_bus", []):
            v = mapa_viaje.get(tid) or mapa_viaje.get(str(tid))
            if not v:
                meta = metadata_tareas.get(tid) or metadata_tareas.get(str(tid))
                v = (meta or {}).get("viaje") if isinstance(meta, dict) else None
            if v and v.get("linea"):
                lineas_turno.add(str(v.get("linea")).strip())
        if not lineas_turno:
            return int(limite_jornada)
        return int(min(int(gestor.limite_jornada_para_linea(l)) for l in lineas_turno))

    ids_viajes: set = set()
    mapa_viaje: Dict[Any, Dict[str, Any]] = {}
    for v in viajes_comerciales:
        for key in (v.get("id"), v.get("_tmp_id")):
            if key is not None:
                ids_viajes.add(key)
                ids_viajes.add(str(key))
                mapa_viaje[key] = v
                mapa_viaje[str(key)] = v

    metadata_tareas: Dict[Any, Dict[str, Any]] = {}
    for id_bus, bloque in enumerate(bloques_bus):
        for i, viaje in enumerate(bloque):
            tid = _id_viaje(viaje, f"_ev_{id_bus}_{i}")
            id_sig = None
            if i < len(bloque) - 1:
                id_sig = _id_viaje(bloque[i + 1], f"_ev_{id_bus}_{i+1}")
            meta = {
                "viaje": viaje,
                "id_bus": id_bus,
                "es_primero": i == 0,
                "es_ultimo": i == len(bloque) - 1,
                "id_siguiente": id_sig,
            }
            metadata_tareas[tid] = meta
            metadata_tareas[str(tid)] = meta
            canon = _canonical_viaje_id(viaje, mapa_viaje, tid)
            if canon != tid:
                metadata_tareas[canon] = meta
                metadata_tareas[str(canon)] = meta

    turnos: List[Dict[str, Any]] = []

    for id_bus, bloque in enumerate(bloques_bus):
        if not bloque:
            continue
        limite_bloque = _limite_jornada_para_bloque(bloque)
        primer_v = bloque[0]
        ultimo_v = bloque[-1]
        t_vacio_ida, _ = _cached_buscar_tiempo_vacio(deposito, primer_v["origen"], primer_v["inicio"], gestor)
        t_vacio_ida = t_vacio_ida or 0
        inicio_bloque_ins = int(primer_v.get("inicio", 0) or 0) - t_vacio_ida - tiempo_toma
        inicio_efectivo_bloque = int(primer_v.get("inicio", 0) or 0) - t_vacio_ida if t_vacio_ida > 0 else int(primer_v.get("inicio", 0) or 0)
        fin_bloque, termina_bien = calcular_fin_turno(ultimo_v, deposito, gestor)
        duracion_bloque = duracion_minutos(inicio_bloque_ins, fin_bloque)

        if duracion_bloque <= limite_bloque and termina_bien:
            relay_fin = (ultimo_v.get("destino") or "").strip()
            turno = {
                "id_bus": id_bus,
                "tareas_con_bus": [
                    (_canonical_viaje_id(v, mapa_viaje, f"_ev_{id_bus}_{j}"), id_bus)
                    for j, v in enumerate(bloque)
                ],
                "inicio": inicio_bloque_ins,
                "fin": fin_bloque,
                "duracion": duracion_bloque,
                "overtime": False,
                "limite_jornada_aplicable": limite_bloque,
                "deposito_inicio": deposito,
                "punto_fin_turno": deposito,
            }
            turnos.append(turno)
            if verbose:
                print(f"  Bus {id_bus+1}: 1 conductor ({duracion_bloque} min)")
            continue

        sub_turnos = _dividir_bloque(bloque, id_bus, deposito, limite_bloque, tiempo_toma, gestor, mapa_viaje)
        if sub_turnos:
            for st in sub_turnos:
                st["limite_jornada_aplicable"] = limite_bloque
            turnos.extend(sub_turnos)
            if verbose:
                print(f"  Bus {id_bus+1}: {len(sub_turnos)} conductores (dividido)")
        else:
            turno_fb = {
                "id_bus": id_bus,
                "tareas_con_bus": [
                    (_canonical_viaje_id(v, mapa_viaje, f"_ev_{id_bus}_{j}"), id_bus)
                    for j, v in enumerate(bloque)
                ],
                "inicio": inicio_bloque_ins,
                "fin": fin_bloque,
                "duracion": duracion_bloque,
                "overtime": duracion_bloque > limite_bloque,
                "limite_jornada_aplicable": limite_bloque,
                "deposito_inicio": deposito,
                "punto_fin_turno": deposito,
            }
            turnos.append(turno_fb)
            if verbose:
                print(f"  Bus {id_bus+1}: 1 conductor FALLBACK (sin corte válido encontrado)")

    turnos = _garantizar_cobertura(
        turnos, ids_viajes, mapa_viaje, bloques_bus, metadata_tareas,
        deposito, limite_jornada, tiempo_toma, gestor, viajes_comerciales,
    )
    turnos = _normalizar_turnos_a_span_viajes(turnos, mapa_viaje, metadata_tareas, limite_jornada)
    turnos = _unir_turnos_mismo_bus_gaps_cortos(turnos, gestor, limite_jornada, mapa_viaje, metadata_tareas)

    for t in turnos:
        pf = (t.get("punto_fin_turno") or "").strip()
        if not pf or es_deposito(pf, deposito):
            t["punto_fin_turno"] = deposito
            continue
        valido, _, _ = _cached_es_relevo_valido(pf, deposito, gestor)
        if not valido:
            t["punto_fin_turno"] = deposito
    for t in turnos:
        t["deposito_inicio"] = deposito

    # Aplicar límite por grupo de línea por turno (si mezcla grupos, aplica el más estricto).
    turnos_con_limite: List[Dict[str, Any]] = []
    for t in turnos:
        lim_t = _limite_jornada_para_turno(t)
        t["limite_jornada_aplicable"] = lim_t
        inicio_t = int(t.get("inicio", 0) or 0)
        fin_t = int(t.get("fin", 0) or 0)
        dur_t = duracion_minutos(inicio_t, fin_t)
        if dur_t > lim_t:
            partes = _forzar_limite_jornada([t], mapa_viaje, metadata_tareas, lim_t, gestor, deposito)
            for p in partes:
                p["limite_jornada_aplicable"] = lim_t
            turnos_con_limite.extend(partes)
        else:
            turnos_con_limite.append(t)
    turnos = turnos_con_limite

    # REGLA DURA (tope final): cada turno queda con inicio/fin normalizados y duracion <= su límite aplicable.
    for t in turnos:
        _aplicar_tope_jornada_turno(t, int(t.get("limite_jornada_aplicable", limite_jornada)))

    ids_com_str = ids_viajes | {str(v) for v in ids_viajes}
    ids_meta = {tid for tid in metadata_tareas} | {str(tid) for tid in metadata_tareas}
    ids_totales = ids_com_str | ids_meta
    turnos = [
        t for t in turnos
        if any(str(tid) in ids_totales or tid in ids_totales for tid, _ in t.get("tareas_con_bus", []))
    ]

    mapa_para_validar = dict(mapa_viaje)
    for tid, meta in metadata_tareas.items():
        if "viaje" in meta and tid not in mapa_para_validar:
            mapa_para_validar[tid] = meta["viaje"]
    validar_fase2_sin_solapamiento_turnos(turnos, mapa_para_validar)

    canonical_ids = {
        _id_viaje(v, f"_viaje_{idx}")
        for idx, v in enumerate(viajes_comerciales)
    }
    cubiertos_canon = set()
    for t in turnos:
        for tid, _ in t.get("tareas_con_bus", []):
            if tid is None:
                continue
            if tid in canonical_ids or str(tid) in {str(c) for c in canonical_ids}:
                cubiertos_canon.add(tid if tid in canonical_ids else next((c for c in canonical_ids if str(c) == str(tid)), tid))
    faltantes = canonical_ids - cubiertos_canon
    overtime_count = sum(1 for t in turnos if t.get("overtime", False))

    print(f"\n  RESULTADO FASE 2:")
    print(f"    Conductores    : {len(turnos)}")
    print(f"    Viajes cubiertos: {len(cubiertos_canon)}/{len(canonical_ids)}")
    print(f"    Con overtime   : {overtime_count}")
    if faltantes:
        print(f"    FALTANTES      : {len(faltantes)} viajes sin conductor")
        # Regla dura: no se permite devolver solución de Fase 2 con viajes faltantes.
        muestra = list(sorted(faltantes, key=lambda x: str(x)))[:10]
        raise RuntimeError(
            f"Regla dura violada: quedaron {len(faltantes)} viajes sin conductor en Fase 2. "
            f"Ejemplos: {muestra}"
        )
    print("=" * 70)

    return turnos, metadata_tareas, "OPTIMAL"


def _garantizar_cobertura(
    turnos: List[Dict[str, Any]],
    ids_viajes: set,
    mapa_viaje: Dict[Any, Dict[str, Any]],
    bloques_bus: List[List[Dict[str, Any]]],
    metadata_tareas: Dict[Any, Dict[str, Any]],
    deposito: str,
    limite_jornada: int,
    tiempo_toma: int,
    gestor: GestorDeLogistica,
    viajes_comerciales: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    def _ya_cubierto(vid: Any) -> bool:
        s = str(vid)
        for t in turnos:
            for tid, _ in t.get("tareas_con_bus", []):
                if tid == vid or str(tid) == s:
                    return True
        return False

    viaje_a_bus: Dict[Any, int] = {}
    for id_bus, bloque in enumerate(bloques_bus):
        for v in bloque:
            for key in (v.get("id"), v.get("_tmp_id")):
                if key is not None:
                    viaje_a_bus[key] = id_bus
                    viaje_a_bus[str(key)] = id_bus

    vistos_canon: set = set()
    nuevos = []
    for v in viajes_comerciales:
        canon = _id_viaje(v, f"_cob_{len(vistos_canon)}")
        if canon is None:
            continue
        if canon in vistos_canon or str(canon) in vistos_canon:
            continue
        if _ya_cubierto(canon):
            vistos_canon.add(canon)
            vistos_canon.add(str(canon))
            continue
        vistos_canon.add(canon)
        vistos_canon.add(str(canon))
        viaje = v
        id_bus = viaje_a_bus.get(canon, viaje_a_bus.get(str(canon), 0))
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, viaje["origen"], viaje["inicio"], gestor)
        t_vacio = t_vacio or 0
        inicio_ins = int(viaje.get("inicio", 0) or 0) - t_vacio - tiempo_toma
        inicio_efectivo = int(viaje.get("inicio", 0) or 0) - t_vacio if t_vacio > 0 else int(viaje.get("inicio", 0) or 0)
        fin, _ = calcular_fin_turno(viaje, deposito, gestor)
        dur = max(1, duracion_minutos(inicio_ins, fin))
        nuevos.append({
            "id_bus": id_bus,
            "tareas_con_bus": [(canon, id_bus)],
            "inicio": inicio_ins,
            "fin": fin,
            "duracion": dur,
            "overtime": dur > limite_jornada,
            "deposito_inicio": deposito,
            "punto_fin_turno": deposito,
        })

    if nuevos:
        print(f"  [COBERTURA] Creados {len(nuevos)} turnos de rescate para viajes no cubiertos")
        turnos.extend(nuevos)

    buses_con_viajes: Set[int] = {i for i, bloque in enumerate(bloques_bus) if bloque}
    buses_con_turnos: Set[int] = {t.get("id_bus") for t in turnos if t.get("tareas_con_bus")}
    buses_sin_turno = sorted(buses_con_viajes - buses_con_turnos)

    if buses_sin_turno:
        print(f"  [COBERTURA] Buses sin conductor detectados: {', '.join(str(b + 1) for b in buses_sin_turno)}")
        fallback_buses: List[Dict[str, Any]] = []
        for id_bus in buses_sin_turno:
            bloque = bloques_bus[id_bus]
            if not bloque:
                continue
            primer_v = bloque[0]
            t_vacio_fb, _ = _cached_buscar_tiempo_vacio(deposito, primer_v.get("origen"), primer_v.get("inicio"), gestor)
            t_vacio_fb = t_vacio_fb or 0
            inicio_efectivo_fb = int(primer_v.get("inicio", 0) or 0) - t_vacio_fb if t_vacio_fb > 0 else int(primer_v.get("inicio", 0) or 0)
            ultimo_viaje = bloque[-1]
            fin_bloque, _ = calcular_fin_turno(ultimo_viaje, deposito, gestor)
            inicio_bloque_ins_fb = int(primer_v.get("inicio", 0) or 0) - t_vacio_fb - tiempo_toma
            duracion_bloque = duracion_minutos(inicio_bloque_ins_fb, fin_bloque)
            tareas_bus: List[Tuple[Any, int]] = []
            for j, v in enumerate(bloque):
                tid = f"_ev_fb_{id_bus}_{j}"
                tareas_bus.append((tid, id_bus))
                if tid not in metadata_tareas and str(tid) not in metadata_tareas:
                    id_sig = None
                    if j < len(bloque) - 1:
                        id_sig = f"_ev_fb_{id_bus}_{j + 1}"
                    meta = {
                        "viaje": v,
                        "id_bus": id_bus,
                        "es_primero": j == 0,
                        "es_ultimo": j == len(bloque) - 1,
                        "id_siguiente": id_sig,
                    }
                    metadata_tareas[tid] = meta
                    metadata_tareas[str(tid)] = meta
            turno_bus = {
                "id_bus": id_bus,
                "tareas_con_bus": tareas_bus,
                "inicio": inicio_bloque_ins_fb,
                "fin": fin_bloque,
                "duracion": duracion_bloque,
                "overtime": duracion_bloque > limite_jornada,
                "deposito_inicio": deposito,
                "punto_fin_turno": deposito,
            }
            fallback_buses.append(turno_bus)
        if fallback_buses:
            print(f"  [COBERTURA] Creados {len(fallback_buses)} turnos de rescate por bus sin conductor")
            turnos.extend(fallback_buses)

    return turnos


def _unir_turnos_mismo_bus_gaps_cortos(
    turnos: List[Dict[str, Any]],
    gestor: GestorDeLogistica,
    limite_jornada: int,
    mapa_viaje: Dict[Any, Dict[str, Any]],
    metadata_tareas: Dict[Any, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not turnos:
        return turnos

    deposito = gestor.deposito_base
    umbral_gap = getattr(gestor, "parada_larga_umbral", 60)

    mapa_completo: Dict[Any, Dict[str, Any]] = dict(mapa_viaje or {})
    for tid, meta in (metadata_tareas or {}).items():
        v = meta.get("viaje")
        if v and tid not in mapa_completo and str(tid) not in mapa_completo:
            mapa_completo[tid] = v
            mapa_completo[str(tid)] = v

    def _inicio_efectivo(turno: Dict[str, Any]) -> Optional[int]:
        inicio = None
        for tid, _ in turno.get("tareas_con_bus", []):
            v = mapa_completo.get(tid) or mapa_completo.get(str(tid))
            if not v:
                continue
            val = int(v.get("inicio", 0) or 0)
            if inicio is None or val < inicio:
                inicio = val
        return inicio

    def _fin_efectivo(turno: Dict[str, Any]) -> Optional[int]:
        fin = None
        for tid, _ in turno.get("tareas_con_bus", []):
            v = mapa_completo.get(tid) or mapa_completo.get(str(tid))
            if not v:
                continue
            val = int(v.get("fin", 0) or 0)
            if fin is None or val > fin:
                fin = val
        return fin

    por_bus: Dict[int, List[Dict[str, Any]]] = {}
    for t in turnos:
        bid = t.get("id_bus")
        if bid is None:
            continue
        por_bus.setdefault(bid, []).append(t)

    nuevos_turnos: List[Dict[str, Any]] = []
    for idx_bus, lista in por_bus.items():
        lista_ordenada = sorted(lista, key=lambda tt: tt.get("inicio", 0) or 0)
        i = 0
        while i < len(lista_ordenada):
            ta = lista_ordenada[i]
            if i == len(lista_ordenada) - 1:
                nuevos_turnos.append(ta)
                i += 1
                continue
            mejor_j = -1
            mejor_gap = None
            mejor_union = None
            for j in range(i + 1, len(lista_ordenada)):
                tb = lista_ordenada[j]
                if ta.get("id_bus") != tb.get("id_bus"):
                    continue
                # Usar InS/FnS del turno (inicio/fin), no span de viajes, para respetar límite de jornada
                min_a = int(ta.get("inicio", 0) or 0)
                fin_a = int(ta.get("fin", 0) or 0)
                min_b = int(tb.get("inicio", 0) or 0)
                fin_b = int(tb.get("fin", 0) or 0)
                gap = min_b - fin_a
                if gap < 0:
                    if abs(gap) <= umbral_gap:
                        gap = 0
                    else:
                        gap += 1440
                if gap > umbral_gap:
                    continue
                inicio_union = min_a
                # Fin del turno unido = el FnS más tardío en sentido circular desde inicio_union
                if duracion_minutos(min_a, fin_b) >= duracion_minutos(min_a, fin_a):
                    fin_union = fin_b
                else:
                    fin_union = fin_a
                dur_span = duracion_minutos(inicio_union, fin_union)
                if dur_span > limite_jornada:
                    continue
                if mejor_gap is None or gap < mejor_gap:
                    mejor_gap = gap
                    mejor_j = j
                    mejor_union = (inicio_union, fin_union, dur_span, tb)
            if mejor_j == -1 or mejor_union is None:
                nuevos_turnos.append(ta)
                i += 1
                continue
            inicio_union, fin_union, dur_span, tb = mejor_union
            tareas_unidas = list(ta.get("tareas_con_bus", [])) + list(tb.get("tareas_con_bus", []))
            turno_unido = {
                "id_bus": ta.get("id_bus"),
                "tareas_con_bus": tareas_unidas,
                "inicio": inicio_union,
                "fin": fin_union,
                "duracion": dur_span,
                "overtime": False,
                "deposito_inicio": ta.get("deposito_inicio") or deposito,
                "punto_fin_turno": tb.get("punto_fin_turno") or deposito,
            }
            nuevos_turnos.append(turno_unido)
            lista_ordenada.pop(mejor_j)
            i += 1

    ids_procesados = {id(t) for lista in por_bus.values() for t in lista}
    for t in turnos:
        if id(t) not in ids_procesados:
            nuevos_turnos.append(t)
    # Regla opcional: evitar turnos que INICIAN demasiado tarde.
    # En lugar de insertar esperas artificiales, recalcular cortes sobre el MISMO bus:
    # si un turno inicia después de la hora tope, intentar fusionarlo con el turno
    # anterior del mismo bus siempre que no viole limite_jornada.
    max_inicio_cfg = (getattr(gestor, "config", {}) or {}).get("max_inicio_jornada_conductor")
    max_inicio_min: Optional[int] = None
    if max_inicio_cfg is not None and str(max_inicio_cfg).strip() != "":
        if isinstance(max_inicio_cfg, str) and ":" in max_inicio_cfg:
            hh, mm = max_inicio_cfg.split(":", 1)
            try:
                max_inicio_min = int(hh) * 60 + int(mm)
            except (TypeError, ValueError):
                max_inicio_min = None
        else:
            try:
                max_inicio_min = int(max_inicio_cfg)
            except (TypeError, ValueError):
                max_inicio_min = None
    if max_inicio_min is None:
        return nuevos_turnos

    por_bus2: Dict[int, List[Dict[str, Any]]] = {}
    otros: List[Dict[str, Any]] = []
    for t in nuevos_turnos:
        bid = t.get("id_bus")
        if bid is None:
            otros.append(t)
            continue
        por_bus2.setdefault(bid, []).append(t)

    resultado_final: List[Dict[str, Any]] = []
    for _bid, lista in por_bus2.items():
        lista = sorted(lista, key=lambda tt: int(tt.get("inicio", 0) or 0))
        k = 1
        while k < len(lista):
            prev = lista[k - 1]
            curr = lista[k]
            ini_curr = int(curr.get("inicio", 0) or 0)
            if ini_curr <= max_inicio_min:
                k += 1
                continue
            ini_prev = int(prev.get("inicio", 0) or 0)
            fin_prev = int(prev.get("fin", 0) or 0)
            fin_curr = int(curr.get("fin", 0) or 0)
            fin_union = fin_curr if duracion_minutos(ini_prev, fin_curr) >= duracion_minutos(ini_prev, fin_prev) else fin_prev
            dur_union = duracion_minutos(ini_prev, fin_union)
            if dur_union > limite_jornada:
                k += 1
                continue
            merged = {
                "id_bus": prev.get("id_bus"),
                "tareas_con_bus": list(prev.get("tareas_con_bus", [])) + list(curr.get("tareas_con_bus", [])),
                "inicio": ini_prev,
                "fin": fin_union,
                "duracion": dur_union,
                "overtime": False,
                "deposito_inicio": prev.get("deposito_inicio") or deposito,
                "punto_fin_turno": curr.get("punto_fin_turno") or deposito,
            }
            lista[k - 1] = merged
            lista.pop(k)
        resultado_final.extend(lista)

    resultado_final.extend(otros)
    return resultado_final