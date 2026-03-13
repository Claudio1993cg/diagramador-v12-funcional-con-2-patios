"""
FASE 3 V2.0 - Unión de Conductores (modelo de grafos)
======================================================
Objetivo: reducir el número total de conductores uniendo turnos compatibles.

La fase es ITERATIVA: se repiten rondas hasta que ya no se pueda juntar ningún par
más (en cada ronda los turnos ya unidos pueden volver a unirse con otros).

ALGORITMO DE GRAFOS (conductor):
  - Grafo G = (V, E): V = nodos (depósito canónico + paradas), E = aristas solo
    DESPLAZAMIENTO habilitado (tiempo en minutos). Depósito y alias (ej. JUANITA)
    se normalizan al mismo nodo canónico para evitar falsas inconsistencias.
  - Unir turno A con turno B solo si en G:
    1. canon(nodo_fin_A) == canon(nodo_inicio_B) (mismo nodo, tiempo 0), O
    2. Existe arista (canon(nodo_fin_A), canon(nodo_inicio_B)) y
       descanso >= tiempo_arista.
  - Así se garantiza que toda unión permitida tiene un camino válido en G y
    el encadenamiento posterior (eventos completos) tendrá 0 teletransportaciones.

REGLAS adicionales:
  - Unión SOLO a través del depósito: el conductor debe ir nodo_fin_A -> depósito -> nodo_inicio_B
    (mismos desplazamientos habilitados; evita teletransportaciones y desconexiones).
  - Solo mismo grupo de líneas; descanso mínimo; duración jornada ≤ limite.
  - Sin solapamiento temporal en el mismo bus.
  - OR-Tools (CP-SAT) para máximo emparejamiento por ronda cuando está disponible.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple  # Tuple for rechazos_descanso_corto

try:
    from ortools.sat.python import cp_model
    _ORTOOLS_DISPONIBLE = True
except ImportError:
    _ORTOOLS_DISPONIBLE = False

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.tempo_conectividad import (
    es_deposito,
    obtener_tiempo_traslado,
    duracion_minutos,
)
from diagramador_optimizado.core.validaciones_fase import validar_fase3_sin_solapamiento_turnos
from diagramador_optimizado.core.engines.fase2_conductores import (
    _cached_es_relevo_valido as _es_relevo_valido,
)


# ---------------------------------------------------------------------------
# Compatibilidad entre turnos
# ---------------------------------------------------------------------------

def _mismo_grupo_lineas(turno_a: Dict, turno_b: Dict, mapa_viajes: Dict, gestor: GestorDeLogistica) -> bool:
    """True si los turnos operan en el mismo grupo de líneas o pueden interlinear."""
    def _lineas(t: Dict) -> Set[str]:
        lineas: Set[str] = set()
        for tid, _ in t.get("tareas_con_bus", []):
            v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
            if v and v.get("linea"):
                lineas.add(str(v["linea"]).strip().upper())
        return lineas

    la = _lineas(turno_a)
    lb = _lineas(turno_b)
    if not la or not lb:
        return True  # Sin línea definida: permitir unión

    for li in la:
        for lj in lb:
            if li == lj or gestor.pueden_interlinear(li, lj):
                return True
    return False


def _buses_del_turno(turno: Dict) -> Set[int]:
    return {bus for _, bus in turno.get("tareas_con_bus", [])}


def _nodo_inicio_turno(tb: Dict, mapa_viajes: Dict, deposito: str) -> str:
    """Devuelve el nodo REAL donde el conductor debe estar para iniciar el turno B.

    Regla:
    - Si el primer viaje del turno (en tareas_con_bus) tiene origen explícito,
      se usa SIEMPRE ese origen como nodo de inicio.
      Ejemplo: si el primer comercial es TORCON->JUANITA, nodo_inicio = TORCON.
    - Solo si no se encuentra ningún viaje con origen definido se usa
      deposito_inicio como fallback.

    Esto evita casos ilógicos donde el conductor termina en un nodo (p.ej.
    depósito) y “aparece” en otro nodo distinto a la misma hora sin un
    vacío/desplazamiento explícito con ese conductor.
    """
    for tid, _ in tb.get("tareas_con_bus", []):
        v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
        if not v:
            continue
        origen = str(v.get("origen") or "").strip()
        if origen:
            return origen
    return (tb.get("deposito_inicio") or deposito).strip()


def _nodo_fin_turno(ta: Dict, deposito: str) -> str:
    """Devuelve el nodo donde está el conductor al final del turno A."""
    return (ta.get("punto_fin_turno") or deposito).strip()


def _tiempo_arista_grafo(
    gestor: GestorDeLogistica,
    origen: str,
    destino: str,
    tiempo_ref: int,
) -> Optional[int]:
    """
    Conectividad del conductor: solo DESPLAZAMIENTO habilitado (no vacío de bus).
    Usa nodo canónico (depósito y alias, ej. JUANITA = Deposito Juanita) para
    evitar rechazar uniones válidas. Sin arista habilitada retorna None.
    """
    if not origen or not destino:
        return 0
    o_canon = (gestor.nodo_canonico_para_conectividad(origen) or "").strip().upper()
    d_canon = (gestor.nodo_canonico_para_conectividad(destino) or "").strip().upper()
    if o_canon and d_canon and o_canon == d_canon:
        return 0
    hab_d, t_d = gestor.buscar_info_desplazamiento(origen, destino, tiempo_ref)
    if hab_d and t_d is not None and t_d > 0:
        return int(t_d)
    deposito = gestor.deposito_base or ""
    if es_deposito(origen, deposito) and es_deposito(destino, deposito):
        return 0
    return None


def _inicio_efectivo_turno(tb: Dict, mapa_viajes: Dict) -> Optional[int]:
    """Hora (minutos) en que empieza el primer evento real del turno B (no el InS).

    Para unir turnos no se considera el InS, sino el siguiente evento después del InS:
    vacío, desplazamiento, comercial, parada, lo que sea. En nuestros datos el turno
    solo tiene comerciales en tareas_con_bus, así que usamos el inicio del primer
    evento de esa lista (primer comercial). No se usa nunca tb['inicio'] (InS).
    Si no hay eventos, devuelve None (no se puede definir inicio efectivo).
    """
    primer_inicio = None
    for tid, _ in tb.get("tareas_con_bus", []):
        v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
        if v and v.get("inicio") is not None:
            ini = int(v["inicio"])
            if primer_inicio is None or ini < primer_inicio:
                primer_inicio = ini
    return primer_inicio  # None si no hay eventos; nunca fallback a InS


def _fin_efectivo_turno(ta: Dict, mapa_viajes: Dict) -> Optional[int]:
    """Hora (minutos) en que termina el último evento real del turno A (no el FinS).

    Para unir turnos no se considera el fin de servicio (FinS) ni ta['fin'] (puede
    incluir vacío a depot), sino el fin del último evento: comercial, vacío,
    desplazamiento, parada, lo que sea. En nuestros datos usamos el fin del último
    comercial en tareas_con_bus. Si no hay eventos, devuelve None.
    """
    ultimo_fin = None
    for tid, _ in ta.get("tareas_con_bus", []):
        v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
        if v and v.get("fin") is not None:
            f = int(v["fin"])
            if ultimo_fin is None or f > ultimo_fin:
                ultimo_fin = f
    return ultimo_fin  # None si no hay eventos; nunca fallback a ta['fin']/FinS


def _fin_real_turno(
    ta: Dict,
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    deposito: str,
) -> Optional[int]:
    """
    Hora (minutos) en que el conductor está realmente en punto_fin_turno.
    Si el último viaje termina en un nodo distinto al punto_fin_turno (ej. depósito),
    se suma el tiempo de la arista habilitada (vacío/desplazamiento) hasta ese punto.
    Así evitamos uniones imposibles: ej. último comercial 23:22 en LIBANO, vacío a
    depósito 24:00 → fin_real = 1440, no 1402 (que daría descanso 38 min falso).
    """
    fin_efectivo = _fin_efectivo_turno(ta, mapa_viajes)
    if fin_efectivo is None:
        return int(ta.get("fin", 0)) if ta.get("fin") is not None else None
    punto_fin = _nodo_fin_turno(ta, deposito)
    tareas = ta.get("tareas_con_bus", [])
    if not tareas:
        return int(ta.get("fin", fin_efectivo)) if ta.get("fin") is not None else fin_efectivo
    ult_tid = tareas[-1][0]
    v = mapa_viajes.get(ult_tid) or mapa_viajes.get(str(ult_tid))
    if not v:
        return int(ta.get("fin", fin_efectivo)) if ta.get("fin") is not None else fin_efectivo
    ultimo_destino = (v.get("destino") or "").strip()
    if not ultimo_destino:
        return fin_efectivo
    if es_deposito(ultimo_destino, deposito):
        return fin_efectivo
    if (punto_fin or "").strip().upper() == (ultimo_destino or "").strip().upper():
        return fin_efectivo
    # El conductor termina el último viaje en ultimo_destino; punto_fin es otro (ej. depósito).
    # Tiempo hasta llegar a punto_fin por arista habilitada.
    tiempo_a_punto = _tiempo_arista_grafo(gestor, ultimo_destino, punto_fin, fin_efectivo)
    if tiempo_a_punto is None:
        return fin_efectivo
    return fin_efectivo + tiempo_a_punto


def _duracion_operativa_desde_tareas(
    tareas_con_bus: List[Tuple[Any, Any]],
    mapa_viajes: Dict[Any, Dict[str, Any]],
    gestor: GestorDeLogistica,
    deposito: str,
    incluir_toma: bool,
) -> Optional[int]:
    """
    Duración estricta del turno considerando:
    - traslado depósito -> primer origen
    - traslado último destino -> depósito
    - tiempo de toma (solo cuando incluir_toma=True)
    """
    if not tareas_con_bus:
        return 0
    viajes = []
    for tid, _ in tareas_con_bus:
        v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
        if not v:
            return None
        viajes.append(v)
    if not viajes:
        return 0

    primero = viajes[0]
    ultimo = viajes[-1]
    ini = int(primero.get("inicio", 0) or 0)
    fin = int(ultimo.get("fin", 0) or 0)

    if not es_deposito(primero.get("origen", ""), deposito):
        ini -= int(obtener_tiempo_traslado(deposito, primero.get("origen", ""), int(primero.get("inicio", 0) or 0), gestor) or 0)
    if not es_deposito(ultimo.get("destino", ""), deposito):
        fin += int(obtener_tiempo_traslado(ultimo.get("destino", ""), deposito, int(ultimo.get("fin", 0) or 0), gestor) or 0)

    if incluir_toma:
        ini -= max(0, int(getattr(gestor, "tiempo_toma", 0) or 0))
    return duracion_minutos(ini, fin)


def _pueden_unirse(
    ta: Dict,
    tb: Dict,
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    limite_jornada: int,
    descanso_min: int,
    max_cambios_bus: int,
    parada_larga_umbral_union: Optional[int] = None,
    parada_larga_excepcion_depot_min: Optional[int] = None,
    union_solo_por_deposito: bool = True,
    restringir_mismo_grupo: bool = True,
    razon_rechazo: Optional[List[str]] = None,
    rechazos_descanso_corto: Optional[List[Tuple[int, str]]] = None,
) -> bool:
    """
    Evalúa si el conductor que hizo turno A puede hacer turno B después.

    Condiciones:
      - Descanso suficiente entre A.fin y B.inicio
      - Duración total ≤ limite_jornada
      - Mismo grupo de líneas si hay cambio de bus
      - max_cambios_bus por defecto muy alto (999) para no bloquear por cantidad de buses
      - CONECTIVIDAD: solo lo configurado y habilitado. Se prueba en orden:
        1) Desplazamiento habilitado (config desplazamientos, habilitado=true)
        2) Vacío habilitado (config vacíos, habilitado=true)
        Si no hay ninguno entre nodo_fin_A y nodo_inicio_B, no se une (no se inventan rutas).
      La unión considera turnos con comerciales y con vacíos; la conexión entre turnos
      solo usa desplazamiento o vacío habilitados.
    Si razon_rechazo no es None y se rechaza la unión, se añade un string con el motivo.
    """
    def _rechazo(msg: str, descanso_minutos: Optional[int] = None) -> bool:
        if razon_rechazo is not None:
            razon_rechazo.append(msg)
        if rechazos_descanso_corto is not None and descanso_minutos is not None and 8 <= descanso_minutos <= 25:
            rechazos_descanso_corto.append((descanso_minutos, msg))
        return False

    limite_jornada = int(
        min(
            int(ta.get("limite_jornada_aplicable", limite_jornada) or limite_jornada),
            int(tb.get("limite_jornada_aplicable", limite_jornada) or limite_jornada),
        )
    )

    # REGLA: Fase 3 une solo dentro del mismo grupo de línea. Rechazar de inmediato si no.
    mismo_grupo = _mismo_grupo_lineas(ta, tb, mapa_viajes, gestor)
    if restringir_mismo_grupo and not mismo_grupo:
        return _rechazo("Fase 3: solo se unen turnos del mismo grupo de línea")

    fin_efectivo_a = _fin_efectivo_turno(ta, mapa_viajes)
    inicio_efectivo_b = _inicio_efectivo_turno(tb, mapa_viajes)
    inicio_efectivo_a = _inicio_efectivo_turno(ta, mapa_viajes)
    # No considerar InS/FinS: solo eventos reales (primer/último comercial u otro si existiera)
    if fin_efectivo_a is None or inicio_efectivo_b is None or inicio_efectivo_a is None:
        return _rechazo("turno sin eventos para definir inicio/fin efectivo (no se usa InS/FinS)")
    # IMPORTANTE: NO bloquear por solape de límites de jornada (InS/FnS).
    # Para unión operativa consideramos solo eventos efectivos y conectividad real.
    # El primer evento real de B debe ser después del último de A (encadenamiento consistente)
    if inicio_efectivo_b <= fin_efectivo_a:
        return _rechazo(
            f"viajes solapados: primer evento B ({inicio_efectivo_b}) <= último evento A ({fin_efectivo_a})",
            inicio_efectivo_b - fin_efectivo_a if fin_efectivo_a is not None else None,
        )

    # Fin REAL del turno A: cuando el conductor está efectivamente en punto_fin_turno.
    # Si el último viaje termina en otro nodo (ej. LIBANO), se suma el tiempo de arista
    # hasta punto_fin (ej. depósito), para no permitir uniones con descanso falso
    # (ej. fin comercial 23:22, vacío a depósito 24:00 → fin_real=1440, no 1402).
    deposito = gestor.deposito_base
    fin_real_a = _fin_real_turno(ta, mapa_viajes, gestor, deposito)
    if fin_real_a is None:
        fin_real_a = int(ta.get("fin", fin_efectivo_a) or fin_efectivo_a)
    descanso = inicio_efectivo_b - fin_real_a
    if descanso < 0:
        descanso += 1440
    if descanso < descanso_min:
        return _rechazo(f"descanso {descanso} min < mínimo {descanso_min} min", descanso)

    # No unir cuando entre A y B hay una parada larga (ej. parada en depósito 10:00–16:00).
    # Excepción: si A termina en depósito, no rechazar por parada larga cuando el descanso
    # es ≤ excepcion_depot_min (por defecto 120 min), para permitir más encadenamientos en depósito.
    deposito = gestor.deposito_base
    nodo_fin_a_early = _nodo_fin_turno(ta, deposito)
    nodo_inicio_b_early = _nodo_inicio_turno(tb, mapa_viajes, deposito)
    conexion_por_depot = nodo_fin_a_early and es_deposito(nodo_fin_a_early, deposito)
    parada_larga_umbral = (
        parada_larga_umbral_union
        if parada_larga_umbral_union is not None
        else getattr(gestor, "parada_larga_umbral", 60)
    )
    excepcion_depot = 120  # permitir hasta 120 min en depósito sin rechazar por parada larga
    if parada_larga_excepcion_depot_min is not None:
        excepcion_depot = int(parada_larga_excepcion_depot_min)
    # Parada larga debe medir descanso "inactivo". Si parte del gap se usa en
    # desplazamiento/vacío necesario para enlazar A->B, se descuenta ese tiempo
    # de conexión (sin cambiar el umbral de parada larga).
    descanso_inactivo = descanso
    if nodo_fin_a_early and nodo_inicio_b_early:
        o_canon = (gestor.nodo_canonico_para_conectividad(nodo_fin_a_early) or "").strip().upper()
        d_canon = (gestor.nodo_canonico_para_conectividad(nodo_inicio_b_early) or "").strip().upper()
        tiempo_conexion_min = None
        if o_canon and d_canon and o_canon == d_canon:
            tiempo_conexion_min = 0
        else:
            t_a_depot = _tiempo_arista_grafo(gestor, nodo_fin_a_early, deposito, fin_real_a)
            t_depot_b = None
            if t_a_depot is not None:
                llegada_depot = fin_real_a + t_a_depot
                t_depot_b = _tiempo_arista_grafo(gestor, deposito, nodo_inicio_b_early, llegada_depot)
            if t_a_depot is not None and t_depot_b is not None:
                tiempo_conexion_min = int(t_a_depot) + int(t_depot_b)
            elif not union_solo_por_deposito:
                t_directo = _tiempo_arista_grafo(gestor, nodo_fin_a_early, nodo_inicio_b_early, fin_real_a)
                if t_directo is not None:
                    tiempo_conexion_min = int(t_directo)
        if tiempo_conexion_min is not None:
            descanso_inactivo = max(0, descanso - tiempo_conexion_min)

    if descanso_inactivo > parada_larga_umbral:
        if conexion_por_depot and descanso_inactivo <= excepcion_depot:
            pass  # Permitir: encadenamiento en depósito
        else:
            return _rechazo(
                f"parada larga: descanso inactivo {descanso_inactivo} min > umbral {parada_larga_umbral} min",
                descanso,
            )

    # Duración estricta combinada (depósito + toma + retorno depósito).
    tareas_unidas = list(ta.get("tareas_con_bus", [])) + list(tb.get("tareas_con_bus", []))
    duracion_strict = _duracion_operativa_desde_tareas(
        tareas_unidas,
        mapa_viajes,
        gestor,
        deposito,
        incluir_toma=True,
    )
    if duracion_strict is None:
        return _rechazo("no se pudo calcular duración estricta combinada", descanso)
    if duracion_strict > limite_jornada:
        return _rechazo(
            f"jornada combinada estricta {duracion_strict} min > límite {limite_jornada} min",
            descanso,
        )

    deposito = gestor.deposito_base
    nodo_fin_a = _nodo_fin_turno(ta, deposito)
    nodo_inicio_b = _nodo_inicio_turno(tb, mapa_viajes, deposito)
    if nodo_fin_a and nodo_inicio_b and not mismo_grupo:
        puede_relevo, _ = gestor.puede_hacer_relevo_en_nodo(nodo_fin_a)
        if (
            puede_relevo
            and not es_deposito(nodo_fin_a, deposito)
            and nodo_fin_a.upper() != nodo_inicio_b.upper()
        ):
            return _rechazo("relevo: A termina en otro nodo y no mismo grupo", descanso)

    buses_a = _buses_del_turno(ta)
    buses_b = _buses_del_turno(tb)
    cambios_existentes = max(ta.get("cambios_bus", 0), tb.get("cambios_bus", 0))
    if buses_a != buses_b:
        # Se permite cambio de bus en Fase 3 SIEMPRE que:
        #   - Estén en el mismo grupo de líneas / interlineo permitido
        #   - La conectividad de nodos esté soportada por vacíos/desplazamientos habilitados
        #   - No se supere el máximo de cambios de bus configurado
        if restringir_mismo_grupo and not mismo_grupo:
            return _rechazo("cambio de bus entre grupos de línea distintos", descanso)
        # Solo rechazar si la config fija un máximo bajo; por defecto 999 para no bloquear
        if max_cambios_bus < 999 and cambios_existentes + 1 > max_cambios_bus:
            return _rechazo(
                f"cambio de bus excede máximo permitido ({cambios_existentes + 1}>{max_cambios_bus})",
                descanso,
            )

    # VERIFICACIÓN: No solapamiento en el MISMO bus
    # Si ambos turnos usan el mismo bus, el último viaje de ta debe terminar antes del primer viaje de tb
    buses_comunes = buses_a & buses_b
    for bus_id in buses_comunes:
        tareas_ta = [(tid, b) for tid, b in ta.get("tareas_con_bus", []) if b == bus_id]
        tareas_tb = [(tid, b) for tid, b in tb.get("tareas_con_bus", []) if b == bus_id]
        if not tareas_ta or not tareas_tb:
            continue
        # Último viaje de ta en este bus
        ult_tid_ta = tareas_ta[-1][0]
        v_ta = mapa_viajes.get(ult_tid_ta) or mapa_viajes.get(str(ult_tid_ta))
        fin_ta = int(v_ta.get("fin", 0)) if v_ta else 0
        # Primer viaje de tb en este bus
        pri_tid_tb = tareas_tb[0][0]
        v_tb = mapa_viajes.get(pri_tid_tb) or mapa_viajes.get(str(pri_tid_tb))
        ini_tb = int(v_tb.get("inicio", 0)) if v_tb else 0
        if ini_tb < fin_ta:
            return _rechazo("solapamiento mismo bus (tb empieza antes de que ta termine)", descanso)

    # Conectividad: unión con camino válido (preferir depósito; mismo nodo o directo cuando se permite).
    if nodo_fin_a and nodo_inicio_b:
        o_canon = (gestor.nodo_canonico_para_conectividad(nodo_fin_a) or "").strip().upper()
        d_canon = (gestor.nodo_canonico_para_conectividad(nodo_inicio_b) or "").strip().upper()
        # Mismo nodo (canónico): el conductor ya está en el sitio; no exige paso por depósito.
        if o_canon and d_canon and o_canon == d_canon:
            pass  # Conectividad OK
        else:
            conectividad_ok = False
            # Intentar ruta por depósito: nodo_fin_A -> depósito -> nodo_inicio_B
            t_a_depot = _tiempo_arista_grafo(gestor, nodo_fin_a, deposito, fin_real_a)
            t_depot_b = None
            if t_a_depot is not None:
                llegada_depot = fin_real_a + t_a_depot
                t_depot_b = _tiempo_arista_grafo(gestor, deposito, nodo_inicio_b, llegada_depot)
            if t_a_depot is not None and t_depot_b is not None:
                tiempo_total_via_depot = t_a_depot + t_depot_b
                if descanso >= tiempo_total_via_depot:
                    descanso_efectivo = descanso - tiempo_total_via_depot
                    if descanso_efectivo >= descanso_min:
                        conectividad_ok = True
            if not conectividad_ok:
                if union_solo_por_deposito:
                    if t_a_depot is None:
                        return _rechazo(
                            f"union solo por deposito: no hay arista habilitada {nodo_fin_a!r}->deposito",
                            descanso,
                        )
                    if t_depot_b is None:
                        return _rechazo(
                            f"union solo por deposito: no hay arista habilitada deposito->{nodo_inicio_b!r}",
                            descanso,
                        )
                    tiempo_total_via_depot = t_a_depot + t_depot_b
                    if descanso < tiempo_total_via_depot:
                        return _rechazo(
                            f"union por deposito: tiempo disponible {descanso} min < desplazamiento A->depot->B ({tiempo_total_via_depot} min)",
                            descanso,
                        )
                    descanso_efectivo = descanso - tiempo_total_via_depot
                    return _rechazo(
                        f"union por deposito: descanso efectivo {descanso_efectivo} min < minimo {descanso_min} min",
                        descanso,
                    )
                # Fallback: conexión directa A -> B cuando no hay ruta por depósito (más uniones).
                t_directo = _tiempo_arista_grafo(gestor, nodo_fin_a, nodo_inicio_b, fin_real_a)
                if t_directo is None:
                    return _rechazo(
                        "no hay arista habilitada nodo_fin_A->nodo_inicio_B ni ruta por deposito",
                        descanso,
                    )
                if descanso < t_directo:
                    return _rechazo(
                        f"conexion directa: tiempo disponible {descanso} min < desplazamiento {t_directo} min",
                        descanso,
                    )
                descanso_efectivo = descanso - t_directo
                if descanso_efectivo < descanso_min:
                    return _rechazo(
                        f"conexion directa: descanso efectivo {descanso_efectivo} min < minimo {descanso_min} min",
                        descanso,
                    )

    return True


def _inicio_viaje(tid: Any, mapa_viajes: Dict) -> int:
    """Retorna el inicio del viaje en minutos, o 0 si no se encuentra."""
    v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
    if v and "inicio" in v:
        return int(v.get("inicio", 0))
    return 0


def _unir_turnos(
    ta: Dict, tb: Dict, gestor: GestorDeLogistica, mapa_viajes: Optional[Dict] = None
) -> Dict:
    """Crea el turno combinado: primero el turno que termina antes, luego el que empieza después.
    Así se evita solapamiento (no reordenar por inicio de viaje)."""
    deposito = gestor.deposito_base
    mapa = mapa_viajes or {}

    fin_ta = _fin_efectivo_turno(ta, mapa)
    inicio_ta = _inicio_efectivo_turno(ta, mapa)
    fin_tb = _fin_efectivo_turno(tb, mapa)
    inicio_tb = _inicio_efectivo_turno(tb, mapa)
    # Orden cronológico: el que termina primero va primero; el otro debe empezar después
    if fin_ta is not None and inicio_tb is not None and fin_ta < inicio_tb:
        primero, segundo = ta, tb
    elif fin_tb is not None and inicio_ta is not None and fin_tb < inicio_ta:
        primero, segundo = tb, ta
    else:
        # Solapamiento: no debería llegar aquí si _pueden_unirse está bien
        raise ValueError(
            f"_unir_turnos: solapamiento entre turnos (ta fin={fin_ta} ini={inicio_ta}, tb fin={fin_tb} ini={inicio_tb}). "
            "No se puede unir sin violar regla de no solapamiento."
        )

    ta, tb = primero, segundo
    buses_a = _buses_del_turno(ta)
    buses_b = _buses_del_turno(tb)
    cambios = max(ta.get("cambios_bus", 0), tb.get("cambios_bus", 0))
    if buses_a != buses_b:
        cambios += 1

    # Orden: ta (termina antes) + tb (empieza después). No reordenar por inicio de viaje.
    tareas_unidas = list(ta.get("tareas_con_bus", [])) + list(tb.get("tareas_con_bus", []))

    # punto_fin_turno del turno unido = el del turno que termina último
    t_ultimo = tb if tb["fin"] >= ta["fin"] else ta
    pf_raw = (t_ultimo.get("punto_fin_turno") or deposito).strip()
    if es_deposito(pf_raw, deposito):
        punto_fin = deposito
    else:
        valido, _, _ = _es_relevo_valido(pf_raw, deposito, gestor)
        punto_fin = pf_raw if valido else deposito

    # Inicio del turno unido: inicio de JORNADA del primer turno (ta['inicio'] = InS), para que
    # la duración almacenada coincida con lo validado en _pueden_unirse y NUNCA supere limite_jornada.
    inicio_union = int(ta["inicio"]) if ta.get("inicio") is not None else None
    if inicio_union is None:
        ini_a = _inicio_efectivo_turno(ta, mapa)
        ini_b = _inicio_efectivo_turno(tb, mapa)
        if ini_a is None and ini_b is None:
            inicio_union = 0
        elif ini_a is None:
            inicio_union = ini_b
        elif ini_b is None:
            inicio_union = ini_a
        else:
            inicio_union = min(ini_a, ini_b)
    duracion_span = (tb["fin"] - inicio_union) if (tb["fin"] >= inicio_union) else (tb["fin"] - inicio_union + 1440)
    limite = int(
        min(
            int(ta.get("limite_jornada_aplicable", getattr(gestor, "limite_jornada", 600)) or getattr(gestor, "limite_jornada", 600)),
            int(tb.get("limite_jornada_aplicable", getattr(gestor, "limite_jornada", 600)) or getattr(gestor, "limite_jornada", 600)),
        )
    )
    # No capar artificialmente: la factibilidad debe validarse estrictamente antes de aceptar la unión.
    fin_cap = tb["fin"]
    overtime = duracion_span > limite

    return {
        "id_bus": ta.get("id_bus"),
        "tareas_con_bus": tareas_unidas,
        "inicio": inicio_union,
        "fin": fin_cap,
        "duracion": duracion_span,
        "overtime": overtime,
        "limite_jornada_aplicable": limite,
        "cambios_bus": cambios,
        "deposito_inicio": ta.get("deposito_inicio") or gestor.deposito_base,
        "punto_fin_turno": punto_fin,
        "es_turno_unido": True,
    }


def _turno_unido_es_consistente(
    merged: Dict,
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    razon_fallo: Optional[List[str]] = None,
) -> bool:
    """
    Verifica que el turno unido tenga encadenamiento válido: cada transición
    (destino_anterior -> origen_siguiente) debe ser mismo nodo canónico o
    arista habilitada (solo conexiones ya configuradas y habilitadas).
    No se inventan comerciales ni vacíos; solo los ya creados en Fase 1.
    Si razon_fallo no es None y falla, se añade un string con el motivo.
    """
    def _fallo(msg: str) -> bool:
        if razon_fallo is not None:
            razon_fallo.append(msg)
        return False

    deposito = (gestor.deposito_base or "").strip()
    tareas = merged.get("tareas_con_bus", [])
    if not tareas:
        return True

    viajes = []
    for tid, _ in tareas:
        v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
        if not v:
            return _fallo("viaje no encontrado en mapa")
        viajes.append({
            "origen": (v.get("origen") or "").strip(),
            "destino": (v.get("destino") or "").strip(),
            "inicio": int(v["inicio"]) if v.get("inicio") is not None else 0,
            "fin": int(v["fin"]) if v.get("fin") is not None else 0,
        })

    # 1) Inicio: deposito_inicio del turno -> origen del primer viaje (solo conexión habilitada)
    dep_ini = (merged.get("deposito_inicio") or deposito).strip()
    origen_1 = viajes[0]["origen"]
    if origen_1:
        canon_dep = (gestor.nodo_canonico_para_conectividad(dep_ini) or "").strip().upper()
        canon_o1 = (gestor.nodo_canonico_para_conectividad(origen_1) or "").strip().upper()
        if canon_dep != canon_o1:
            tiempo_arista = _tiempo_arista_grafo(gestor, dep_ini, origen_1, 0)
            if tiempo_arista is not None and viajes[0]["inicio"] < tiempo_arista:
                return _fallo("inicio: viaje_1.inicio < tiempo_arista")

    # 2) Entre viajes consecutivos: dest_i -> orig_{i+1}
    for k in range(len(viajes) - 1):
        dest_prev = viajes[k]["destino"]
        orig_curr = viajes[k + 1]["origen"]
        fin_prev = viajes[k]["fin"]
        ini_curr = viajes[k + 1]["inicio"]
        if not dest_prev or not orig_curr:
            continue
        canon_d = (gestor.nodo_canonico_para_conectividad(dest_prev) or "").strip().upper()
        canon_o = (gestor.nodo_canonico_para_conectividad(orig_curr) or "").strip().upper()
        if canon_d == canon_o:
            if ini_curr < fin_prev:
                return _fallo(f"entre_viajes: ini_curr({ini_curr})<fin_prev({fin_prev}) en k={k}")
            continue
        tiempo_arista = _tiempo_arista_grafo(gestor, dest_prev, orig_curr, fin_prev)
        if tiempo_arista is None:
            return _fallo(f"entre_viajes: no arista {dest_prev}->{orig_curr} k={k}")
        if ini_curr < fin_prev + tiempo_arista:
            return _fallo(f"entre_viajes: ini_curr<fin_prev+t_arista k={k}")

    # 3) Fin: destino último viaje -> punto_fin_turno
    punto_fin = (merged.get("punto_fin_turno") or deposito).strip()
    dest_last = viajes[-1]["destino"]
    if dest_last:
        canon_d = (gestor.nodo_canonico_para_conectividad(dest_last) or "").strip().upper()
        canon_pf = (gestor.nodo_canonico_para_conectividad(punto_fin) or "").strip().upper()
        if canon_d != canon_pf:
            tiempo_arista = _tiempo_arista_grafo(gestor, dest_last, punto_fin, viajes[-1]["fin"])
            if tiempo_arista is None:
                # Si punto_fin es el depósito, permitir igual: el regreso puede hacerse en vacío (bus).
                if not es_deposito(punto_fin, deposito):
                    return _fallo("fin: no arista dest_last->punto_fin")
                # punto_fin es depósito: no bloquear por falta de arista desplazamiento (vacío lo cubre)

    # 4) Jornada estricta: también validar con traslado a/desde depósito + tiempo de toma.
    limite = int(merged.get("limite_jornada_aplicable", getattr(gestor, "limite_jornada", 600)) or getattr(gestor, "limite_jornada", 600))
    dur_strict = _duracion_operativa_desde_tareas(
        merged.get("tareas_con_bus", []),
        mapa_viajes,
        gestor,
        deposito,
        incluir_toma=True,
    )
    if dur_strict is None:
        return _fallo("jornada estricta: no se pudo calcular duración")
    if dur_strict > limite:
        return _fallo(f"jornada estricta {dur_strict} min > límite {limite} min")

    return True


# ---------------------------------------------------------------------------
# OR-Tools: máximo emparejamiento (máximas uniones respetando todas las reglas)
# ---------------------------------------------------------------------------

def _resolver_emparejamiento_ortools(
    turnos: List[Dict],
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    limite_jornada: int,
    descanso_min: int,
    max_cambios_bus: int,
    parada_larga_umbral_union: Optional[int] = None,
    parada_larga_excepcion_depot_min: Optional[int] = None,
    union_solo_por_deposito: bool = True,
    restringir_mismo_grupo: bool = True,
    timeout_seconds: float = 60.0,
) -> List[Tuple[int, int]]:
    """
    Calcula un emparejamiento máximo de turnos que pueden unirse (mismo grupo,
    solo por depósito, sin solapamientos, sin huecos). Cada turno entra a lo sumo
    en un par. Devuelve lista de (i, j) con i < j a unir.
    """
    if not _ORTOOLS_DISPONIBLE or len(turnos) < 2:
        return []

    deposito = gestor.deposito_base

    def _penalizacion_duracion_objetivo(ta: Dict, tb: Dict) -> int:
        """
        Penaliza duraciones alejadas del objetivo operativo (8-10h).
        Rango ideal: 480-600 min. Se permite >600 solo si reglas lo habilitan,
        pero con menor preferencia.
        """
        ini = int(ta.get("inicio", 0) or 0)
        fin = int(tb.get("fin", 0) or 0)
        dur = fin - ini if fin >= ini else fin - ini + 1440
        if 480 <= dur <= 600:
            return 0
        if dur < 480:
            return int(480 - dur)
        return int((dur - 600) * 2)
    # Construir lista de pares válidos (i, j) con i < j y el tiempo de parada (gap) de cada par
    pares_validos: List[Tuple[int, int]] = []
    gaps_minutos: List[int] = []
    penalizaciones_duracion: List[int] = []
    for i in range(len(turnos)):
        for j in range(i + 1, len(turnos)):
            if not _pueden_unirse(
                turnos[i], turnos[j], mapa_viajes, gestor, limite_jornada, descanso_min,
                max_cambios_bus, parada_larga_umbral_union=parada_larga_umbral_union,
                parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
                union_solo_por_deposito=union_solo_por_deposito,
                restringir_mismo_grupo=restringir_mismo_grupo,
            ):
                continue
            ta, tb = turnos[i], turnos[j]
            fin_ta = _fin_real_turno(ta, mapa_viajes, gestor, deposito) or _fin_efectivo_turno(ta, mapa_viajes) or 0
            fin_tb = _fin_real_turno(tb, mapa_viajes, gestor, deposito) or _fin_efectivo_turno(tb, mapa_viajes) or 0
            ini_ta = _inicio_efectivo_turno(ta, mapa_viajes) or 0
            ini_tb = _inicio_efectivo_turno(tb, mapa_viajes) or 0
            if fin_ta < ini_tb:
                gap = ini_tb - fin_ta
            elif fin_tb < ini_ta:
                gap = ini_ta - fin_tb
            else:
                gap = 0
            pares_validos.append((i, j))
            gaps_minutos.append(min(gap, 9999))
            penalizaciones_duracion.append(min(_penalizacion_duracion_objetivo(ta, tb), 9999))

    if not pares_validos:
        return []

    model = cp_model.CpModel()
    # Variable x_p = 1 si el par p se une
    x = [model.NewBoolVar(f"x_{i}_{j}") for (i, j) in pares_validos]

    # Cada turno participa en a lo sumo un par
    n = len(turnos)
    for turno_idx in range(n):
        inds_como_primero = [k for k, (i, j) in enumerate(pares_validos) if i == turno_idx]
        inds_como_segundo = [k for k, (i, j) in enumerate(pares_validos) if j == turno_idx]
        model.Add(sum(x[k] for k in inds_como_primero) + sum(x[k] for k in inds_como_segundo) <= 1)

    # Maximizar uniones y, a igualdad, minimizar tiempo de parada total (menor imp productividad)
    # Coeficiente por par: K - gap; así más uniones suma más y a igual número preferimos menor gap
    K = 100000
    model.Maximize(
        sum((K - gaps_minutos[k] - penalizaciones_duracion[k]) * x[k] for k in range(len(x)))
    )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = timeout_seconds
    solver.parameters.num_search_workers = 8
    status = solver.Solve(model)

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []

    resultado: List[Tuple[int, int]] = []
    for k, (i, j) in enumerate(pares_validos):
        if solver.Value(x[k]) == 1:
            resultado.append((i, j))
    return resultado


def _aplicar_emparejamiento(
    turnos: List[Dict],
    pares: List[Tuple[int, int]],
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    rechazos_consistencia: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Dado un emparejamiento (lista de (i,j) con i<j), une esos pares y devuelve
    la lista de turnos resultante (unidos + no emparejados). Los índices se refieren
    a turnos antes de cualquier fusión en esta ronda.
    """
    if not pares:
        return list(turnos)
    usados: Set[int] = set()
    for i, j in pares:
        usados.add(i)
        usados.add(j)
    resultado: List[Dict] = []
    for i, j in pares:
        ta, tb = turnos[i], turnos[j]
        merged = _unir_turnos(ta, tb, gestor, mapa_viajes)
        if not _turno_unido_es_consistente(
            merged, mapa_viajes, gestor, razon_fallo=rechazos_consistencia
        ):
            resultado.append(ta)
            resultado.append(tb)
        else:
            resultado.append(merged)
    for idx in range(len(turnos)):
        if idx not in usados:
            resultado.append(turnos[idx])
    return sorted(resultado, key=lambda t: t["inicio"])


# ---------------------------------------------------------------------------
# Algoritmo greedy de unión (solo uniones que no generen inconsistencias)
# ---------------------------------------------------------------------------

def _greedy_union(
    turnos: List[Dict],
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    limite_jornada: int,
    descanso_min: int,
    max_cambios_bus: int,
    parada_larga_umbral_union: Optional[int] = None,
    parada_larga_excepcion_depot_min: Optional[int] = None,
    union_solo_por_deposito: bool = True,
    restringir_mismo_grupo: bool = True,
    orden_inicial: str = "inicio",
    razon_rechazo: Optional[List[str]] = None,
    emparejar_menor_gap: bool = False,
    rechazos_descanso_corto: Optional[List[Tuple[int, str]]] = None,
    rechazos_por_consistencia: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Un pase greedy: ordena turnos, une cada turno con un compatible.
    Solo se acepta la unión si _turno_unido_es_consistente (encadenamiento válido
    con conexiones habilitadas; no se inventan comerciales ni vacíos).
    - emparejar_menor_gap=False: toma el primer compatible (orden temporal).
    - emparejar_menor_gap=True: entre todos los compatibles, elige el de menor gap (descanso).
    """
    if orden_inicial == "fin":
        key_sort = lambda t: t["fin"]
    elif orden_inicial == "fin_desc":
        key_sort = lambda t: (-t["fin"], t["inicio"])
    elif orden_inicial == "duracion":
        key_sort = lambda t: (t.get("duracion") or (t["fin"] - t["inicio"]))
    elif orden_inicial == "duracion_desc":
        key_sort = lambda t: (-(t.get("duracion") or (t["fin"] - t["inicio"])), t["inicio"])
    else:
        key_sort = lambda t: t["inicio"]
    # Repetir pasadas: en cada una se recorre todo y se evalúa cada turno con todos los demás.
    # Se sigue hasta que ya no quede ninguna unión posible en esta orden/estrategia.
    cambio = True
    actuales = sorted(turnos, key=key_sort)

    while cambio:
        cambio = False
        usados: Set[int] = set()
        nuevo: List[Dict] = []

        def _penalizacion_duracion_objetivo(ta: Dict, tb: Dict) -> int:
            ini = int(ta.get("inicio", 0) or 0)
            fin = int(tb.get("fin", 0) or 0)
            dur = fin - ini if fin >= ini else fin - ini + 1440
            if 480 <= dur <= 600:
                return 0
            if dur < 480:
                return int(480 - dur)
            return int((dur - 600) * 2)

        for i, ta in enumerate(actuales):
            if i in usados:
                continue
            mejor_j = -1
            mejor_gap = 999999
            mejor_pen_duracion = 999999

            for j in range(i + 1, len(actuales)):
                if j in usados:
                    continue
                tb = actuales[j]
                if not _pueden_unirse(
                    ta, tb, mapa_viajes, gestor, limite_jornada, descanso_min, max_cambios_bus,
                    parada_larga_umbral_union=parada_larga_umbral_union,
                    parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
                    union_solo_por_deposito=union_solo_por_deposito,
                    restringir_mismo_grupo=restringir_mismo_grupo,
                    razon_rechazo=razon_rechazo,
                    rechazos_descanso_corto=rechazos_descanso_corto,
                ):
                    continue
                gap = tb["inicio"] - ta["fin"]
                if emparejar_menor_gap:
                    pen_d = _penalizacion_duracion_objetivo(ta, tb)
                    if gap < mejor_gap or (gap == mejor_gap and pen_d < mejor_pen_duracion):
                        mejor_gap = gap
                        mejor_pen_duracion = pen_d
                        mejor_j = j
                else:
                    mejor_j = j
                    break

            if mejor_j >= 0:
                merged = _unir_turnos(ta, actuales[mejor_j], gestor, mapa_viajes)
                # Rechazar unión si generaría inconsistencias (encadenamiento no válido con conexiones habilitadas)
                if not _turno_unido_es_consistente(
                    merged, mapa_viajes, gestor,
                    razon_fallo=rechazos_por_consistencia,
                ):
                    nuevo.append(ta)
                    usados.add(i)
                    continue
                nuevo.append(merged)
                usados.add(i)
                usados.add(mejor_j)
                cambio = True
            else:
                nuevo.append(ta)
                usados.add(i)

        actuales = sorted(nuevo, key=lambda t: t["inicio"])

    return actuales


def _fusionar_uniones_pendientes(
    turnos: List[Dict[str, Any]],
    mapa_viajes: Dict,
    gestor: GestorDeLogistica,
    limite_jornada: int,
    descanso_min: int,
    max_cambios_bus: int,
    parada_larga_umbral_union: Optional[int] = None,
    parada_larga_excepcion_depot_min: Optional[int] = None,
    union_solo_por_deposito: bool = True,
    restringir_mismo_grupo: bool = True,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Barrido final de seguridad: si por combinatoria quedó algún par unible y consistente,
    lo fusiona iterativamente hasta agotar posibilidades.
    """
    actuales = list(turnos or [])
    if len(actuales) < 2:
        return actuales, 0

    uniones_extra = 0
    max_iter = max(1, len(actuales) * len(actuales))
    it = 0
    while it < max_iter:
        it += 1
        fusion_hecha = False
        n = len(actuales)
        for i in range(n):
            if fusion_hecha:
                break
            for j in range(i + 1, n):
                candidatos = ((actuales[i], actuales[j]), (actuales[j], actuales[i]))
                merged_ok = None
                for ta, tb in candidatos:
                    if not _pueden_unirse(
                        ta,
                        tb,
                        mapa_viajes,
                        gestor,
                        limite_jornada,
                        descanso_min,
                        max_cambios_bus,
                        parada_larga_umbral_union=parada_larga_umbral_union,
                        parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
                        union_solo_por_deposito=union_solo_por_deposito,
                        restringir_mismo_grupo=restringir_mismo_grupo,
                    ):
                        continue
                    merged = _unir_turnos(ta, tb, gestor, mapa_viajes)
                    if _turno_unido_es_consistente(merged, mapa_viajes, gestor):
                        merged_ok = merged
                        break
                if merged_ok is None:
                    continue
                nuevos = [t for k, t in enumerate(actuales) if k not in (i, j)]
                nuevos.append(merged_ok)
                actuales = sorted(nuevos, key=lambda t: int(t.get("inicio", 0) or 0))
                uniones_extra += 1
                fusion_hecha = True
                break
        if not fusion_hecha:
            break

    return actuales, uniones_extra


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def resolver_union_conductores(
    config: Dict[str, Any],
    turnos_conductores: List[Dict[str, Any]],
    metadata_tareas: Dict[Any, Dict[str, Any]],
    viajes_comerciales: List[Dict[str, Any]],
    gestor: GestorDeLogistica,
    verbose: bool = False,
    seed_externo: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    FASE 3: Une turnos compatibles para reducir el número total de conductores.

    Une hasta que ya no quede nada por unir. En cada ronda recorre TODO el listado
    de turnos y evalúa TODAS las combinaciones posibles (cada turno contra todos los
    demás), respetando reglas (mismo grupo, solo por depósito, límite jornada, sin
    solapamientos ni huecos). Se repite con los turnos ya unidos hasta que una ronda
    no reduzca el número de conductores.
    Returns: (turnos_unidos, estado)
    """
    print("\n" + "=" * 70)
    print("FASE 3: Unión de Conductores V2.0")
    print("=" * 70)

    f3_cfg = config.get("fase_3_union_conductores", {})
    limite_jornada: int = gestor.limite_jornada
    descanso_min: int = gestor.tiempo_descanso_minimo()
    deposito: str = gestor.deposito_base

    # Sin importar la cantidad de buses: solo se respeta limite_jornada y mismo grupo de línea.
    # Por defecto no limitar cambios de bus (999); la config puede bajar si se desea.
    grupos_lineas = getattr(gestor, "grupos_lineas", {}) or {}
    default_max_cambios = 999
    max_cambios_bus: int = int(f3_cfg.get("max_cambios_bus", default_max_cambios))

    parada_larga_cfg = f3_cfg.get("parada_larga_umbral_union")
    parada_larga_umbral_union: Optional[int] = int(parada_larga_cfg) if parada_larga_cfg is not None else None
    excepcion_depot_cfg = f3_cfg.get("parada_larga_excepcion_depot_min")
    parada_larga_excepcion_depot_min: Optional[int] = int(excepcion_depot_cfg) if excepcion_depot_cfg is not None else 120

    union_solo_por_deposito: bool = bool(f3_cfg.get("union_solo_por_deposito", True))
    # Regla dura solicitada: Fase 3 siempre restringida a mismo grupo de línea.
    restringir_mismo_grupo: bool = True

    print(f"  Límite jornada   : {limite_jornada} min")
    print(f"  Descanso mínimo  : {descanso_min} min")
    print(f"  Max cambios bus  : {max_cambios_bus} (solo se respeta límite jornada y mismo grupo)")
    print(f"  Estrategia       : iterativa hasta ya no poder juntar más; varias órdenes/estrategias por ronda")
    print(f"  Regla grupo      : solo se unen turnos del mismo grupo de línea; límite fijo: {limite_jornada} min")
    if union_solo_por_deposito:
        print(f"  Unión por depósito: obligatorio (nodo_fin_A -> depósito -> nodo_inicio_B); mismo nodo OK sin depósito")
    else:
        print(f"  Unión por depósito: preferido; si no hay ruta por depósito se permite conexión directa A->B (más uniones)")
    print(f"  Iteración        : hasta que ya no quede nada por unir (cada ronda recorre todo)")
    if _ORTOOLS_DISPONIBLE:
        print(f"  Solver           : OR-Tools CP-SAT (todas las combinaciones) + greedy por ronda")
    if parada_larga_umbral_union is not None:
        print(f"  Parada larga (F3): {parada_larga_umbral_union} min")
    print(f"  Excepción depósito (parada larga): hasta {parada_larga_excepcion_depot_min} min permitidos")
    print(f"  Turnos entrada   : {len(turnos_conductores)}")

    # Filtrar turnos sin viajes comerciales (capa de seguridad). Incluir id, _tmp_id
    # y también ids sintéticos presentes en metadata_tareas (p.ej. rescates por bus).
    ids_com: Set[Any] = set()
    for v in viajes_comerciales:
        for key in (v.get("id"), v.get("_tmp_id")):
            if key is not None:
                ids_com.add(key)
                ids_com.add(str(key))
    ids_meta: Set[Any] = set(metadata_tareas.keys()) | {str(tid) for tid in metadata_tareas.keys()}

    def _tiene_comerciales(turno: Dict[str, Any]) -> bool:
        for tid, _ in turno.get("tareas_con_bus", []):
            if tid in ids_com or str(tid) in ids_com:
                return True
            if tid in ids_meta or str(tid) in ids_meta:
                return True
        return False

    turnos_validos = [t for t in turnos_conductores if _tiene_comerciales(t)]
    if len(turnos_validos) < len(turnos_conductores):
        print(f"  [Filtro] Descartados {len(turnos_conductores) - len(turnos_validos)} turnos sin comerciales")

    if len(turnos_validos) <= 1:
        print("  Sin turnos suficientes para unir.")
        return turnos_validos, "Sin uniones"

    mapa_viajes: Dict[Any, Dict[str, Any]] = {}
    for v in viajes_comerciales:
        for key in (v.get("id"), v.get("_tmp_id")):
            if key is not None:
                mapa_viajes[key] = v
                mapa_viajes[str(key)] = v
    # Incluir metadata_tareas para que ids sintéticos (_ev_X_Y) resuelvan a viaje con inicio/fin
    # y _pueden_unirse / _unir_turnos no permitan uniones con solapamiento
    for tid, meta in metadata_tareas.items():
        if meta.get("viaje") and tid not in mapa_viajes:
            mapa_viajes[tid] = meta["viaje"]
            mapa_viajes[str(tid)] = meta["viaje"]

    # Fase 3 es ITERATIVA: unir hasta que ya no quede nada por unir.
    # En cada ronda se recorre TODO el listado de turnos y se evalúan TODAS las combinaciones
    # posibles (cada turno contra todos los demás), respetando reglas: mismo grupo, solo por
    # depósito, límite jornada, sin solapamientos ni huecos. Se repite con los turnos ya unidos
    # hasta que una ronda no reduzca el número de conductores.
    actuales: List[Dict] = list(turnos_validos)
    ordenes = ["inicio", "fin", "fin_desc", "duracion", "duracion_desc"]
    max_rondas = max(1, int(f3_cfg.get("max_rondas_union", 1000)))
    timeout_ortools = float(f3_cfg.get("timeout_ortools_segundos", 90.0))
    mostrar_rechazos_fase3: bool = bool(f3_cfg.get("mostrar_rechazos_fase3", False))
    ronda = 0

    # Diagnóstico ronda 0: cuántos pares podrían unirse (para evaluar si las condiciones son demasiado estrictas)
    n_turnos = len(actuales)
    total_pares = n_turnos * (n_turnos - 1) // 2
    pares_validos_count = 0
    razones_diag: List[str] = []
    for i in range(min(n_turnos, 500)):  # limitar a 500 turnos para no tardar mucho
        for j in range(i + 1, min(n_turnos, i + 1 + 300)):  # por cada i, solo hasta 300 j's
            if _pueden_unirse(
                actuales[i], actuales[j], mapa_viajes, gestor, limite_jornada, descanso_min,
                max_cambios_bus, parada_larga_umbral_union=parada_larga_umbral_union,
                parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
                union_solo_por_deposito=union_solo_por_deposito,
                restringir_mismo_grupo=restringir_mismo_grupo,
                razon_rechazo=razones_diag,
            ):
                pares_validos_count += 1
            if len(razones_diag) > 2000:
                break
        if len(razones_diag) > 2000:
            break
    muestreo = " (muestreo)" if n_turnos > 500 else ""
    print(f"  [Fase 3 diagnóstico] Turnos: {n_turnos}. Pares válidos para unir{muestreo}: {pares_validos_count}")
    if mostrar_rechazos_fase3 and razones_diag:
        por_razon = Counter(razones_diag)
        print(f"  [Fase 3 diagnóstico] Motivos de rechazo (muestra):")
        for msg, n in por_razon.most_common(8):
            print(f"    {n}x {msg[:85]}{'...' if len(msg) > 85 else ''}")

    rechazos_corto: Optional[List[Tuple[int, str]]] = [] if mostrar_rechazos_fase3 else None
    rechazos_consistencia: Optional[List[str]] = [] if mostrar_rechazos_fase3 else None
    while True:
        mejor_ronda = actuales

        # 1) OR-Tools: recorre todos los pares (i,j), evalúa todas las combinaciones válidas
        #    y devuelve el emparejamiento máximo que respeta todas las reglas.
        if _ORTOOLS_DISPONIBLE and len(actuales) >= 2:
            pares_ortools = _resolver_emparejamiento_ortools(
                actuales,
                mapa_viajes,
                gestor,
                limite_jornada,
                descanso_min,
                max_cambios_bus,
                parada_larga_umbral_union=parada_larga_umbral_union,
                parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
                union_solo_por_deposito=union_solo_por_deposito,
                restringir_mismo_grupo=restringir_mismo_grupo,
                timeout_seconds=timeout_ortools,
            )
            if pares_ortools:
                candidatos_ortools = _aplicar_emparejamiento(
                    actuales,
                    pares_ortools,
                    mapa_viajes,
                    gestor,
                    rechazos_consistencia=rechazos_consistencia if (mostrar_rechazos_fase3 and ronda == 0) else None,
                )
                if len(candidatos_ortools) < len(mejor_ronda):
                    mejor_ronda = candidatos_ortools
                if verbose and pares_ortools:
                    print(f"  [Fase 3 OR-Tools ronda {ronda}] Emparejamiento: {len(pares_ortools)} pares -> {len(candidatos_ortools)} conductores")

        # 2) Greedy: recorre cada turno contra todos los demás, varias órdenes y menor-gap;
        #    repite pasadas internas hasta que no haya más uniones en esa orden.
        for emparejar_menor_gap in (True, False):
            for orden_inicial in ordenes:
                razones: List[str] = [] if verbose else None
                candidatos = _greedy_union(
                    actuales,
                    mapa_viajes,
                    gestor,
                    limite_jornada,
                    descanso_min,
                    max_cambios_bus,
                    parada_larga_umbral_union=parada_larga_umbral_union,
                    parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
                    union_solo_por_deposito=union_solo_por_deposito,
                    restringir_mismo_grupo=restringir_mismo_grupo,
                    orden_inicial=orden_inicial,
                    razon_rechazo=razones,
                    emparejar_menor_gap=emparejar_menor_gap,
                    rechazos_descanso_corto=rechazos_corto if (mostrar_rechazos_fase3 and ronda == 0) else None,
                    rechazos_por_consistencia=rechazos_consistencia if (mostrar_rechazos_fase3 and ronda == 0) else None,
                )
                if verbose and razones and ronda == 0 and not emparejar_menor_gap:
                    resumen = Counter(razones)
                    print(f"  [Fase 3 orden '{orden_inicial}'] Motivos de rechazo (muestra):")
                    for msg, n in resumen.most_common(5):
                        print(f"    {n}x {msg}")
                if len(candidatos) < len(mejor_ronda):
                    mejor_ronda = candidatos

        if mostrar_rechazos_fase3 and ronda == 0 and (verbose or rechazos_consistencia):
            if rechazos_corto:
                por_razon = Counter(msg for _, msg in rechazos_corto)
                print(f"  [Fase 3 diagnóstico] Rechazos con descanso corto o solapamiento (muestra):")
                for msg, n in por_razon.most_common(5):
                    print(f"    {n}x {msg[:90]}{'...' if len(msg) > 90 else ''}")
            if rechazos_consistencia:
                por_razon = Counter(rechazos_consistencia)
                print(f"  [Fase 3 diagnóstico] Uniones rechazadas por consistencia: {len(rechazos_consistencia)}")
                for msg, n in por_razon.most_common(3):
                    print(f"    {n}x {msg}")
        if len(mejor_ronda) >= len(actuales):
            if verbose and ronda > 0:
                print(f"  [Fase 3] Sin más uniones posibles (ya no queda nada por unir). Rondas: {ronda}")
            break
        actuales = mejor_ronda
        ronda += 1
        if ronda >= max_rondas:
            print(f"  [Fase 3] Tope de seguridad de rondas ({max_rondas}). Conductores: {len(actuales)}")
            break
        if verbose:
            print(f"  [Fase 3 ronda {ronda}] Conductores: {len(actuales)}")

    mejor = actuales

    # Barrido final de seguridad: no dejar pares unibles pendientes.
    mejor, uniones_extra = _fusionar_uniones_pendientes(
        mejor,
        mapa_viajes,
        gestor,
        limite_jornada,
        descanso_min,
        max_cambios_bus,
        parada_larga_umbral_union=parada_larga_umbral_union,
        parada_larga_excepcion_depot_min=parada_larga_excepcion_depot_min,
        union_solo_por_deposito=union_solo_por_deposito,
        restringir_mismo_grupo=restringir_mismo_grupo,
    )
    if uniones_extra > 0:
        print(f"  [Fase 3] Uniones adicionales por barrido final: {uniones_extra}")

    # Normalizar punto_fin_turno en todos los turnos resultantes
    for t in mejor:
        pf = (t.get("punto_fin_turno") or "").strip()
        if not pf or es_deposito(pf, deposito):
            t["punto_fin_turno"] = deposito
        else:
            valido, _, _ = _es_relevo_valido(pf, deposito, gestor)
            if not valido:
                t["punto_fin_turno"] = deposito

    # REGLA DURA: Sin solapamiento conductor+bus en turnos unidos (no negociable)
    mapa_completo = dict(mapa_viajes)
    for tid, meta in metadata_tareas.items():
        if meta.get("viaje") and tid not in mapa_completo:
            mapa_completo[tid] = meta["viaje"]
    validar_fase3_sin_solapamiento_turnos(mejor, mapa_completo)

    reduccion = len(turnos_validos) - len(mejor)
    pct = (reduccion / len(turnos_validos) * 100) if turnos_validos else 0

    # REGLA DURA: Verificar que todo viaje comercial siga cubierto tras la unión
    canon_per_viaje = {v.get("id") or v.get("_tmp_id") for v in viajes_comerciales if (v.get("id") or v.get("_tmp_id")) is not None}
    cubiertos_f3 = set()
    for t in mejor:
        for tid, _ in t.get("tareas_con_bus", []):
            if tid is not None:
                cubiertos_f3.add(tid)
                cubiertos_f3.add(str(tid))
    faltantes_f3 = [c for c in canon_per_viaje if c not in cubiertos_f3 and str(c) not in cubiertos_f3]
    if faltantes_f3:
        print(f"  [FASE 3] ADVERTENCIA: {len(faltantes_f3)} viajes sin conductor tras unión (no debería ocurrir)")

    print(f"\n  RESULTADO FASE 3:")
    print(f"    Conductores antes : {len(turnos_validos)}")
    print(f"    Conductores después: {len(mejor)}")
    print(f"    Reducción         : {reduccion} ({pct:.1f}%)")
    print("=" * 70)

    estado = f"Fase 3: {len(mejor)} conductores ({reduccion} reducidos, {pct:.1f}%)"
    return mejor, estado
