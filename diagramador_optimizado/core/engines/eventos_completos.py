# -*- coding: utf-8 -*-
"""
Construcción de eventos completos como parte del cálculo (Fase 2/3).

Genera la lista completa de eventos por conductor (InS, FnS, Vacio, Parada,
Comercial, Desplazamiento, paradas de relleno) a partir de turnos, bloques
y eventos de bus. Toda la creación de eventos ocurre aquí; el exportador
solo recibe y escribe la lista sin mutarla.
"""
from __future__ import annotations

import collections
from typing import Any, Dict, List, Optional, Tuple

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.validaciones_fase import (
    validar_fase2_sin_solapamiento_turnos,
    validar_eventos_limite_jornada,
    validar_comerciales_todos_asignados,
    validar_conductores_con_comercial,
)
from diagramador_optimizado.core.tempo_conectividad import (
    es_deposito,
    obtener_tiempo_traslado,
    calcular_fin_turno,
    tiempo_a_minutos as _tiempo_a_minutos_tc,
    duracion_minutos,
    evento_cubierto_por_turno,
)
from diagramador_optimizado.utils.time_utils import (
    formatear_hora_deltatime,
)
from diagramador_optimizado.core.builders.eventos_completos_builder import crear_copia_evento_bus as _copia_evento_bus

# ---------------------------------------------------------------------------
# Simple caching wrappers for gestor calls
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
# Funciones auxiliares puras
# ---------------------------------------------------------------------------

def _tiempo_a_minutos(val: Any) -> int:
    return _tiempo_a_minutos_tc(val)

def _hora_para_excel(val: Any) -> str:
    if val is None or val == "": return ""
    if isinstance(val, (int, float)): return formatear_hora_deltatime(int(val))
    return str(val)

def _origenes_deposito_estricto(origen: str, gestor: GestorDeLogistica) -> bool:
    o = (origen or "").strip().upper()
    if not o: return False
    nombres_dep = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else None
    for d in (nombres_dep or [gestor.deposito_base]):
        if (d or "").strip().upper() == o: return True
    return False

def _es_relevo_valido_uncached(nodo: str, deposito: str, gestor: GestorDeLogistica) -> Tuple[bool, int, int]:
    if es_deposito(nodo, deposito): return True, 0, 0
    puede_relevo, _ = gestor.puede_hacer_relevo_en_nodo(nodo)
    if not puede_relevo: return False, 0, 0
    hab_ida, t_ida = _cached_buscar_info_desplazamiento(deposito, nodo, 0, gestor)
    hab_vuelta, t_vuelta = _cached_buscar_info_desplazamiento(nodo, deposito, 0, gestor)
    if hab_ida and hab_vuelta and t_ida is not None and t_vuelta is not None:
        return True, int(t_ida), int(t_vuelta)
    return False, 0, 0

def _es_relevo_valido(nodo: str, deposito: str, gestor: GestorDeLogistica) -> Tuple[bool, int, int]:
    return _cached_es_relevo_valido(nodo, deposito, gestor)

def _obtener_rangos_parada(gestor: Optional[GestorDeLogistica], nombre_nodo: str) -> Tuple[int, int]:
    if not nombre_nodo or not gestor: return 0, 24 * 60
    regla = getattr(gestor, "paradas_dict", None) or {}
    regla = regla.get(str(nombre_nodo).strip().upper(), {}) if isinstance(regla, dict) else {}
    return regla.get("min", 0), regla.get("max", 24 * 60)

def _tiempo_vacio_config(origen: str, destino: str, gestor: Optional[GestorDeLogistica]) -> Optional[int]:
    if gestor is None: return None
    config = getattr(gestor, "config", None) or {}
    vacios = config.get("vacios", {})
    if not isinstance(vacios, dict): return None
    origen_norm = str(origen or "").strip().upper()
    destino_norm = str(destino or "").strip().upper()
    if not origen_norm or not destino_norm: return None

    for clave, entrada in vacios.items():
        try:
            orig_cfg, dest_cfg = str(clave).split("_", 1)
        except ValueError: continue
        if origen_norm != orig_cfg.strip().upper() or destino_norm != dest_cfg.strip().upper(): continue
        franjas = entrada.get("franjas") or []
        if not franjas: continue
        t = franjas[0].get("tiempo")
        if t is None: continue
        try: return int(round(float(t)))
        except Exception: continue
    return None

def _nodo_para_excel(val: Any, gestor: Optional[GestorDeLogistica]) -> str:
    s = (val or "").strip()
    if not s or not gestor: return s
    if es_deposito(s, gestor.deposito_base or ""): return (gestor.deposito_base or s).strip()
    return s


# ---------------------------------------------------------------------------
# Creación de Eventos Base
# ---------------------------------------------------------------------------

def _evento_vacio(bus: Any, inicio_min: int, fin_min: int, origen: str, destino: str, km: float = 0, desc: str = "", conductor: Any = "") -> Dict[str, Any]:
    return {"evento": "Vacio", "bus": bus, "conductor": conductor, "inicio": formatear_hora_deltatime(inicio_min), "fin": formatear_hora_deltatime(fin_min), "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)), "origen": origen, "destino": destino, "linea": "", "kilometros": km, "desc": desc or f"Vacío {origen}->{destino}"}

def _evento_parada(bus: Any, inicio_min: int, fin_min: int, origen: str, desc: str = "", conductor: Any = "") -> Dict[str, Any]:
    return {"evento": "Parada", "bus": bus, "conductor": conductor, "inicio": formatear_hora_deltatime(inicio_min), "fin": formatear_hora_deltatime(fin_min), "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)), "origen": origen, "destino": origen, "linea": "", "kilometros": 0, "desc": desc or f"Parada en {origen}"}

def _evento_comercial(bus: Any, inicio_min: int, fin_min: int, origen: str, destino: str, conductor: Any = "", linea: str = "", km: float = 0, desc: str = "", viaje_id: Any = None, sentido: str = "", tipo_bus: str = "") -> Dict[str, Any]:
    return {"evento": "Comercial", "bus": bus, "conductor": conductor, "inicio": formatear_hora_deltatime(inicio_min), "fin": formatear_hora_deltatime(fin_min), "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)), "origen": origen, "destino": destino, "linea": linea, "kilometros": km, "desc": desc, "viaje_id": viaje_id, "sentido": sentido, "tipo_bus": tipo_bus}

def _evento_ins(id_conductor: Any, ins_inicio_min: int, ins_fin_min: int, deposito: str) -> Dict[str, Any]:
    return {"evento": "InS", "bus": "", "conductor": id_conductor, "inicio": formatear_hora_deltatime(ins_inicio_min), "fin": formatear_hora_deltatime(ins_fin_min), "duracion": formatear_hora_deltatime(max(0, ins_fin_min - ins_inicio_min)), "origen": deposito, "destino": deposito, "linea": "", "kilometros": 0, "desc": "Inicio de Jornada (Toma)"}

def _evento_fns(id_conductor: Any, fin_turno_min: int, deposito: str) -> Dict[str, Any]:
    return {"evento": "FnS", "bus": "", "conductor": id_conductor, "inicio": formatear_hora_deltatime(fin_turno_min), "fin": formatear_hora_deltatime(fin_turno_min), "duracion": "00:00", "origen": deposito, "destino": deposito, "linea": "", "kilometros": 0, "desc": "Fin de Jornada (Deja)"}

def _evento_desplazamiento(id_conductor: Any, inicio_min: int, fin_min: int, origen: str, destino: str, desc: str = "") -> Dict[str, Any]:
    return {"evento": "Desplazamiento", "bus": "", "conductor": id_conductor, "inicio": formatear_hora_deltatime(inicio_min), "fin": formatear_hora_deltatime(fin_min), "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)), "origen": origen, "destino": destino, "linea": "", "kilometros": 0, "desc": desc or f"Desplazamiento {origen}->{destino}"}

def _evento_parada_sintetica(id_conductor: Any, inicio_min: int, fin_min: int, origen: str, destino: str, desc: str = "Parada/descanso") -> Dict[str, Any]:
    return {"evento": "Parada", "bus": "", "conductor": id_conductor, "inicio": formatear_hora_deltatime(inicio_min), "fin": formatear_hora_deltatime(fin_min), "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)), "origen": origen, "destino": destino, "linea": "", "kilometros": 0, "desc": desc}

def _copia_evento_bus(ev: Dict[str, Any], bus_id: int, inicio_min: int, fin_min: int) -> Dict[str, Any]:
    dur_min = max(0, fin_min - inicio_min)
    tipo_raw = (str(ev.get("evento", "")) or "").strip()
    copia = {
        "evento": tipo_raw, "bus": bus_id, "conductor": "", "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min), "duracion": formatear_hora_deltatime(dur_min),
        "origen": ev.get("origen", ""), "destino": ev.get("destino", ""), "linea": ev.get("linea", ""),
        "kilometros": ev.get("kilometros", 0), "desc": ev.get("desc", ""), "tipo_bus": ev.get("tipo_bus", ""),
    }
    if tipo_raw == "Recarga":
        copia["porcentaje_bateria"] = ev.get("porcentaje_bateria", "")
        copia["posicion_recarga"] = ev.get("posicion_recarga", "")
    return copia


def _generar_eventos_de_bus_fase1(gestor: GestorDeLogistica, bloques_bus: List[List[Dict[str, Any]]]) -> Tuple[List[Dict[str, Any]], float]:
    deposito = gestor.deposito_base
    eventos_fase1_buses: List[Dict[str, Any]] = []
    km_vacio_total = 0.0

    for bus_id_idx, bloque in enumerate(bloques_bus):
        bus_id = bus_id_idx + 1
        if not bloque: continue
        primer_viaje = bloque[0]
        parada_min_origen, parada_max_origen = _obtener_rangos_parada(gestor, primer_viaje.get("origen", ""))

        if not es_deposito(primer_viaje.get("origen", ""), deposito):
            tiempo_vacio_ini, km_vacio_ini = gestor.buscar_tiempo_vacio(deposito, primer_viaje["origen"], primer_viaje["inicio"])
            if tiempo_vacio_ini is None or tiempo_vacio_ini <= 0: tiempo_vacio_ini, km_vacio_ini = 0, 0
            fin_vacio_ini = primer_viaje["inicio"] - parada_min_origen
            inicio_vacio_ini = fin_vacio_ini - tiempo_vacio_ini
            if inicio_vacio_ini < 0: inicio_vacio_ini, fin_vacio_ini = 0, tiempo_vacio_ini
            eventos_fase1_buses.append(_evento_vacio(bus_id, inicio_vacio_ini, fin_vacio_ini, deposito, primer_viaje["origen"], km=km_vacio_ini or 0, desc=f"Vacío {deposito}->{primer_viaje['origen']}"))
            km_vacio_total += km_vacio_ini or 0
            tiempo_parada_ini = primer_viaje["inicio"] - (fin_vacio_ini if tiempo_vacio_ini else primer_viaje["inicio"])
            if tiempo_parada_ini > 0: eventos_fase1_buses.append(_evento_parada(bus_id, fin_vacio_ini, primer_viaje["inicio"], primer_viaje["origen"], desc=f"Parada en {primer_viaje['origen']}"))
        else:
            hora_inicio_parada = max(primer_viaje["inicio"] - parada_min_origen, 0)
            tiempo_parada_ini = primer_viaje["inicio"] - hora_inicio_parada
            if tiempo_parada_ini > 0: eventos_fase1_buses.append(_evento_parada(bus_id, hora_inicio_parada, primer_viaje["inicio"], primer_viaje["origen"], desc=f"Parada en {primer_viaje['origen']}"))

        for indice_viaje, viaje in enumerate(bloque):
            if indice_viaje >= len(bloque) - 1: continue
            siguiente_viaje = bloque[indice_viaje + 1]
            if viaje["destino"] != siguiente_viaje["origen"]:
                tiempo_vacio_inter, km_vacio_inter = gestor.buscar_tiempo_vacio(viaje["destino"], siguiente_viaje["origen"], viaje["fin"])
                if tiempo_vacio_inter is None or tiempo_vacio_inter <= 0: tiempo_vacio_inter, km_vacio_inter = 0, 0
                inicio_vacio = viaje["fin"]
                fin_vacio = inicio_vacio + tiempo_vacio_inter
                eventos_fase1_buses.append(_evento_vacio(bus_id, inicio_vacio, fin_vacio, viaje["destino"], siguiente_viaje["origen"], km=km_vacio_inter or 0, desc=f"Vacío {viaje['destino']}->{siguiente_viaje['origen']}"))
                km_vacio_total += km_vacio_inter or 0
                parada_min_dest, parada_max_dest = _obtener_rangos_parada(gestor, siguiente_viaje["origen"])
                tiempo_parada = siguiente_viaje["inicio"] - fin_vacio
                if tiempo_parada > 0: eventos_fase1_buses.append(_evento_parada(bus_id, fin_vacio, siguiente_viaje["inicio"], siguiente_viaje["origen"], desc=f"Parada en {siguiente_viaje['origen']}"))
            else:
                parada_min_dest, parada_max_dest = _obtener_rangos_parada(gestor, viaje["destino"])
                tiempo_parada = siguiente_viaje["inicio"] - viaje["fin"]
                if tiempo_parada > 0: eventos_fase1_buses.append(_evento_parada(bus_id, viaje["fin"], siguiente_viaje["inicio"], viaje["destino"], desc=f"Parada en {viaje['destino']}"))

        ultimo_viaje = bloque[-1]
        if not es_deposito(ultimo_viaje.get("destino", ""), deposito):
            tiempo_vacio_fin, km_vacio_fin = gestor.buscar_tiempo_vacio(ultimo_viaje["destino"], deposito, ultimo_viaje["fin"])
            if tiempo_vacio_fin is None or tiempo_vacio_fin <= 0: tiempo_vacio_fin, km_vacio_fin = 0, 0
            inicio_vacio_fin = ultimo_viaje["fin"]
            fin_vacio_fin = inicio_vacio_fin + tiempo_vacio_fin
            eventos_fase1_buses.append(_evento_vacio(bus_id, inicio_vacio_fin, fin_vacio_fin, ultimo_viaje["destino"], deposito, km=km_vacio_fin or 0, desc=f"Vacío {ultimo_viaje['destino']}->{deposito}"))
            km_vacio_total += km_vacio_fin or 0

    eventos_fase1_buses.sort(key=lambda e: (_tiempo_a_minutos(e.get("inicio", 0)), e.get("bus", 0)))
    return eventos_fase1_buses, round(km_vacio_total, 1)


# ---------------------------------------------------------------------------
# Helpers para tratar Paradas y Cortes de Turnos
# ---------------------------------------------------------------------------

def _es_parada_viaje(v: Dict[str, Any]) -> bool:
    if not v: return False
    for k in ("evento", "tipo", "estado", "status", "actividad"):
        val = v.get(k)
        if not val or not isinstance(val, str): continue
        if "parada" in val.strip().lower(): return True
    return False

def _siguiente_no_parada_idx(bloque: List[Dict[str, Any]], start: int) -> Optional[int]:
    for i in range(start, len(bloque)):
        if not _es_parada_viaje(bloque[i]): return i
    return None

def _calcular_inicio_turno(primer_viaje: Dict[str, Any], relay_node_anterior: Optional[str], deposito: str, gestor: GestorDeLogistica, tiempo_toma: int, es_primer_turno: bool) -> int:
    origen = (primer_viaje.get("origen") or "").strip()
    inicio_viaje = int(primer_viaje.get("inicio", 0) or 0)
    if es_primer_turno or relay_node_anterior is None:
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, origen, inicio_viaje, gestor)
        return inicio_viaje - (t_vacio or 0) - tiempo_toma
    relay = relay_node_anterior
    _, t_dep_to_relay, _ = _cached_es_relevo_valido(relay, deposito, gestor)
    if es_deposito(relay, deposito):
        t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, origen, inicio_viaje, gestor)
        return inicio_viaje - (t_vacio or 0) - tiempo_toma
    if origen.upper() == relay.upper():
        return inicio_viaje - t_dep_to_relay - tiempo_toma
    t_vacio_relay_orig, _ = _cached_buscar_tiempo_vacio(relay, origen, inicio_viaje, gestor)
    return inicio_viaje - (t_vacio_relay_orig or 0) - t_dep_to_relay - tiempo_toma

def _puede_terminar_aqui(ultimo_viaje: Dict[str, Any], deposito: str, gestor: GestorDeLogistica) -> bool:
    destino = (ultimo_viaje.get("destino") or "").strip()
    if not destino: return False
    if es_deposito(destino, deposito): return True
    valido, _, _ = _cached_es_relevo_valido(destino, deposito, gestor)
    return valido

def _id_viaje(viaje: Dict[str, Any], fallback: str) -> Any:
    if viaje.get("id") is not None: return viaje.get("id")
    if viaje.get("_tmp_id") is not None: return viaje.get("_tmp_id")
    return fallback

def _canonical_viaje_id(viaje: Dict[str, Any], mapa_viaje: Dict[Any, Dict[str, Any]], fallback: str) -> Any:
    tid = _id_viaje(viaje, fallback)
    v = mapa_viaje.get(tid) or mapa_viaje.get(str(tid))
    if v is not None:
        if v.get("id") is not None: return v.get("id")
        if v.get("_tmp_id") is not None: return v.get("_tmp_id")
        return tid
    return tid


# ---------------------------------------------------------------------------
# LÓGICA DE FASE 2: RESOLVER DIAGRAMACIÓN Y CREACIÓN DE TURNOS
# ---------------------------------------------------------------------------

def _dividir_bloque(
    bloque: List[Dict[str, Any]], id_bus: int, deposito: str, limite_jornada: int, 
    tiempo_toma: int, gestor: GestorDeLogistica, mapa_viaje: Optional[Dict[Any, Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    n = len(bloque)
    if n == 0: return []
    umbral_parada_larga = gestor.parada_larga_umbral
    gaps = [int(bloque[i+1].get("inicio",0) or 0) - int(bloque[i].get("fin",0) or 0) for i in range(n-1)]
    for idx in range(len(gaps)): 
        if gaps[idx] < 0: gaps[idx] += 1440

    turnos, idx_inicio, relay_anterior, es_primer_turno = [], 0, None, True
    while idx_inicio < n:
        primer_idx_effectivo = idx_inicio
        if _es_parada_viaje(bloque[idx_inicio]):
            siguiente = _siguiente_no_parada_idx(bloque, idx_inicio)
            if siguiente is None: break
            primer_idx_effectivo = siguiente

        primer_viaje = bloque[primer_idx_effectivo]
        inicio_turno = _calcular_inicio_turno(primer_viaje, relay_anterior, deposito, gestor, tiempo_toma, es_primer_turno)

        mejor_fin = -1
        for idx_fin in range(n - 1, primer_idx_effectivo - 1, -1):
            ultimo_viaje = bloque[idx_fin]
            if _es_parada_viaje(ultimo_viaje) or not _puede_terminar_aqui(ultimo_viaje, deposito, gestor): continue
            
            tiene_parada_larga = any(gaps[j] > umbral_parada_larga for j in range(primer_idx_effectivo, idx_fin) if j < len(gaps))
            if tiene_parada_larga: continue
            
            fin_turno, _ = calcular_fin_turno(ultimo_viaje, deposito, gestor)
            if duracion_minutos(inicio_turno, fin_turno) > limite_jornada: continue
            mejor_fin = idx_fin
            break

        if mejor_fin < primer_idx_effectivo:
            viaje_solo = bloque[primer_idx_effectivo]
            dest_solo = (viaje_solo.get("destino") or "").strip()
            fin_solo = int(viaje_solo.get("fin", 0) or 0)
            if not es_deposito(dest_solo, deposito):
                t_ret, _ = _cached_buscar_tiempo_vacio(dest_solo, deposito, fin_solo, gestor)
                if not t_ret: _, _, t_ret = _cached_es_relevo_valido(dest_solo, deposito, gestor)
                fin_solo += (t_ret or 0)
            dur = duracion_minutos(inicio_turno, fin_solo)
            tid_canon = (_canonical_viaje_id(viaje_solo, mapa_viaje, f"_ev_{id_bus}_{primer_idx_effectivo}") if mapa_viaje else _id_viaje(viaje_solo, f"_ev_{id_bus}_{primer_idx_effectivo}"))
            turnos.append({
                "id_bus": id_bus, "tareas_con_bus": [(tid_canon, id_bus)], "inicio": inicio_turno, "fin": fin_solo,
                "duracion": dur, "overtime": dur > limite_jornada, "deposito_inicio": deposito, "punto_fin_turno": deposito
            })
            idx_inicio, relay_anterior, es_primer_turno = primer_idx_effectivo + 1, None, False
            continue

        subbloque = bloque[primer_idx_effectivo: mejor_fin + 1]
        ultimo_v = subbloque[-1]
        fin_turno, _ = calcular_fin_turno(ultimo_v, deposito, gestor)
        relay_node = (ultimo_v.get("destino") or "").strip()
        def _tid(v, j): return _canonical_viaje_id(v, mapa_viaje, f"_ev_{id_bus}_{primer_idx_effectivo + j}") if mapa_viaje else _id_viaje(v, f"_ev_{id_bus}_{primer_idx_effectivo + j}")

        turnos.append({
            "id_bus": id_bus, "tareas_con_bus": [(_tid(v, j), id_bus) for j, v in enumerate(subbloque)],
            "inicio": inicio_turno, "fin": fin_turno, "duracion": duracion_minutos(inicio_turno, fin_turno),
            "overtime": False, "deposito_inicio": deposito, "punto_fin_turno": deposito if es_deposito(relay_node, deposito) else relay_node
        })
        relay_anterior = relay_node if not es_deposito(relay_node, deposito) else None
        idx_inicio, es_primer_turno = mejor_fin + 1, False

    return turnos


def _forzar_limite_jornada(turnos: List[Dict[str, Any]], mapa_viaje: Dict, metadata_tareas: Dict, limite_jornada: int, gestor: GestorDeLogistica, deposito: str) -> List[Dict[str, Any]]:
    mapa_completo = dict(mapa_viaje or {})
    for tid, meta in (metadata_tareas or {}).items():
        v = meta.get("viaje")
        if v and tid not in mapa_completo: mapa_completo[tid] = v; mapa_completo[str(tid)] = v

    resultado: List[Dict[str, Any]] = []
    for t in turnos:
        inicio_t, fin_t = int(t.get("inicio", 0) or 0), int(t.get("fin", 0) or 0)
        tasks = list(t.get("tareas_con_bus") or [])
        if tasks:
            vf, vl = mapa_completo.get(tasks[0][0]) or mapa_completo.get(str(tasks[0][0])), mapa_completo.get(tasks[-1][0]) or mapa_completo.get(str(tasks[-1][0]))
            if vf and vl:
                inicio_t = _calcular_inicio_turno(vf, None, deposito, gestor, int(getattr(gestor, "tiempo_toma", 15) or 15), True)
                fin_t, _ = calcular_fin_turno(vl, deposito, gestor)
        duracion = duracion_minutos(inicio_t, fin_t)
        
        if duracion <= limite_jornada or not tasks:
            t["inicio"], t["fin"], t["duracion"], t["overtime"] = inicio_t, fin_t, duracion, False
            resultado.append(t)
            continue

        first_over = 1
        for i in range(len(tasks)):
            v = mapa_completo.get(tasks[i][0]) or mapa_completo.get(str(tasks[i][0]))
            if not v: break
            fi, _ = calcular_fin_turno(v, deposito, gestor)
            if duracion_minutos(inicio_t, fi) > limite_jornada:
                first_over = max(1, i)
                break

        part1, part2 = tasks[0:first_over], tasks[first_over:]
        vl1 = mapa_completo.get(part1[-1][0]) or mapa_completo.get(str(part1[-1][0]))
        f1_raw, _ = calcular_fin_turno(vl1, deposito, gestor) if vl1 else ((inicio_t + limite_jornada) % 1440, False)
        f1 = (inicio_t + limite_jornada) % 1440 if duracion_minutos(inicio_t, f1_raw) > limite_jornada else f1_raw
        
        resultado.append({
            "id_bus": t.get("id_bus"), "tareas_con_bus": part1, "inicio": inicio_t, "fin": f1,
            "duracion": min(duracion_minutos(inicio_t, f1), limite_jornada), "overtime": False,
            "deposito_inicio": t.get("deposito_inicio"), "punto_fin_turno": t.get("punto_fin_turno") if not part2 else deposito,
        })

        if part2:
            vf2 = mapa_completo.get(part2[0][0]) or mapa_completo.get(str(part2[0][0]))
            ini2 = _calcular_inicio_turno(vf2, None, deposito, gestor, int(getattr(gestor, "tiempo_toma", 15) or 15), True) if vf2 else ((inicio_t + limite_jornada) % 1440)
            vl2 = mapa_completo.get(part2[-1][0]) or mapa_completo.get(str(part2[-1][0]))
            f2, _ = calcular_fin_turno(vl2, deposito, gestor) if vl2 else ((ini2 + limite_jornada) % 1440, False)
            dur2 = duracion_minutos(ini2, f2)
            
            t2 = {
                "id_bus": t.get("id_bus"), "tareas_con_bus": part2, "inicio": ini2, "fin": f2,
                "duracion": dur2, "overtime": dur2 > limite_jornada, "deposito_inicio": t.get("deposito_inicio"), "punto_fin_turno": t.get("punto_fin_turno"),
            }
            if dur2 > limite_jornada: resultado.extend(_forzar_limite_jornada([t2], mapa_viaje, metadata_tareas, limite_jornada, gestor, deposito))
            else:
                t2["overtime"] = False
                resultado.append(t2)

    return resultado


def resolver_diagramacion_conductores(
    config: Dict[str, Any], viajes_comerciales: List[Dict[str, Any]], bloques_bus: List[List[Dict[str, Any]]], gestor: GestorDeLogistica, verbose: bool = False
) -> Tuple[List[Dict[str, Any]], Dict[Any, Dict[str, Any]], str]:
    print("\n" + "=" * 70)
    print("FASE 2: Asignación de Conductores V2.0")
    print("=" * 70)

    limite_jornada = gestor.limite_jornada
    tiempo_toma = gestor.tiempo_toma
    deposito = gestor.deposito_base

    ids_viajes = set()
    mapa_viaje = {}
    for v in viajes_comerciales:
        for key in (v.get("id"), v.get("_tmp_id")):
            if key is not None:
                ids_viajes.add(key)
                ids_viajes.add(str(key))
                mapa_viaje[key] = v
                mapa_viaje[str(key)] = v

    metadata_tareas = {}
    for id_bus, bloque in enumerate(bloques_bus):
        for i, viaje in enumerate(bloque):
            tid = _id_viaje(viaje, f"_ev_{id_bus}_{i}")
            meta = {"viaje": viaje, "id_bus": id_bus, "es_primero": i == 0, "es_ultimo": i == len(bloque) - 1}
            metadata_tareas[tid] = meta
            metadata_tareas[str(tid)] = meta
            canon = _canonical_viaje_id(viaje, mapa_viaje, tid)
            if canon != tid:
                metadata_tareas[canon] = meta
                metadata_tareas[str(canon)] = meta

    turnos = []
    for id_bus, bloque in enumerate(bloques_bus):
        if not bloque: continue
        primer_v, ultimo_v = bloque[0], bloque[-1]
        t_vacio_ida, _ = _cached_buscar_tiempo_vacio(deposito, primer_v["origen"], primer_v["inicio"], gestor)
        inicio_bloque_ins = int(primer_v.get("inicio", 0) or 0) - (t_vacio_ida or 0) - tiempo_toma
        fin_bloque, termina_bien = calcular_fin_turno(ultimo_v, deposito, gestor)
        duracion_bloque = duracion_minutos(inicio_bloque_ins, fin_bloque)

        if duracion_bloque <= limite_jornada and termina_bien:
            relay_fin = (ultimo_v.get("destino") or "").strip()
            turnos.append({
                "id_bus": id_bus,
                "tareas_con_bus": [(_canonical_viaje_id(v, mapa_viaje, f"_ev_{id_bus}_{j}"), id_bus) for j, v in enumerate(bloque)],
                "inicio": inicio_bloque_ins, "fin": fin_bloque, "duracion": duracion_bloque, "overtime": False,
                "deposito_inicio": deposito, "punto_fin_turno": deposito if es_deposito(relay_fin, deposito) else relay_fin,
            })
        else:
            turnos.extend(_dividir_bloque(bloque, id_bus, deposito, limite_jornada, tiempo_toma, gestor, mapa_viaje))

    # Fallback para no dejar viajes descubiertos
    vistos = set()
    for t in turnos:
        for tid, _ in t.get("tareas_con_bus", []): vistos.add(str(tid))
    for v in viajes_comerciales:
        canon = str(_id_viaje(v, ""))
        if canon and canon not in vistos:
            t_vacio, _ = _cached_buscar_tiempo_vacio(deposito, v["origen"], v["inicio"], gestor)
            inicio_ins = int(v.get("inicio", 0) or 0) - (t_vacio or 0) - tiempo_toma
            fin, _ = calcular_fin_turno(v, deposito, gestor)
            turnos.append({
                "id_bus": 0, "tareas_con_bus": [(canon, 0)], "inicio": inicio_ins, "fin": fin,
                "duracion": max(1, duracion_minutos(inicio_ins, fin)), "overtime": False, "deposito_inicio": deposito, "punto_fin_turno": deposito,
            })
            vistos.add(canon)

    for t in turnos: t["deposito_inicio"] = deposito
    turnos = _forzar_limite_jornada(turnos, mapa_viaje, metadata_tareas, limite_jornada, gestor, deposito)

    # Tope final de emergencia
    for t in turnos:
        ini, fin = int(t.get("inicio", 0) or 0), int(t.get("fin", 0) or 0)
        dur = duracion_minutos(ini, fin)
        if dur > limite_jornada:
            t["fin"] = (ini + limite_jornada) % 1440
            t["duracion"] = limite_jornada

    return turnos, metadata_tareas, "OPTIMAL"


# =========================================================================================
# CREACION DE EVENTOS COMPLETOS (PIPELINE DE 5 FASES LIMPIO Y FIEL A TURNOS)
# =========================================================================================

def _crear_evento_base_dict(evento: str, bus: Any, conductor: Any, ini_min: int, fin_min: int, origen: str, destino: str, linea: str = "", km: float = 0, desc: str = "", viaje_id: Any = None, sentido: str = "", tipo_bus: str = "") -> dict:
    return {
        "evento": evento, "bus": bus, "conductor": conductor,
        "_ini": ini_min, "_fin": fin_min,
        "origen": origen, "destino": destino, "linea": linea, "kilometros": km, "desc": desc,
        "viaje_id": viaje_id, "sentido": sentido, "tipo_bus": tipo_bus
    }

def _construir_mapa_viajes(viajes_comerciales: List[Dict[str, Any]], metadata_tareas: Dict[Any, Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
    mapa: Dict[Any, Dict[str, Any]] = {}
    for v in viajes_comerciales or []:
        for key in (v.get("id"), v.get("_tmp_id")):
            if key is not None:
                mapa[key] = v
                mapa[str(key)] = v
    for tid, meta in (metadata_tareas or {}).items():
        v = meta.get("viaje") if isinstance(meta, dict) else None
        if v and tid not in mapa:
            mapa[tid] = v
            mapa[str(tid)] = v
    return mapa

def construir_eventos_completos(
    gestor: GestorDeLogistica, bloques_bus: List[List[Dict[str, Any]]], turnos_seleccionados: List[Dict[str, Any]],
    viajes_comerciales: List[Dict[str, Any]], metadata_tareas: Dict[Any, Dict[str, Any]],
    eventos_bus: Optional[List[List[Dict[str, Any]]]] = None,
    limites_por_conductor: Optional[Dict[Any, int]] = None,
) -> List[Dict[str, Any]]:
    
    deposito = gestor.deposito_base
    tiempo_toma = int(getattr(gestor, "tiempo_toma", 15) or 15)
    limite_jornada_final = int(getattr(gestor, "limite_jornada", 600) or 600)

    # 1. Base Events
    eventos_base_bus_puro = []
    if eventos_bus:
        for bus_idx, lista in enumerate(eventos_bus):
            bus_id = bus_idx + 1
            for ev in lista or []:
                if (str(ev.get("evento", "")) or "").strip().upper() in ("VACIO", "PARADA", "RECARGA"):
                    ev_c = _copia_evento_bus(ev, bus_id, _tiempo_a_minutos(ev.get("inicio", 0)), _tiempo_a_minutos(ev.get("fin", 0)))
                    ev_c["_ini"] = _tiempo_a_minutos(ev_c["inicio"])
                    ev_c["_fin"] = _tiempo_a_minutos(ev_c["fin"])
                    eventos_base_bus_puro.append(ev_c)
    else:
        evs, _ = _generar_eventos_de_bus_fase1(gestor, bloques_bus)
        for ev in evs:
            ev["_ini"] = _tiempo_a_minutos(ev["inicio"])
            ev["_fin"] = _tiempo_a_minutos(ev["fin"])
            eventos_base_bus_puro.append(ev)

    def _vid_ec(v: Dict[str, Any]) -> Any:
        """ID canónico para deduplicación. Maneja id=0 correctamente."""
        vid = v.get("id")
        if vid is None:
            vid = v.get("_tmp_id")
        return vid

    viaje_a_bus = {}
    for id_bus, bloque in enumerate(bloques_bus):
        for v in bloque:
            if (v.get("evento") or "").strip().lower() in ("vacio", "parada", "recarga"):
                continue
            vid_v = _vid_ec(v)
            if vid_v is not None:
                viaje_a_bus[vid_v] = id_bus
    tarea_a_conductor = {
        str(tid): c_id
        for c_id, t in enumerate(turnos_seleccionados or [], start=1)
        for tid, bus_idx in t.get("tareas_con_bus", [])
        if tid is not None
    }
    # Índice robusto bus->conductores usando tareas_con_bus (no id_bus),
    # para cubrir turnos multibus en Fase 3.
    conductores_por_bus_idx: Dict[int, List[int]] = collections.defaultdict(list)
    for c_id, t in enumerate(turnos_seleccionados or [], start=1):
        buses_turno = {int(bus_idx) for _, bus_idx in (t.get("tareas_con_bus", []) or []) if bus_idx is not None}
        for bidx in sorted(buses_turno):
            conductores_por_bus_idx[bidx].append(c_id)
    umbral_parada_larga = int(getattr(gestor, "parada_larga_umbral", 60) or 60)

    comerciales = []
    conduc_bus_con_comercial = set()
    for v in (viajes_comerciales or []):
        vid = _vid_ec(v)
        bus_idx = viaje_a_bus.get(vid)
        if bus_idx is None:
            bus_idx = viaje_a_bus.get(str(vid))
        bus_id = (bus_idx + 1) if bus_idx is not None else ""
        ini_min, fin_min = _tiempo_a_minutos(v.get("inicio", 0)), _tiempo_a_minutos(v.get("fin", 0))
        c_asig = tarea_a_conductor.get(str(vid))
        tipo_bus_com = next((vb.get("tipo_bus", "") for vb in bloques_bus[bus_idx] if (vb.get("evento") or "").strip().lower() not in ("vacio", "parada", "recarga")), "") if bus_idx is not None else ""
        
        comerciales.append(_crear_evento_base_dict("Comercial", bus_id, c_asig or "", ini_min, fin_min, v.get("origen", ""), v.get("destino", ""), v.get("linea", ""), v.get("kilometros", 0), v.get("desc", ""), vid, v.get("sentido", ""), tipo_bus_com))
        if c_asig and bus_id: conduc_bus_con_comercial.add((c_asig, int(bus_id)))

    eventos_base = []
    for ev in eventos_base_bus_puro:
        bus_val = ev.get("bus")
        ev["conductor"] = ""
        if str(ev.get("evento", "")).strip().upper() == "PARADA":
            dur_ev = int(ev.get("_fin", 0) or 0) - int(ev.get("_ini", 0) or 0)
            if dur_ev > umbral_parada_larga:
                # Regla dura operativa: una parada larga NO puede quedar asociada
                # a un conductor; el corte de turno debe ocurrir antes/después.
                eventos_base.append(ev)
                continue
        if bus_val not in (None, "", 0, "0"):
            bus_idx = int(bus_val) - 1
            candidatos = conductores_por_bus_idx.get(bus_idx, [])
            for c_id in candidatos:
                turno = turnos_seleccionados[c_id - 1]
                t_ini, t_fin = int(turno.get("inicio", 0)), int(turno.get("fin", 0))
                if evento_cubierto_por_turno(ev["_ini"], ev["_fin"], t_ini, t_fin, margen_min=0):
                    ev["conductor"] = c_id
                    break
        eventos_base.append(ev)

    eventos_base.extend(comerciales)

    # Asignación obligatoria de VACIO por continuidad operacional del bus.
    # Si un vacío de BusEventos queda fuera del rango exacto del turno, se intenta
    # heredar el conductor del evento contiguo (prev/next) del mismo bus.
    def _can_local(nodo: Any) -> str:
        return (gestor.nodo_canonico_para_conectividad(nodo) or "").strip().upper()

    def _bus_key_norm(bus_id: Any) -> str:
        if bus_id in (None, "", 0, "0"):
            return ""
        s = str(bus_id).strip()
        if not s:
            return ""
        try:
            f = float(s)
            if abs(f - int(f)) < 1e-9:
                return str(int(f))
        except Exception:
            pass
        if s.isdigit():
            return str(int(s))
        return s

    by_bus_asignados: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    for evb in eventos_base:
        bus_e = evb.get("bus")
        cond_e = evb.get("conductor")
        if bus_e in (None, "", 0, "0") or cond_e in (None, ""):
            continue
        key_bus = _bus_key_norm(bus_e)
        if not key_bus:
            continue
        by_bus_asignados[key_bus].append(evb)
    for k in list(by_bus_asignados.keys()):
        by_bus_asignados[k].sort(key=lambda x: (int(x.get("_ini", 0) or 0), int(x.get("_fin", 0) or 0)))

    for evb in eventos_base:
        if str(evb.get("evento", "")).strip().upper() != "VACIO":
            continue
        if evb.get("conductor") not in (None, ""):
            continue
        bus_e = evb.get("bus")
        if bus_e in (None, "", 0, "0"):
            continue
        bus_key = _bus_key_norm(bus_e)
        if not bus_key:
            continue
        candidatos = by_bus_asignados.get(bus_key, [])
        if not candidatos:
            continue

        ev_ini = int(evb.get("_ini", 0) or 0)
        ev_fin = int(evb.get("_fin", 0) or 0)
        ev_o = _can_local(evb.get("origen", ""))
        ev_d = _can_local(evb.get("destino", ""))
        tol = 2

        prev_match = None
        next_match = None
        for c in candidatos:
            c_ini = int(c.get("_ini", 0) or 0)
            c_fin = int(c.get("_fin", 0) or 0)
            c_o = _can_local(c.get("origen", ""))
            c_d = _can_local(c.get("destino", ""))
            if c_fin <= ev_ini and (ev_ini - c_fin) <= tol and c_d == ev_o:
                prev_match = c
            if next_match is None and c_ini >= ev_fin and (c_ini - ev_fin) <= tol and c_o == ev_d:
                next_match = c

        conductor_asignado = None
        if prev_match is not None and next_match is not None:
            c_prev = prev_match.get("conductor")
            c_next = next_match.get("conductor")
            if c_prev not in (None, "") and str(c_prev) == str(c_next):
                conductor_asignado = c_prev
        if conductor_asignado is None and prev_match is not None:
            c_prev = prev_match.get("conductor")
            if c_prev not in (None, ""):
                conductor_asignado = c_prev
        if conductor_asignado is None and next_match is not None:
            c_next = next_match.get("conductor")
            if c_next not in (None, ""):
                conductor_asignado = c_next

        if conductor_asignado not in (None, ""):
            evb["conductor"] = conductor_asignado
            by_bus_asignados[bus_key].append(evb)
            by_bus_asignados[bus_key].sort(key=lambda x: (int(x.get("_ini", 0) or 0), int(x.get("_fin", 0) or 0)))

    eventos_sin_conductor = [e for e in eventos_base if not e.get("conductor")]
    eventos_con_conductor = collections.defaultdict(list)
    for e in eventos_base:
        if e.get("conductor"): eventos_con_conductor[e["conductor"]].append(e)

    def get_canonical(nodo): return (gestor.nodo_canonico_para_conectividad(nodo) or "").strip().upper()

    # Regla operativa: EventosCompletos solo puede usar "Vacio" si ese vacío
    # existe en la traza de Fase 1 para el mismo bus.
    vacios_fase1_por_bus: Dict[str, List[Tuple[int, int, str, str]]] = collections.defaultdict(list)
    for ev in eventos_base_bus_puro:
        if str(ev.get("evento", "")).strip().upper() != "VACIO":
            continue
        bus_v = ev.get("bus")
        if bus_v in (None, "", 0, "0"):
            continue
        bus_key = _bus_key_norm(bus_v)
        if not bus_key:
            continue
        vacios_fase1_por_bus[bus_key].append(
            (
                int(ev.get("_ini", 0) or 0),
                int(ev.get("_fin", 0) or 0),
                get_canonical(ev.get("origen", "")),
                get_canonical(ev.get("destino", "")),
            )
        )

    def _vacio_existe_en_fase1(bus_id: Any, ini_min: int, fin_min: int, origen: str, destino: str) -> bool:
        if bus_id in (None, "", 0, "0"):
            return False
        bus_key = _bus_key_norm(bus_id)
        if not bus_key:
            return False
        candidatos = vacios_fase1_por_bus.get(bus_key, [])
        if not candidatos:
            return False
        o_can = get_canonical(origen)
        d_can = get_canonical(destino)
        ini = int(ini_min)
        fin = int(fin_min)
        # Tolerancia mínima de 1 minuto para absorber redondeos de borde.
        tol = 1
        for ini_b, fin_b, o_b, d_b in candidatos:
            if o_b != o_can or d_b != d_can:
                continue
            if abs(ini_b - ini) <= tol and abs(fin_b - fin) <= tol:
                return True
        return False

    eventos_casi_finales = []
    for cid, lista in eventos_con_conductor.items():
        turno_ref = None
        try:
            turno_ref = turnos_seleccionados[int(cid) - 1]
        except Exception:
            turno_ref = None
        limite_jornada_conductor = int(
            (turno_ref or {}).get(
                "limite_jornada_aplicable",
                (limites_por_conductor or {}).get(
                    cid,
                    (limites_por_conductor or {}).get(str(cid), limite_jornada_final),
                ),
            ) or limite_jornada_final
        )
        lista.sort(key=lambda x: x["_ini"])
        if not any(str(ev.get("evento", "")).strip().upper() == "COMERCIAL" for ev in lista):
            for ev in lista:
                if ev.get("bus"):
                    ev["conductor"] = ""
                    eventos_sin_conductor.append(ev)
            continue
        
        reales_limpios = []
        for ev in lista:
            ev_descartado = False
            while reales_limpios:
                prev = reales_limpios[-1]
                t_necesario = obtener_tiempo_traslado(prev['destino'], ev['origen'], prev['_fin'], gestor)
                gap = ev["_ini"] - prev["_fin"]
                
                conflicto = False
                if gap < t_necesario: conflicto = True
                elif get_canonical(prev['destino']) != get_canonical(ev['origen']) and gap <= 0: conflicto = True

                if conflicto:
                    if prev["evento"] != "Comercial" and ev["evento"] == "Comercial":
                        if prev.get("bus"):
                            prev["conductor"] = ""
                            eventos_sin_conductor.append(prev)
                        reales_limpios.pop()
                        continue 
                    elif prev["evento"] == "Comercial" and ev["evento"] != "Comercial":
                        if ev.get("bus"):
                            ev["conductor"] = ""
                            eventos_sin_conductor.append(ev)
                        ev_descartado = True
                        break
                    else:
                        if ev.get("bus"):
                            ev["conductor"] = ""
                            eventos_sin_conductor.append(ev)
                        ev_descartado = True
                        break
                else: break
            if not ev_descartado and ev["_fin"] >= ev["_ini"]:
                if reales_limpios:
                    prev_ok = reales_limpios[-1]
                    if (
                        str(prev_ok.get("evento", "")).strip().upper() == "PARADA"
                        and str(ev.get("evento", "")).strip().upper() == "PARADA"
                        and get_canonical(prev_ok.get("origen", "")) == get_canonical(ev.get("origen", ""))
                        and int(ev.get("_ini", 0) or 0) <= int(prev_ok.get("_fin", 0) or 0)
                    ):
                        prev_ok["_fin"] = max(int(prev_ok.get("_fin", 0) or 0), int(ev.get("_fin", 0) or 0))
                        # La espera es del conductor: mantener un único evento de parada continuo.
                        continue
                reales_limpios.append(ev)

        if not reales_limpios: continue

        # Regla dura: EventosCompletos no segmenta ni crea nuevos IDs.
        # La factibilidad debe venir resuelta desde Fase 2/Fase 3.
        if isinstance(turno_ref, dict) and ("inicio" in turno_ref) and ("fin" in turno_ref):
            dur_turno = duracion_minutos(int(turno_ref.get("inicio", 0) or 0), int(turno_ref.get("fin", 0) or 0))
        else:
            primer_ev_turno = reales_limpios[0]
            ultimo_ev_turno = reales_limpios[-1]
            t_ini_dep = obtener_tiempo_traslado(deposito, primer_ev_turno['origen'], primer_ev_turno['_ini'], gestor) if not es_deposito(primer_ev_turno['origen'], deposito) else 0
            ini_turno_eff = primer_ev_turno['_ini'] - t_ini_dep
            t_retorno_fin = 0
            if not es_deposito(ultimo_ev_turno['destino'], deposito):
                t_retorno_fin = max(0, int(obtener_tiempo_traslado(ultimo_ev_turno['destino'], deposito, ultimo_ev_turno['_fin'], gestor) or 0))
            fin_turno_eff = ultimo_ev_turno['_fin'] + t_retorno_fin
            dur_turno = fin_turno_eff - (ini_turno_eff - tiempo_toma)
        if dur_turno > limite_jornada_conductor:
            lim_ref = (turno_ref or {}).get("limite_jornada_aplicable", "") if isinstance(turno_ref, dict) else ""
            ini_ref = (turno_ref or {}).get("inicio", "") if isinstance(turno_ref, dict) else ""
            fin_ref = (turno_ref or {}).get("fin", "") if isinstance(turno_ref, dict) else ""
            raise ValueError(
                f"[EVENTOS COMPLETOS - REGLA DURA] Turno conductor {cid} excede límite de jornada "
                f"({dur_turno} > {limite_jornada_conductor}) y no se permite segmentación en exportación. "
                f"turno_ref[inicio={ini_ref}, fin={fin_ref}, limite={lim_ref}]"
            )
        segmentos = [reales_limpios]

        for seg in segmentos:
            if not any(str(ev.get("evento", "")).strip().upper() == "COMERCIAL" for ev in seg):
                for ev in seg:
                    if ev.get("bus"):
                        ev["conductor"] = ""
                        eventos_sin_conductor.append(ev)
                continue
            cid_seg = cid
            t_toma = tiempo_toma

            def _agregar_conector(ev_prev: Optional[Dict[str, Any]], ev_next: Optional[Dict[str, Any]], ini_min: int, fin_min: int, origen: str, destino: str, desc: str = "") -> None:
                def _normalizar_bus(ev: Optional[Dict[str, Any]]) -> Any:
                    if not ev:
                        return ""
                    bus_val = ev.get("bus")
                    if bus_val in (None, "", 0, "0"):
                        return ""
                    bus_txt = str(bus_val).strip()
                    if bus_txt.isdigit():
                        return int(bus_txt)
                    return bus_txt

                bus_prev = _normalizar_bus(ev_prev)
                bus_next = _normalizar_bus(ev_next)
                hay_bus = bool(bus_prev or bus_next)
                hay_cambio_bus = bool(bus_prev and bus_next and bus_prev != bus_next)
                duracion = int(fin_min) - int(ini_min)
                if duracion < 0:
                    duracion += 1440
                if get_canonical(origen) == get_canonical(destino):
                    origen_txt = str(origen or "").strip().upper()
                    destino_txt = str(destino or "").strip().upper()
                    conecta_deposito = ("DEPOSITO" in origen_txt) or ("DEPOSITO" in destino_txt)
                    # Permitir conectores depósito<->alias con duración positiva
                    # (son operativos y evitan huecos InS->primer comercial).
                    if not (conecta_deposito and duracion > 0):
                        return

                # En el inicio de jornada (InS -> primer evento), el traslado inicial
                # es del conductor y se modela como Desplazamiento.
                if ev_prev is None:
                    if duracion <= 0:
                        if get_canonical(origen) != get_canonical(destino):
                            linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, ini_min, origen, destino, desc=desc))
                        return
                    linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                    return

                # En el cierre de jornada (último evento -> FnS), si existe bus y no hay
                # cambio de bus, se modela como Vacio; de lo contrario Desplazamiento.
                if ev_next is None:
                    if duracion <= 0:
                        if get_canonical(origen) == get_canonical(destino):
                            return
                        # Cierre con 0 min y nodos distintos: usar Desplazamiento para evitar vacío inválido.
                        linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, ini_min, origen, destino, desc=desc))
                        return
                    if hay_bus and not hay_cambio_bus:
                        bus_conector = bus_prev or bus_next
                        if _vacio_existe_en_fase1(bus_conector, ini_min, fin_min, origen, destino):
                            linea_tiempo.append(_crear_evento_base_dict("Vacio", bus_conector, cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                        else:
                            linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                    else:
                        linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                    return

                # REGLA OPERATIVA:
                # - Si es el mismo bus, usar Vacio solo si existe en Fase 1;
                #   si no, usar Desplazamiento (sin bus).
                # - Si no hay duración efectiva, no se inventa conector.
                if hay_bus and not hay_cambio_bus and duracion > 0:
                    bus_conector = bus_prev or bus_next
                    if _vacio_existe_en_fase1(bus_conector, ini_min, fin_min, origen, destino):
                        linea_tiempo.append(_crear_evento_base_dict("Vacio", bus_conector, cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                    else:
                        linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                elif hay_bus and not hay_cambio_bus:
                    # Mismo bus y sin duración útil: no agregar nada.
                    return
                else:
                    linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, fin_min, origen, destino, desc=desc))
            
            turno_ini_ref = int((turno_ref or {}).get("inicio", -1) or -1)
            turno_fin_ref = int((turno_ref or {}).get("fin", -1) or -1)
            if turno_ini_ref >= 0 and turno_fin_ref >= 0:
                seg = [
                    ev for ev in seg
                    if evento_cubierto_por_turno(ev["_ini"], ev["_fin"], turno_ini_ref, turno_fin_ref, margen_min=0)
                ]
                if not seg:
                    continue

            # Regla: no permitir Parada pegada al inicio/fin de jornada.
            while seg and str(seg[0].get("evento", "")).strip().upper() == "PARADA":
                seg.pop(0)
            while seg and str(seg[-1].get("evento", "")).strip().upper() == "PARADA":
                seg.pop()
            if not seg:
                continue

            linea_tiempo = []
            primer_ev = seg[0]
            
            if turno_ini_ref >= 0:
                ins_ini = turno_ini_ref
                if es_deposito(primer_ev['origen'], deposito):
                    # Regla: no crear Parada inmediatamente después de InS.
                    # Si el primer evento parte en depósito, extender InS hasta ese primer evento.
                    ins_fin = primer_ev["_ini"]
                else:
                    ins_fin = min(primer_ev["_ini"], turno_ini_ref + t_toma)
                if ins_fin < ins_ini:
                    ins_fin = ins_ini
                linea_tiempo.append(
                    _crear_evento_base_dict("InS", "", cid_seg, ins_ini, ins_fin, deposito, deposito, desc="Inicio Jornada")
                )
                if not es_deposito(primer_ev['origen'], deposito):
                    _agregar_conector(None, primer_ev, ins_fin, primer_ev['_ini'], deposito, primer_ev['origen'], desc=f"A {primer_ev['origen']}")
            elif es_deposito(primer_ev['origen'], deposito):
                ini_jornada = primer_ev['_ini']
                linea_tiempo.append(_crear_evento_base_dict("InS", "", cid_seg, max(0, ini_jornada - t_toma), ini_jornada, deposito, deposito, desc="Inicio Jornada"))
            else:
                t = obtener_tiempo_traslado(deposito, primer_ev['origen'], primer_ev['_ini'], gestor)
                ini_jornada = primer_ev['_ini'] - t
                linea_tiempo.append(_crear_evento_base_dict("InS", "", cid_seg, max(0, ini_jornada - t_toma), ini_jornada, deposito, deposito, desc="Inicio Jornada"))
                _agregar_conector(None, primer_ev, ini_jornada, primer_ev['_ini'], deposito, primer_ev['origen'], desc=f"A {primer_ev['origen']}")

            for i, ev in enumerate(seg):
                ev["conductor"] = cid_seg

                def _agregar_espera_o_corte(inicio_espera: int, fin_espera: int, nodo_espera: str) -> None:
                    if fin_espera <= inicio_espera:
                        return
                    dur_espera = fin_espera - inicio_espera
                    if dur_espera > umbral_parada_larga and es_deposito(nodo_espera, deposito):
                        # Regla dura: espera larga => corte de turno (sin Parada asignada).
                        linea_tiempo.append(
                            _crear_evento_base_dict(
                                "FnS", "", cid_seg, inicio_espera, inicio_espera, nodo_espera, nodo_espera, desc="Fin Jornada (corte parada larga)"
                            )
                        )
                        ins_ini = inicio_espera
                        linea_tiempo.append(
                            _crear_evento_base_dict(
                                "InS", "", cid_seg, ins_ini, fin_espera, nodo_espera, nodo_espera, desc="Inicio Jornada (corte parada larga)"
                            )
                        )
                        return
                    # Construcción de origen: si ya existe una Parada contigua/solapada
                    # del mismo conductor en el mismo nodo, ampliar esa misma espera.
                    if linea_tiempo:
                        ult = linea_tiempo[-1]
                        if (
                            str(ult.get("evento", "")).strip().upper() == "PARADA"
                            and get_canonical(ult.get("origen", "")) == get_canonical(nodo_espera)
                        ):
                            ult_ini = int(ult.get("_ini", 0) or 0)
                            ult_fin = int(ult.get("_fin", 0) or 0)
                            if inicio_espera <= ult_fin and fin_espera >= ult_ini:
                                ult["_ini"] = min(ult_ini, inicio_espera)
                                ult["_fin"] = max(ult_fin, fin_espera)
                                return
                            if inicio_espera == ult_fin:
                                ult["_fin"] = fin_espera
                                return
                    linea_tiempo.append(
                        _crear_evento_base_dict(
                            "Parada", "", cid_seg, inicio_espera, fin_espera, nodo_espera, nodo_espera, desc="Espera"
                        )
                    )

                if i > 0:
                    prev = seg[i-1]
                    gap = ev['_ini'] - prev['_fin']
                    o_can = get_canonical(prev['destino'])
                    d_can = get_canonical(ev['origen'])
                    
                    if gap > 0:
                        if o_can == d_can:
                            hab, tm = _cached_buscar_info_desplazamiento(prev['destino'], ev['origen'], prev['_fin'], gestor)
                            t = int(tm) if (hab and tm and int(tm) > 0) else 0
                            if t <= 0 and prev['destino'].upper() != ev['origen'].upper():
                                # Fallback robusto: si hay desplazamiento/vacío configurado con alias
                                # distintos, usar tiempo de conectividad consolidado.
                                t = int(obtener_tiempo_traslado(prev['destino'], ev['origen'], prev['_fin'], gestor) or 0)
                            if t > 0 and gap >= t and prev['destino'].upper() != ev['origen'].upper():
                                if gap > t:
                                    _agregar_espera_o_corte(prev["_fin"], ev["_ini"] - t, prev["destino"])
                                _agregar_conector(prev, ev, ev['_ini'] - t, ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                            elif prev['destino'].upper() != ev['origen'].upper():
                                # Último fallback: conector de 0 min para dejar continuidad explícita
                                # y evitar teletransporte lógico cuando faltan tiempos.
                                _agregar_conector(prev, ev, ev['_ini'], ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                            else:
                                _agregar_espera_o_corte(prev["_fin"], ev["_ini"], prev["destino"])
                        else:
                            t = obtener_tiempo_traslado(prev['destino'], ev['origen'], prev['_fin'], gestor)
                            if t > 0 and gap >= t:
                                if gap > t:
                                    _agregar_espera_o_corte(prev["_fin"], ev["_ini"] - t, prev["destino"])
                                _agregar_conector(prev, ev, ev['_ini'] - t, ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                            else:
                                # Sin traslado válido configurado entre nodos distintos:
                                # no inventar duración. Se deja el hueco como parada.
                                _agregar_espera_o_corte(prev["_fin"], ev["_ini"], prev["destino"])
                    elif gap == 0 and o_can != d_can:
                        _agregar_conector(prev, ev, ev['_ini'], ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                if linea_tiempo:
                    ult_lt = linea_tiempo[-1]
                    if (
                        str(ult_lt.get("evento", "")).strip().upper() == "PARADA"
                        and str(ev.get("evento", "")).strip().upper() == "PARADA"
                        and get_canonical(ult_lt.get("origen", "")) == get_canonical(ev.get("origen", ""))
                        and int(ev.get("_ini", 0) or 0) <= int(ult_lt.get("_fin", 0) or 0)
                    ):
                        ult_lt["_fin"] = max(int(ult_lt.get("_fin", 0) or 0), int(ev.get("_fin", 0) or 0))
                        continue
                linea_tiempo.append(ev)

            ultimo_ev = seg[-1]
            if turno_fin_ref >= 0:
                fin_jornada = turno_fin_ref
                if not es_deposito(ultimo_ev['destino'], deposito):
                    t_retorno = max(0, int(obtener_tiempo_traslado(ultimo_ev['destino'], deposito, ultimo_ev['_fin'], gestor) or 0))
                    # Si la conectividad canónica devolvió 0 pero el texto de nodos difiere,
                    # buscar tiempo configurado estricto (desplazamiento/vacío) para no perder
                    # conectores operativos cuando existen alias/sinónimos de nodos.
                    if t_retorno <= 0:
                        dest_txt = str(ultimo_ev.get('destino', '') or '').strip().upper()
                        dep_txt = str(deposito or '').strip().upper()
                        if dest_txt and dep_txt and dest_txt != dep_txt:
                            hab_alias, t_alias = _cached_buscar_info_desplazamiento(
                                ultimo_ev['destino'],
                                deposito,
                                ultimo_ev['_fin'],
                                gestor,
                            )
                            if hab_alias and t_alias is not None and int(t_alias) > 0:
                                t_retorno = int(t_alias)
                            else:
                                t_vac_alias, _ = _cached_buscar_tiempo_vacio(
                                    ultimo_ev['destino'],
                                    deposito,
                                    ultimo_ev['_fin'],
                                    gestor,
                                )
                                if t_vac_alias is not None and int(t_vac_alias) > 0:
                                    t_retorno = int(t_vac_alias)
                    fin_con_retorno = ultimo_ev['_fin'] + t_retorno
                    if t_retorno > 0:
                        _agregar_conector(ultimo_ev, None, ultimo_ev['_fin'], fin_con_retorno, ultimo_ev['destino'], deposito, desc=f"A {deposito}")
                    else:
                        _agregar_conector(ultimo_ev, None, ultimo_ev['_fin'], ultimo_ev['_fin'], ultimo_ev['destino'], deposito, desc=f"A {deposito} (sin ruta configurada)")
                    # Si no hay traslado efectivo (0), no forzar espera hasta turno_fin_ref.
                    # Evita huecos Comercial->FnS por alias de depósito.
                    fin_jornada = fin_con_retorno if t_retorno == 0 else max(fin_jornada, fin_con_retorno)
                else:
                    # Regla: no crear Parada inmediatamente antes de FnS.
                    # Si el último evento ya termina en depósito, FnS cierra al fin real.
                    fin_jornada = ultimo_ev['_fin']
                linea_tiempo.append(
                    _crear_evento_base_dict(
                        "FnS",
                        "",
                        cid_seg,
                        fin_jornada,
                        fin_jornada,
                        deposito,
                        deposito,
                        desc="Fin Jornada",
                    )
                )
            else:
                fin_jornada = ultimo_ev['_fin']
                if not es_deposito(ultimo_ev['destino'], deposito):
                    t = max(0, int(obtener_tiempo_traslado(ultimo_ev['destino'], deposito, ultimo_ev['_fin'], gestor) or 0))
                    fin_con_retorno = ultimo_ev['_fin'] + t
                    if t > 0:
                        _agregar_conector(ultimo_ev, None, ultimo_ev['_fin'], fin_con_retorno, ultimo_ev['destino'], deposito, desc=f"A {deposito}")
                    else:
                        # Sin ruta configurada: dejar trazabilidad explícita del traslado al cierre.
                        linea_tiempo.append(
                            _crear_evento_base_dict(
                                "Desplazamiento",
                                "",
                                cid_seg,
                                ultimo_ev['_fin'],
                                ultimo_ev['_fin'],
                                ultimo_ev['destino'],
                                deposito,
                                desc=f"A {deposito} (sin ruta configurada)",
                            )
                        )
                    fin_jornada = fin_con_retorno
                    linea_tiempo.append(
                        _crear_evento_base_dict(
                            "FnS",
                            "",
                            cid_seg,
                            fin_jornada,
                            fin_jornada,
                            deposito,
                            deposito,
                            desc="Fin Jornada",
                        )
                    )
                else:
                    linea_tiempo.append(
                        _crear_evento_base_dict(
                            "FnS",
                            "",
                            cid_seg,
                            fin_jornada,
                            fin_jornada,
                            deposito,
                            deposito,
                            desc="Fin Jornada",
                        )
                    )
            # Ajuste fino para no exceder límite de jornada sin truncar conectores configurados:
            # si hay exceso pequeño, se recorta primero desde el inicio de InS (tiempo de toma).
            if linea_tiempo:
                min_ini_seg = min(int(e.get("_ini", 0) or 0) for e in linea_tiempo)
                max_fin_seg = max(int(e.get("_fin", 0) or 0) for e in linea_tiempo)
                dur_seg = duracion_minutos(min_ini_seg, max_fin_seg)
                if dur_seg > limite_jornada_conductor:
                    exceso = dur_seg - limite_jornada_conductor
                    for e in linea_tiempo:
                        if str(e.get("evento", "")).strip().upper() == "INS":
                            ins_ini = int(e.get("_ini", 0) or 0)
                            ins_fin = int(e.get("_fin", 0) or 0)
                            disponible = max(0, ins_fin - ins_ini)
                            mover = min(exceso, disponible)
                            if mover > 0:
                                e["_ini"] = ins_ini + mover
                                exceso -= mover
                            break
            eventos_casi_finales.extend(linea_tiempo)

    validar_comerciales_todos_asignados(eventos_base)

    # REGLA DURA: todo VACIO debe quedar asignado a un conductor.
    # Intento de rescate por continuidad operacional en el mismo bus.
    sin_conductor_vacio = [e for e in eventos_sin_conductor if str(e.get("evento", "")).strip().upper() == "VACIO"]
    if sin_conductor_vacio:
        eventos_por_bus: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        for ev_cf in eventos_casi_finales:
            bus_cf = ev_cf.get("bus")
            cond_cf = ev_cf.get("conductor")
            if bus_cf in (None, "", 0, "0") or cond_cf in (None, ""):
                continue
            key_cf = _bus_key_norm(bus_cf)
            if not key_cf:
                continue
            eventos_por_bus[key_cf].append(ev_cf)
        for k in list(eventos_por_bus.keys()):
            eventos_por_bus[k].sort(key=lambda x: (int(x.get("_ini", 0) or 0), int(x.get("_fin", 0) or 0)))

        def _promover_desplazamiento_a_vacio(cid: Any, v_ev: Dict[str, Any], bus_key: str) -> bool:
            v_ini = int(v_ev.get("_ini", 0) or 0)
            v_fin = int(v_ev.get("_fin", 0) or 0)
            v_o = get_canonical(v_ev.get("origen", ""))
            v_d = get_canonical(v_ev.get("destino", ""))
            tol = 5
            candidato = None
            mejor_delta = 10**9
            for ex in eventos_casi_finales:
                if str(ex.get("conductor", "")) != str(cid):
                    continue
                ex_tipo = str(ex.get("evento", "")).strip().upper()
                ex_ini = int(ex.get("_ini", 0) or 0)
                ex_fin = int(ex.get("_fin", 0) or 0)
                ex_o = get_canonical(ex.get("origen", ""))
                ex_d = get_canonical(ex.get("destino", ""))
                if abs(ex_ini - v_ini) > tol or abs(ex_fin - v_fin) > tol:
                    continue
                if ex_o != v_o or ex_d != v_d:
                    continue
                if ex_tipo == "VACIO" and _bus_key_norm(ex.get("bus", "")) == bus_key:
                    return True
                # Caso robusto: desplazamiento del conductor cubre totalmente el vacío
                # operativo del bus. Se parte en segmentos para no perder continuidad.
                if ex_tipo == "DESPLAZAMIENTO" and ex_ini <= v_ini and ex_fin >= v_fin:
                    before_ini = ex_ini
                    before_fin = v_ini
                    after_ini = v_fin
                    after_fin = ex_fin
                    ex["evento"] = "Vacio"
                    ex["bus"] = v_ev.get("bus", ex.get("bus", ""))
                    ex["_ini"] = v_ini
                    ex["_fin"] = v_fin
                    ex["origen"] = v_ev.get("origen", ex.get("origen", ""))
                    ex["destino"] = v_ev.get("destino", ex.get("destino", ""))
                    if v_ev.get("desc"):
                        ex["desc"] = v_ev.get("desc")
                    if v_ev.get("kilometros") not in (None, ""):
                        ex["kilometros"] = v_ev.get("kilometros")
                    if before_fin > before_ini:
                        eventos_casi_finales.append(
                            _crear_evento_base_dict(
                                "Desplazamiento",
                                "",
                                cid,
                                before_ini,
                                before_fin,
                                v_ev.get("origen", ""),
                                v_ev.get("origen", ""),
                                desc="Conexión conductor (previa)",
                            )
                        )
                    if after_fin > after_ini:
                        eventos_casi_finales.append(
                            _crear_evento_base_dict(
                                "Desplazamiento",
                                "",
                                cid,
                                after_ini,
                                after_fin,
                                v_ev.get("destino", ""),
                                v_ev.get("destino", ""),
                                desc="Conexión conductor (posterior)",
                            )
                        )
                    return True
                if ex_tipo == "DESPLAZAMIENTO":
                    delta = abs(ex_ini - v_ini) + abs(ex_fin - v_fin)
                    if delta < mejor_delta:
                        mejor_delta = delta
                        candidato = ex
            if candidato is None:
                return False
            candidato["evento"] = "Vacio"
            candidato["bus"] = v_ev.get("bus", candidato.get("bus", ""))
            if v_ev.get("desc"):
                candidato["desc"] = v_ev.get("desc")
            if v_ev.get("kilometros") not in (None, ""):
                candidato["kilometros"] = v_ev.get("kilometros")
            return True

        for vv in sin_conductor_vacio:
            bus_v = vv.get("bus")
            if bus_v in (None, "", 0, "0"):
                continue
            bus_key = _bus_key_norm(bus_v)
            if not bus_key:
                continue
            candidatos = eventos_por_bus.get(bus_key, [])
            if not candidatos:
                continue

            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            prev_ev = None
            next_ev = None
            for evb in candidatos:
                evb_fin = int(evb.get("_fin", 0) or 0)
                evb_ini = int(evb.get("_ini", 0) or 0)
                if evb_fin <= v_ini:
                    prev_ev = evb
                if next_ev is None and evb_ini >= v_fin:
                    next_ev = evb

            cid_rescate = None
            if prev_ev is not None and next_ev is not None and str(prev_ev.get("conductor", "")) == str(next_ev.get("conductor", "")):
                cid_test = prev_ev.get("conductor")
                if cid_test not in (None, ""):
                    cid_rescate = cid_test
            if cid_rescate is None and prev_ev is not None:
                cid_test = prev_ev.get("conductor")
                if cid_test not in (None, ""):
                    cid_rescate = cid_test
            if cid_rescate is None and next_ev is not None:
                cid_test = next_ev.get("conductor")
                if cid_test not in (None, ""):
                    cid_rescate = cid_test

            if cid_rescate not in (None, ""):
                if _promover_desplazamiento_a_vacio(cid_rescate, vv, bus_key):
                    vv["conductor"] = cid_rescate

        pendientes = [e for e in sin_conductor_vacio if not e.get("conductor")]
        def _hay_solape_conductor(cid: Any, ini: int, fin: int) -> bool:
            for ex in eventos_casi_finales:
                if str(ex.get("conductor", "")) != str(cid):
                    continue
                ex_ini = int(ex.get("_ini", 0) or 0)
                ex_fin = int(ex.get("_fin", 0) or 0)
                if not (fin <= ex_ini or ex_fin <= ini):
                    return True
            return False

        def _hay_solape_conductor_no_ins(cid: Any, ini: int, fin: int) -> bool:
            for ex in eventos_casi_finales:
                if str(ex.get("conductor", "")) != str(cid):
                    continue
                if str(ex.get("evento", "")).strip().upper() == "INS":
                    continue
                ex_ini = int(ex.get("_ini", 0) or 0)
                ex_fin = int(ex.get("_fin", 0) or 0)
                if not (fin <= ex_ini or ex_fin <= ini):
                    return True
            return False

        def _extender_fns(cid: Any, nuevo_fin: int) -> None:
            cands_fns = [
                e for e in eventos_casi_finales
                if str(e.get("conductor", "")) == str(cid)
                and str(e.get("evento", "")).strip().upper() == "FNS"
            ]
            if not cands_fns:
                return
            fns_ref = max(cands_fns, key=lambda x: int(x.get("_fin", 0) or 0))
            fin_act = int(fns_ref.get("_fin", 0) or 0)
            if nuevo_fin > fin_act:
                fns_ref["_ini"] = nuevo_fin
                fns_ref["_fin"] = nuevo_fin

        def _adelantar_ins(cid: Any, nuevo_ini_vacio: int) -> None:
            cands_ins = [
                e for e in eventos_casi_finales
                if str(e.get("conductor", "")) == str(cid)
                and str(e.get("evento", "")).strip().upper() == "INS"
            ]
            if not cands_ins:
                return
            ins_ref = min(cands_ins, key=lambda x: int(x.get("_ini", 0) or 0))
            t_toma_ref = int(tiempo_toma or 0)
            ins_ref["_fin"] = min(int(ins_ref.get("_fin", 0) or 0), int(nuevo_ini_vacio))
            ins_ref["_ini"] = min(int(ins_ref.get("_ini", 0) or 0), max(0, int(nuevo_ini_vacio) - t_toma_ref))
            if int(ins_ref["_fin"]) < int(ins_ref["_ini"]):
                ins_ref["_fin"] = ins_ref["_ini"]

        for vv in pendientes:
            bus_v = vv.get("bus")
            if bus_v in (None, "", 0, "0"):
                continue
            bus_key = _bus_key_norm(bus_v)
            if not bus_key:
                continue
            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            o_dep = es_deposito(vv.get("origen", ""), deposito)
            d_dep = es_deposito(vv.get("destino", ""), deposito)
            cands_bus = [
                e for e in eventos_casi_finales
                if _bus_key_norm(e.get("bus", "")) == bus_key
                and e.get("conductor") not in (None, "")
                and int(e.get("_fin", 0) or 0) <= v_ini
            ]
            cands_bus_next = [
                e for e in eventos_casi_finales
                if _bus_key_norm(e.get("bus", "")) == bus_key
                and e.get("conductor") not in (None, "")
                and int(e.get("_ini", 0) or 0) >= v_fin
            ]

            if cands_bus:
                ref = max(cands_bus, key=lambda x: int(x.get("_fin", 0) or 0))
                cid_ref = ref.get("conductor")
                if cid_ref not in (None, "") and _promover_desplazamiento_a_vacio(cid_ref, vv, bus_key):
                    vv["conductor"] = cid_ref
                    continue
                if cid_ref not in (None, "") and d_dep and not _hay_solape_conductor(cid_ref, v_ini, v_fin):
                    vv["conductor"] = cid_ref
                    eventos_casi_finales.append(vv)
                    _extender_fns(cid_ref, v_fin)
                    continue

            if cands_bus_next:
                refn = min(cands_bus_next, key=lambda x: int(x.get("_ini", 0) or 0))
                cid_next = refn.get("conductor")
                if cid_next not in (None, "") and _promover_desplazamiento_a_vacio(cid_next, vv, bus_key):
                    vv["conductor"] = cid_next
                    continue
                if cid_next not in (None, "") and o_dep and not _hay_solape_conductor_no_ins(cid_next, v_ini, v_fin):
                    vv["conductor"] = cid_next
                    eventos_casi_finales.append(vv)
                    _adelantar_ins(cid_next, v_ini)

        # Último recurso controlado por continuidad de nodo/tiempo (cuando no hay
        # coincidencia limpia por id_bus, p.ej. diferencias de tipado bus str/int).
        pendientes = [e for e in sin_conductor_vacio if not e.get("conductor")]
        for vv in pendientes:
            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            v_o = get_canonical(vv.get("origen", ""))
            v_d = get_canonical(vv.get("destino", ""))
            o_dep = es_deposito(vv.get("origen", ""), deposito)
            d_dep = es_deposito(vv.get("destino", ""), deposito)
            tol = 10
            if o_dep:
                cands_next = [
                    e for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                    and int(e.get("_ini", 0) or 0) >= v_fin
                    and int(e.get("_ini", 0) or 0) - v_fin <= tol
                    and get_canonical(e.get("origen", "")) == v_d
                ]
                if cands_next:
                    refn = min(cands_next, key=lambda x: int(x.get("_ini", 0) or 0))
                    cid_next = refn.get("conductor")
                    if cid_next not in (None, "") and not _hay_solape_conductor_no_ins(cid_next, v_ini, v_fin):
                        vv["conductor"] = cid_next
                        eventos_casi_finales.append(vv)
                        _adelantar_ins(cid_next, v_ini)
                        continue
            if d_dep:
                cands_prev = [
                    e for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                    and int(e.get("_fin", 0) or 0) <= v_ini
                    and v_ini - int(e.get("_fin", 0) or 0) <= tol
                    and get_canonical(e.get("destino", "")) == v_o
                ]
                if cands_prev:
                    refp = max(cands_prev, key=lambda x: int(x.get("_fin", 0) or 0))
                    cid_prev = refp.get("conductor")
                    if cid_prev not in (None, "") and not _hay_solape_conductor(cid_prev, v_ini, v_fin):
                        vv["conductor"] = cid_prev
                        eventos_casi_finales.append(vv)
                        _extender_fns(cid_prev, v_fin)

        # Fallback sin límite de ventana: asignar por continuidad de nodo aunque
        # exista una espera larga (se explicita con Parada para evitar huecos).
        pendientes = [e for e in sin_conductor_vacio if not e.get("conductor")]
        for vv in pendientes:
            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            v_o = get_canonical(vv.get("origen", ""))
            v_d = get_canonical(vv.get("destino", ""))
            o_dep = es_deposito(vv.get("origen", ""), deposito)
            d_dep = es_deposito(vv.get("destino", ""), deposito)
            if o_dep:
                cands_next_any = [
                    e for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                    and int(e.get("_ini", 0) or 0) >= v_fin
                    and get_canonical(e.get("origen", "")) == v_d
                ]
                if cands_next_any:
                    refn = min(cands_next_any, key=lambda x: int(x.get("_ini", 0) or 0))
                    cid_next = refn.get("conductor")
                    ini_ref = int(refn.get("_ini", 0) or 0)
                    if cid_next not in (None, "") and not _hay_solape_conductor_no_ins(cid_next, v_ini, v_fin):
                        vv["conductor"] = cid_next
                        eventos_casi_finales.append(vv)
                        if ini_ref > v_fin:
                            eventos_casi_finales.append(
                                _crear_evento_base_dict("Parada", "", cid_next, v_fin, ini_ref, vv.get("destino", ""), vv.get("destino", ""), desc="Espera")
                            )
                        _adelantar_ins(cid_next, v_ini)
                        continue
            if d_dep:
                cands_prev_any = [
                    e for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                    and int(e.get("_fin", 0) or 0) <= v_ini
                    and get_canonical(e.get("destino", "")) == v_o
                ]
                if cands_prev_any:
                    refp = max(cands_prev_any, key=lambda x: int(x.get("_fin", 0) or 0))
                    cid_prev = refp.get("conductor")
                    fin_ref = int(refp.get("_fin", 0) or 0)
                    if cid_prev not in (None, "") and not _hay_solape_conductor(cid_prev, v_ini, v_fin):
                        if v_ini > fin_ref:
                            eventos_casi_finales.append(
                                _crear_evento_base_dict("Parada", "", cid_prev, fin_ref, v_ini, vv.get("origen", ""), vv.get("origen", ""), desc="Espera")
                            )
                        vv["conductor"] = cid_prev
                        eventos_casi_finales.append(vv)
                        _extender_fns(cid_prev, v_fin)

        # Último intento: usar continuidad en eventos_base (antes de limpieza por conflictos),
        # para no perder vacíos operativos válidos del bus.
        pendientes = [e for e in sin_conductor_vacio if not e.get("conductor")]
        base_por_bus: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        for eb in eventos_base:
            if eb.get("conductor") in (None, ""):
                continue
            bus_b = eb.get("bus")
            if bus_b in (None, "", 0, "0"):
                continue
            key_b = _bus_key_norm(bus_b)
            if not key_b:
                continue
            base_por_bus[key_b].append(eb)
        for k in list(base_por_bus.keys()):
            base_por_bus[k].sort(key=lambda x: (int(x.get("_ini", 0) or 0), int(x.get("_fin", 0) or 0)))

        for vv in pendientes:
            if vv.get("conductor") not in (None, ""):
                continue
            bus_v = vv.get("bus")
            if bus_v in (None, "", 0, "0"):
                continue
            bus_key = _bus_key_norm(bus_v)
            if not bus_key:
                continue
            cands = base_por_bus.get(bus_key, [])
            if not cands:
                continue
            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            o_dep = es_deposito(vv.get("origen", ""), deposito)
            d_dep = es_deposito(vv.get("destino", ""), deposito)
            prev_base = None
            next_base = None
            for eb in cands:
                eb_ini = int(eb.get("_ini", 0) or 0)
                eb_fin = int(eb.get("_fin", 0) or 0)
                if eb_fin <= v_ini:
                    prev_base = eb
                if next_base is None and eb_ini >= v_fin:
                    next_base = eb
            cid_pick = None
            if o_dep and next_base is not None:
                cid_pick = next_base.get("conductor")
            elif d_dep and prev_base is not None:
                cid_pick = prev_base.get("conductor")
            elif next_base is not None:
                cid_pick = next_base.get("conductor")
            elif prev_base is not None:
                cid_pick = prev_base.get("conductor")
            if cid_pick in (None, ""):
                continue
            if _promover_desplazamiento_a_vacio(cid_pick, vv, bus_key):
                vv["conductor"] = cid_pick
                continue
            if _hay_solape_conductor_no_ins(cid_pick, v_ini, v_fin):
                continue
            vv["conductor"] = cid_pick
            eventos_casi_finales.append(vv)
            if o_dep:
                _adelantar_ins(cid_pick, v_ini)
            if d_dep:
                _extender_fns(cid_pick, v_fin)

        # Último cierre determinista: asignar al conductor más cercano del mismo bus.
        pendientes = [e for e in sin_conductor_vacio if not e.get("conductor")]
        for vv in pendientes:
            bus_v = vv.get("bus")
            bus_key = _bus_key_norm(bus_v)
            if not bus_key:
                continue
            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            o_dep = es_deposito(vv.get("origen", ""), deposito)
            d_dep = es_deposito(vv.get("destino", ""), deposito)
            cands_same_bus = [
                e for e in eventos_base
                if e.get("conductor") not in (None, "")
                and _bus_key_norm(e.get("bus", "")) == bus_key
            ]
            if not cands_same_bus:
                continue
            mejor_cid = None
            mejor_score = 10**9
            for evc in cands_same_bus:
                cid = evc.get("conductor")
                if cid in (None, ""):
                    continue
                ini_c = int(evc.get("_ini", 0) or 0)
                fin_c = int(evc.get("_fin", 0) or 0)
                if fin_c <= v_ini:
                    score = v_ini - fin_c
                elif ini_c >= v_fin:
                    score = ini_c - v_fin
                else:
                    score = 0
                if score < mejor_score:
                    mejor_score = score
                    mejor_cid = cid
            if mejor_cid in (None, ""):
                continue
            if _promover_desplazamiento_a_vacio(mejor_cid, vv, bus_key):
                vv["conductor"] = mejor_cid
                continue
            if _hay_solape_conductor_no_ins(mejor_cid, v_ini, v_fin):
                continue
            vv["conductor"] = mejor_cid
            eventos_casi_finales.append(vv)
            if o_dep:
                _adelantar_ins(mejor_cid, v_ini)
            if d_dep:
                _extender_fns(mejor_cid, v_fin)

        # Fallback final absoluto: continuidad por nodo/tiempo aunque cambie bus.
        pendientes = [e for e in sin_conductor_vacio if not e.get("conductor")]
        for vv in pendientes:
            v_ini = int(vv.get("_ini", 0) or 0)
            v_fin = int(vv.get("_fin", 0) or 0)
            v_o = get_canonical(vv.get("origen", ""))
            v_d = get_canonical(vv.get("destino", ""))
            o_dep = es_deposito(vv.get("origen", ""), deposito)
            d_dep = es_deposito(vv.get("destino", ""), deposito)
            tol = 90
            cid_pick = None
            if o_dep:
                cands_next_any = [
                    e for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                    and int(e.get("_ini", 0) or 0) >= v_fin
                    and int(e.get("_ini", 0) or 0) - v_fin <= tol
                    and get_canonical(e.get("origen", "")) == v_d
                ]
                if cands_next_any:
                    refn = min(cands_next_any, key=lambda x: int(x.get("_ini", 0) or 0))
                    cid_pick = refn.get("conductor")
                    if cid_pick not in (None, "") and not _hay_solape_conductor_no_ins(cid_pick, v_ini, v_fin):
                        vv["conductor"] = cid_pick
                        eventos_casi_finales.append(vv)
                        _adelantar_ins(cid_pick, v_ini)
                        continue
            if d_dep:
                cands_prev_any = [
                    e for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                    and int(e.get("_fin", 0) or 0) <= v_ini
                    and v_ini - int(e.get("_fin", 0) or 0) <= tol
                    and get_canonical(e.get("destino", "")) == v_o
                ]
                if cands_prev_any:
                    refp = max(cands_prev_any, key=lambda x: int(x.get("_fin", 0) or 0))
                    cid_pick = refp.get("conductor")
                    if cid_pick not in (None, "") and not _hay_solape_conductor(cid_pick, v_ini, v_fin):
                        vv["conductor"] = cid_pick
                        eventos_casi_finales.append(vv)
                        _extender_fns(cid_pick, v_fin)

        sin_conductor_vacio = [e for e in sin_conductor_vacio if not e.get("conductor")]
        if sin_conductor_vacio:
            # Cierre forzado para garantizar solución viable:
            # asignar cada vacío pendiente al conductor más cercano, removiendo
            # solo conflictos no-comerciales en ese intervalo.
            def _solapa(a_ini: int, a_fin: int, b_ini: int, b_fin: int) -> bool:
                return not (a_fin <= b_ini or b_fin <= a_ini)

            def _limite_cid(cid: Any) -> int:
                return int(
                    (limites_por_conductor or {}).get(
                        cid,
                        (limites_por_conductor or {}).get(str(cid), limite_jornada_final),
                    )
                    or limite_jornada_final
                )

            def _jornada_resultante_ok(cid: Any, add_ini: int, add_fin: int, anticipa_ins: bool, extiende_fns: bool) -> bool:
                evs = [e for e in eventos_casi_finales if str(e.get("conductor", "")) == str(cid)]
                if evs:
                    min_ini = min(int(e.get("_ini", 0) or 0) for e in evs)
                    max_fin = max(int(e.get("_fin", 0) or 0) for e in evs)
                else:
                    min_ini = add_ini
                    max_fin = add_fin
                if anticipa_ins:
                    min_ini = min(min_ini, max(0, add_ini - int(tiempo_toma or 0)))
                else:
                    min_ini = min(min_ini, add_ini)
                if extiende_fns:
                    max_fin = max(max_fin, add_fin)
                else:
                    max_fin = max(max_fin, add_fin)
                return duracion_minutos(min_ini, max_fin) <= _limite_cid(cid)

            conductores_todos = sorted(
                {
                    e.get("conductor")
                    for e in eventos_casi_finales
                    if e.get("conductor") not in (None, "")
                },
                key=lambda x: int(x) if str(x).isdigit() else str(x),
            )
            for vv in [e for e in sin_conductor_vacio if not e.get("conductor")]:
                bus_key = _bus_key_norm(vv.get("bus"))
                v_ini = int(vv.get("_ini", 0) or 0)
                v_fin = int(vv.get("_fin", 0) or 0)
                o_dep = es_deposito(vv.get("origen", ""), deposito)
                d_dep = es_deposito(vv.get("destino", ""), deposito)

                cands_bus = [
                    e.get("conductor")
                    for e in eventos_base
                    if e.get("conductor") not in (None, "")
                    and _bus_key_norm(e.get("bus")) == bus_key
                ]
                candidatos = []
                for cid in cands_bus + conductores_todos:
                    if cid in (None, ""):
                        continue
                    if cid not in candidatos:
                        candidatos.append(cid)

                asignado = False
                for cid in candidatos:
                    if not _jornada_resultante_ok(cid, v_ini, v_fin, o_dep, d_dep):
                        continue
                    conflictos = []
                    conflicto_comercial = False
                    for ex in eventos_casi_finales:
                        if str(ex.get("conductor", "")) != str(cid):
                            continue
                        ex_ini = int(ex.get("_ini", 0) or 0)
                        ex_fin = int(ex.get("_fin", 0) or 0)
                        if not _solapa(v_ini, v_fin, ex_ini, ex_fin):
                            continue
                        if str(ex.get("evento", "")).strip().upper() == "COMERCIAL":
                            conflicto_comercial = True
                            break
                        conflictos.append(ex)
                    if conflicto_comercial:
                        continue
                    if conflictos:
                        eventos_casi_finales[:] = [e for e in eventos_casi_finales if e not in conflictos]
                    vv["conductor"] = cid
                    eventos_casi_finales.append(vv)
                    if o_dep:
                        _adelantar_ins(cid, v_ini)
                    if d_dep:
                        _extender_fns(cid, v_fin)
                    asignado = True
                    break
                if not asignado:
                    vv["conductor"] = ""

            sin_conductor_vacio = [e for e in sin_conductor_vacio if not e.get("conductor")]
        if sin_conductor_vacio:
            muestra = "\n".join(
                f"  - bus={e.get('bus','')} {e.get('origen','')}->{e.get('destino','')} {e.get('inicio','')}-{e.get('fin','')}"
                for e in sin_conductor_vacio[:10]
            )
            if len(sin_conductor_vacio) > 10:
                muestra += f"\n  ... y {len(sin_conductor_vacio) - 10} más."
            raise ValueError(
                "[EVENTOS COMPLETOS - REGLA DURA] Existen vacíos sin conductor. "
                "Todos los VACIO deben quedar asignados.\n" + muestra
            )

    # Solo añadir al resultado eventos sin conductor que NO sean Comercial ni Vacio.
    eventos_sin_conductor_ok = [e for e in eventos_sin_conductor if str(e.get("evento", "")).strip().upper() not in ("COMERCIAL", "VACIO")]
    eventos_casi_finales.extend(eventos_sin_conductor_ok)

    prioridad_tipo_pre = {"InS": 0, "Desplazamiento": 1, "Vacio": 2, "Comercial": 3, "Parada": 4, "FnS": 9}

    # Regla dura: ninguna Parada larga puede quedar asociada a conductor.
    # Se transforma en corte explícito FnS -> InS (misma marca de tiempo inicial).
    eventos_sin_parada_larga: List[Dict[str, Any]] = []
    for ev in eventos_casi_finales:
        if str(ev.get("evento", "")).strip().upper() == "PARADA" and ev.get("conductor") not in (None, ""):
            ini_ev = int(ev.get("_ini", 0) or 0)
            fin_ev = int(ev.get("_fin", 0) or 0)
            if (fin_ev - ini_ev) > umbral_parada_larga:
                nodo_ev = ev.get("origen", "") or ev.get("destino", "") or deposito
                cid_ev = ev.get("conductor", "")
                if es_deposito(nodo_ev, deposito):
                    eventos_sin_parada_larga.append(
                        _crear_evento_base_dict("FnS", "", cid_ev, ini_ev, ini_ev, deposito, deposito, desc="Fin Jornada (corte parada larga)")
                    )
                    eventos_sin_parada_larga.append(
                        _crear_evento_base_dict("InS", "", cid_ev, ini_ev, fin_ev, deposito, deposito, desc="Inicio Jornada (corte parada larga)")
                    )
                else:
                    # Si no es depósito, evitar FnS/InS fuera de depósito:
                    # trocear la parada larga en dos paradas <= umbral separadas por un
                    # marcador de conexión (no-parada) sin duración.
                    corte = min(fin_ev, ini_ev + umbral_parada_larga)
                    eventos_sin_parada_larga.append(
                        _crear_evento_base_dict("Parada", ev.get("bus", ""), cid_ev, ini_ev, corte, nodo_ev, nodo_ev, desc=ev.get("desc", "Parada"))
                    )
                    if fin_ev > corte:
                        eventos_sin_parada_larga.append(
                            _crear_evento_base_dict("Desplazamiento", "", cid_ev, corte, corte, nodo_ev, nodo_ev, desc="Corte de parada larga")
                        )
                        eventos_sin_parada_larga.append(
                            _crear_evento_base_dict("Parada", ev.get("bus", ""), cid_ev, corte, fin_ev, nodo_ev, nodo_ev, desc=ev.get("desc", "Parada"))
                        )
                continue
        eventos_sin_parada_larga.append(ev)
    eventos_casi_finales = eventos_sin_parada_larga

    # Cierre de huecos residuales por conductor sin romper reglas:
    # - Si el hueco es tras InS: extender InS hasta el siguiente evento.
    # - Si el hueco es antes de FnS: mover FnS al fin del evento previo.
    # - En medio: insertar Parada de espera.
    eventos_casi_finales.sort(
        key=lambda e: (
            str(e.get("conductor", "")),
            int(e.get("_ini", 0) or 0),
            int(e.get("_fin", 0) or 0),
            prioridad_tipo_pre.get((e.get("evento") or "").strip(), 5),
        )
    )
    cerrados: List[Dict[str, Any]] = []
    i = 0
    while i < len(eventos_casi_finales):
        curr = eventos_casi_finales[i]
        cerrados.append(curr)
        if i + 1 >= len(eventos_casi_finales):
            i += 1
            continue
        nxt = eventos_casi_finales[i + 1]
        if str(curr.get("conductor", "")) != str(nxt.get("conductor", "")):
            i += 1
            continue
        fin_curr = int(curr.get("_fin", 0) or 0)
        ini_nxt = int(nxt.get("_ini", 0) or 0)
        if ini_nxt > fin_curr:
            t_curr = str(curr.get("evento", "")).strip().upper()
            t_next = str(nxt.get("evento", "")).strip().upper()
            if t_curr == "INS":
                curr["_fin"] = ini_nxt
            elif t_curr == "FNS" and t_next == "INS":
                # Corte explícito entre jornadas del mismo ID lógico:
                # no insertar "Espera" entre FnS -> InS.
                pass
            elif (
                (ini_nxt - fin_curr) > umbral_parada_larga
                and es_deposito(curr.get("destino", ""), deposito)
                and es_deposito(nxt.get("origen", ""), deposito)
            ):
                # Regla operativa: una espera larga en depósito debe representarse
                # como corte de jornada (FnS/InS), nunca como Parada del conductor.
                cid = curr.get("conductor", "")
                if t_curr != "FNS":
                    cerrados.append(
                        _crear_evento_base_dict(
                            "FnS", "", cid, fin_curr, fin_curr, deposito, deposito, desc="Fin Jornada"
                        )
                    )
                if t_next != "INS":
                    ins_ini = fin_curr
                    cerrados.append(
                        _crear_evento_base_dict(
                            "InS", "", cid, ins_ini, ini_nxt, deposito, deposito, desc="Inicio Jornada"
                        )
                    )
            elif t_next == "FNS":
                nxt["_ini"] = fin_curr
                nxt["_fin"] = fin_curr
            else:
                # Construcción de origen: si ya venimos en Parada en el mismo nodo,
                # extender esa misma espera para no crear una nueva parada consecutiva.
                if (
                    t_curr == "PARADA"
                    and get_canonical(curr.get("destino", "")) == get_canonical(nxt.get("origen", ""))
                ):
                    curr["_fin"] = ini_nxt
                else:
                    cerrados.append(
                        _crear_evento_base_dict(
                            "Parada",
                            "",
                            curr.get("conductor", ""),
                            fin_curr,
                            ini_nxt,
                            curr.get("destino", ""),
                            curr.get("destino", ""),
                            desc="Espera",
                        )
                    )
        i += 1
    eventos_casi_finales = cerrados

    # Reconciliación final de conectividad:
    # si dos eventos consecutivos del mismo conductor "empalman" en el tiempo
    # pero cambian de nodo, insertar conexión explícita (priorizando Desplazamiento
    # configurado). Evita teletransportes al asignar VACIOs en etapas tardías.
    def _canon_nodo_auditoria(nodo: Any) -> str:
        n = str(nodo or "").strip().upper().replace("DEPÓSITO", "DEPOSITO")
        n = n.replace("DEPOSITO", "").strip()
        return " ".join(n.split())

    prioridad_reconc = {"InS": 0, "Desplazamiento": 1, "Vacio": 2, "Comercial": 3, "Parada": 4, "FnS": 9}
    eventos_casi_finales.sort(
        key=lambda e: (
            str(e.get("conductor", "")),
            int(e.get("_ini", 0) or 0),
            int(e.get("_fin", 0) or 0),
            prioridad_reconc.get((e.get("evento") or "").strip(), 5),
        )
    )
    reconciliados: List[Dict[str, Any]] = []
    for ev in eventos_casi_finales:
        if not reconciliados:
            reconciliados.append(ev)
            continue
        prev = reconciliados[-1]
        if str(prev.get("conductor", "")) != str(ev.get("conductor", "")):
            reconciliados.append(ev)
            continue
        fin_prev = int(prev.get("_fin", 0) or 0)
        ini_ev = int(ev.get("_ini", 0) or 0)
        if ini_ev != fin_prev:
            reconciliados.append(ev)
            continue
        nodo_prev = _canon_nodo_auditoria(prev.get("destino", ""))
        nodo_next = _canon_nodo_auditoria(ev.get("origen", ""))
        if nodo_prev == nodo_next:
            reconciliados.append(ev)
            continue

        # Si hay tiempo configurado de traslado, y el evento previo es Parada,
        # recortar esa espera para insertar el desplazamiento real.
        t_conn = int(obtener_tiempo_traslado(prev.get("destino", ""), ev.get("origen", ""), fin_prev, gestor) or 0)
        if t_conn <= 0:
            hab, tm = _cached_buscar_info_desplazamiento(prev.get("destino", ""), ev.get("origen", ""), fin_prev, gestor)
            if hab and tm and int(tm) > 0:
                t_conn = int(tm)
        ini_conn = fin_prev
        if t_conn > 0 and str(prev.get("evento", "")).strip().upper() == "PARADA":
            prev_ini = int(prev.get("_ini", 0) or 0)
            ini_candidato = fin_prev - t_conn
            if ini_candidato >= prev_ini:
                prev["_fin"] = ini_candidato
                ini_conn = ini_candidato

        reconciliados.append(
            _crear_evento_base_dict(
                "Desplazamiento",
                "",
                ev.get("conductor", ""),
                ini_conn,
                fin_prev,
                prev.get("destino", ""),
                ev.get("origen", ""),
                desc=f"Conexión a {ev.get('origen', '')}",
            )
        )
        reconciliados.append(ev)
    eventos_casi_finales = reconciliados

    # Ajuste final de jornada por segmento (InS -> FnS):
    # las etapas tardías (rescate de vacíos/reconciliación) pueden extender algunos
    # segmentos pocos minutos. Para mantener la regla dura de jornada, se recorta
    # el inicio de InS del mismo segmento (sin tocar comerciales ni conectores).
    def _limite_cid_eventos(cid: Any) -> int:
        return int(
            (limites_por_conductor or {}).get(
                cid,
                (limites_por_conductor or {}).get(str(cid), limite_jornada_final),
            )
            or limite_jornada_final
        )

    prioridad_ajuste = {"InS": 0, "Desplazamiento": 1, "Vacio": 2, "Comercial": 3, "Parada": 4, "FnS": 9}
    por_conductor_eventos: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    for ev in eventos_casi_finales:
        cid = str(ev.get("conductor", "") or "").strip()
        if cid:
            por_conductor_eventos[cid].append(ev)

    for cid, lista in por_conductor_eventos.items():
        lista.sort(
            key=lambda e: (
                int(e.get("_ini", 0) or 0),
                int(e.get("_fin", 0) or 0),
                prioridad_ajuste.get((e.get("evento") or "").strip(), 5),
            )
        )
        limite_cid = _limite_cid_eventos(cid)
        inicio_seg = None
        ins_seg = None
        for ev in lista:
            tp = str(ev.get("evento", "")).strip().upper()
            if tp == "INS":
                inicio_seg = int(ev.get("_ini", 0) or 0)
                ins_seg = ev
                continue
            if tp == "FNS" and inicio_seg is not None and ins_seg is not None:
                fin_seg = int(ev.get("_fin", 0) or 0)
                dur_seg = duracion_minutos(inicio_seg, fin_seg)
                if dur_seg > limite_cid:
                    exceso = dur_seg - limite_cid
                    ins_ini = int(ins_seg.get("_ini", 0) or 0)
                    ins_fin = int(ins_seg.get("_fin", 0) or 0)
                    disponible = max(0, ins_fin - ins_ini)
                    mover = min(exceso, disponible)
                    if mover > 0:
                        ins_seg["_ini"] = ins_ini + mover
                inicio_seg = None
                ins_seg = None

    eventos_finales = []
    for ev in eventos_casi_finales:
        if ev["_fin"] < ev["_ini"]: ev["_fin"] = ev["_ini"]
        if str(ev.get("evento", "")).strip().upper() == "PARADA" and ev["_fin"] <= ev["_ini"]:
            continue
        ev["inicio"] = formatear_hora_deltatime(ev["_ini"])
        ev["fin"] = formatear_hora_deltatime(ev["_fin"])
        ev["duracion"] = formatear_hora_deltatime(ev["_fin"] - ev["_ini"])
        ev.pop("_ini", None); ev.pop("_fin", None); ev.pop("_encadenamiento_nodo", None)
        eventos_finales.append(ev)

    prioridad_tipo_orden = {"InS": 0, "Desplazamiento": 1, "Vacio": 2, "Comercial": 3, "Parada": 4, "FnS": 9}
    eventos_finales.sort(key=lambda e: (str(e.get("conductor", "")), _tiempo_a_minutos(e.get("inicio", 0)) or 0, _tiempo_a_minutos(e.get("fin", 0)) or 0, prioridad_tipo_orden.get((e.get("evento") or "").strip(), 5), str(e.get("bus", ""))))

    validar_eventos_limite_jornada(eventos_finales, limite_jornada_final, limites_por_conductor=limites_por_conductor)
    validar_conductores_con_comercial(eventos_finales)
    return eventos_finales
