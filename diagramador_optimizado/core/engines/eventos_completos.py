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

    eventos_sin_conductor = [e for e in eventos_base if not e.get("conductor")]
    eventos_con_conductor = collections.defaultdict(list)
    for e in eventos_base:
        if e.get("conductor"): eventos_con_conductor[e["conductor"]].append(e)

    def get_canonical(nodo): return (gestor.nodo_canonico_para_conectividad(nodo) or "").strip().upper()
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
            if not ev_descartado and ev["_fin"] >= ev["_ini"]: reales_limpios.append(ev)

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
                        linea_tiempo.append(_crear_evento_base_dict("Vacio", bus_conector, cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                    else:
                        linea_tiempo.append(_crear_evento_base_dict("Desplazamiento", "", cid_seg, ini_min, fin_min, origen, destino, desc=desc))
                    return

                # REGLA OPERATIVA:
                # - Si es el mismo bus, el conector es Vacio (nunca Desplazamiento).
                # - Si no hay duración efectiva, no se inventa conector.
                if hay_bus and not hay_cambio_bus and duracion > 0:
                    bus_conector = bus_prev or bus_next
                    linea_tiempo.append(_crear_evento_base_dict("Vacio", bus_conector, cid_seg, ini_min, fin_min, origen, destino, desc=desc))
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

            linea_tiempo = []
            primer_ev = seg[0]
            
            if turno_ini_ref >= 0:
                ins_ini = turno_ini_ref
                ins_fin = min(primer_ev["_ini"], turno_ini_ref + t_toma)
                if ins_fin < ins_ini:
                    ins_fin = ins_ini
                linea_tiempo.append(
                    _crear_evento_base_dict("InS", "", cid_seg, ins_ini, ins_fin, deposito, deposito, desc="Inicio Jornada")
                )
                if not es_deposito(primer_ev['origen'], deposito):
                    _agregar_conector(None, primer_ev, ins_fin, primer_ev['_ini'], deposito, primer_ev['origen'], desc=f"A {primer_ev['origen']}")
                elif primer_ev["_ini"] > ins_fin:
                    linea_tiempo.append(
                        _crear_evento_base_dict("Parada", "", cid_seg, ins_fin, primer_ev["_ini"], deposito, deposito, desc="Espera")
                    )
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
                if i > 0:
                    prev = seg[i-1]
                    gap = ev['_ini'] - prev['_fin']
                    o_can = get_canonical(prev['destino'])
                    d_can = get_canonical(ev['origen'])
                    
                    if gap > 0:
                        if o_can == d_can:
                            hab, tm = _cached_buscar_info_desplazamiento(prev['destino'], ev['origen'], prev['_fin'], gestor)
                            if hab and tm and int(tm) > 0 and gap >= int(tm) and prev['destino'].upper() != ev['origen'].upper():
                                t = int(tm)
                                if gap > t: linea_tiempo.append(_crear_evento_base_dict("Parada", "", cid_seg, prev['_fin'], ev['_ini'] - t, prev['destino'], prev['destino'], desc="Espera"))
                                _agregar_conector(prev, ev, ev['_ini'] - t, ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                            else:
                                linea_tiempo.append(_crear_evento_base_dict("Parada", "", cid_seg, prev['_fin'], ev['_ini'], prev['destino'], prev['destino'], desc="Espera"))
                        else:
                            t = obtener_tiempo_traslado(prev['destino'], ev['origen'], prev['_fin'], gestor)
                            if t == 0: t = gap 
                            if t > gap: t = gap 
                            if gap > t: linea_tiempo.append(_crear_evento_base_dict("Parada", "", cid_seg, prev['_fin'], ev['_ini'] - t, prev['destino'], prev['destino'], desc="Espera"))
                            _agregar_conector(prev, ev, ev['_ini'] - t, ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                    elif gap == 0 and o_can != d_can:
                        _agregar_conector(prev, ev, ev['_ini'], ev['_ini'], prev['destino'], ev['origen'], desc=f"A {ev['origen']}")
                tipo_ev_i = str(ev.get("evento", "")).strip().upper()
                # Regla operativa solicitada: al inicio de jornada del conductor,
                # el primer traslado se modela como Desplazamiento (no Vacio).
                if (
                    i == 0
                    and tipo_ev_i == "VACIO"
                    and linea_tiempo
                    and str(linea_tiempo[-1].get("evento", "")).strip().upper() == "INS"
                ):
                    ev_inicio = dict(ev)
                    ev_inicio["evento"] = "Desplazamiento"
                    ev_inicio["bus"] = ""
                    linea_tiempo.append(ev_inicio)
                else:
                    linea_tiempo.append(ev)

            ultimo_ev = seg[-1]
            if turno_fin_ref >= 0:
                fin_jornada = turno_fin_ref
                if not es_deposito(ultimo_ev['destino'], deposito):
                    _agregar_conector(ultimo_ev, None, ultimo_ev['_fin'], fin_jornada, ultimo_ev['destino'], deposito, desc=f"A {deposito}")
                elif ultimo_ev['_fin'] < fin_jornada:
                    linea_tiempo.append(
                        _crear_evento_base_dict(
                            "Parada",
                            "",
                            cid_seg,
                            ultimo_ev['_fin'],
                            fin_jornada,
                            deposito,
                            deposito,
                            desc="Espera",
                        )
                    )
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
            eventos_casi_finales.extend(linea_tiempo)

    validar_comerciales_todos_asignados(eventos_base)
    # Vacíos sin asignar: advertir pero no fallar (p. ej. retornos al depósito fuera del rango del turno)
    sin_conductor_vacio = [e for e in eventos_sin_conductor if str(e.get("evento", "")).strip().upper() == "VACIO"]
    if sin_conductor_vacio:
        for e in sin_conductor_vacio[:5]:
            print(f"  [ADVERTENCIA] Vacio sin conductor: {e.get('origen','')}->{e.get('destino','')} {e.get('inicio','')}-{e.get('fin','')}")
        if len(sin_conductor_vacio) > 5:
            print(f"  [ADVERTENCIA] ... y {len(sin_conductor_vacio) - 5} vacíos más sin conductor.")
    # Solo añadir al resultado eventos sin conductor que NO sean Comercial ni Vacio (ej. Paradas al inicio/fin)
    eventos_sin_conductor_ok = [e for e in eventos_sin_conductor if str(e.get("evento", "")).strip().upper() not in ("COMERCIAL", "VACIO")]
    eventos_casi_finales.extend(eventos_sin_conductor_ok)

    # Normalizar paradas consecutivas por conductor (mismo nodo, contiguas/solapadas).
    prioridad_tipo_pre = {"InS": 0, "Desplazamiento": 1, "Vacio": 2, "Comercial": 3, "Parada": 4, "FnS": 9}
    eventos_casi_finales.sort(
        key=lambda e: (
            str(e.get("conductor", "")),
            int(e.get("_ini", 0) or 0),
            int(e.get("_fin", 0) or 0),
            prioridad_tipo_pre.get((e.get("evento") or "").strip(), 5),
            str(e.get("bus", "")),
        )
    )
    eventos_casi_finales_norm = []
    for ev in eventos_casi_finales:
        if not eventos_casi_finales_norm:
            eventos_casi_finales_norm.append(ev)
            continue
        prev = eventos_casi_finales_norm[-1]
        if (
            str(prev.get("conductor", "")) == str(ev.get("conductor", ""))
            and str(prev.get("evento", "")).strip().upper() == "PARADA"
            and str(ev.get("evento", "")).strip().upper() == "PARADA"
            and get_canonical(prev.get("origen", "")) == get_canonical(ev.get("origen", ""))
            and int(ev.get("_ini", 0) or 0) <= int(prev.get("_fin", 0) or 0) + 1
        ):
            prev["_fin"] = max(int(prev.get("_fin", 0) or 0), int(ev.get("_fin", 0) or 0))
            prev["desc"] = f"Parada en {prev.get('origen', '')}"
            continue
        eventos_casi_finales_norm.append(ev)
    eventos_casi_finales = eventos_casi_finales_norm

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
