# -*- coding: utf-8 -*-
"""
Builder de eventos para exportación: todas las creaciones de eventos (InS, FnS,
Parada, Desplazamiento, Vacio, Comercial) se realizan aquí. El excel_writer
solo orquesta y escribe; no crea ningún evento.
"""
from __future__ import annotations

from typing import Any, Dict

from diagramador_optimizado.utils.time_utils import formatear_hora_deltatime


def crear_evento_vacio(
    bus: Any,
    inicio_min: int,
    fin_min: int,
    origen: str,
    destino: str,
    km: float = 0,
    desc: str = "",
    conductor: Any = "",
) -> Dict[str, Any]:
    return {
        "evento": "Vacio",
        "bus": bus,
        "conductor": conductor,
        "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min),
        "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)),
        "origen": origen,
        "destino": destino,
        "linea": "",
        "kilometros": km,
        "desc": desc or f"Vacío {origen}->{destino}",
    }


def crear_evento_parada(
    bus: Any,
    inicio_min: int,
    fin_min: int,
    origen: str,
    desc: str = "",
    conductor: Any = "",
) -> Dict[str, Any]:
    return {
        "evento": "Parada",
        "bus": bus,
        "conductor": conductor,
        "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min),
        "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)),
        "origen": origen,
        "destino": origen,
        "linea": "",
        "kilometros": 0,
        "desc": desc or f"Parada en {origen}",
    }


def crear_evento_comercial(
    bus: Any,
    inicio_min: int,
    fin_min: int,
    origen: str,
    destino: str,
    conductor: Any = "",
    linea: str = "",
    km: float = 0,
    desc: str = "",
    viaje_id: Any = None,
    sentido: str = "",
    tipo_bus: str = "",
) -> Dict[str, Any]:
    return {
        "evento": "Comercial",
        "bus": bus,
        "conductor": conductor,
        "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min),
        "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)),
        "origen": origen,
        "destino": destino,
        "linea": linea,
        "kilometros": km,
        "desc": desc,
        "viaje_id": viaje_id,
        "sentido": sentido,
        "tipo_bus": tipo_bus,
    }


def crear_evento_ins(
    id_conductor: int,
    ins_inicio_min: int,
    ins_fin_min: int,
    deposito: str,
) -> Dict[str, Any]:
    return {
        "evento": "InS",
        "bus": "",
        "conductor": id_conductor,
        "inicio": formatear_hora_deltatime(ins_inicio_min),
        "fin": formatear_hora_deltatime(ins_fin_min),
        "duracion": formatear_hora_deltatime(max(0, ins_fin_min - ins_inicio_min)),
        "origen": deposito,
        "destino": deposito,
        "linea": "",
        "kilometros": 0,
        "desc": "Inicio de Jornada (Toma)",
    }


def crear_evento_fns(
    id_conductor: int,
    fin_turno_min: int,
    deposito: str,
) -> Dict[str, Any]:
    return {
        "evento": "FnS",
        "bus": "",
        "conductor": id_conductor,
        "inicio": formatear_hora_deltatime(fin_turno_min),
        "fin": formatear_hora_deltatime(fin_turno_min),
        "duracion": "00:00",
        "origen": deposito,
        "destino": deposito,
        "linea": "",
        "kilometros": 0,
        "desc": "Fin de Jornada (Deja)",
    }


def crear_evento_desplazamiento(
    id_conductor: int,
    inicio_min: int,
    fin_min: int,
    origen: str,
    destino: str,
    desc: str = "",
) -> Dict[str, Any]:
    return {
        "evento": "Desplazamiento",
        "bus": "",
        "conductor": id_conductor,
        "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min),
        "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)),
        "origen": origen,
        "destino": destino,
        "linea": "",
        "kilometros": 0,
        "desc": desc or f"Desplazamiento {origen}->{destino}",
    }


def crear_evento_parada_sintetica(
    id_conductor: int,
    inicio_min: int,
    fin_min: int,
    origen: str,
    destino: str,
    desc: str = "Parada/descanso",
) -> Dict[str, Any]:
    """Parada para rellenar huecos (sin bus)."""
    return {
        "evento": "Parada",
        "bus": "",
        "conductor": id_conductor,
        "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min),
        "duracion": formatear_hora_deltatime(max(0, fin_min - inicio_min)),
        "origen": origen,
        "destino": destino,
        "linea": "",
        "kilometros": 0,
        "desc": desc,
    }


def crear_copia_evento_bus(
    ev: Dict[str, Any],
    bus_id: int,
    inicio_min: int,
    fin_min: int,
) -> Dict[str, Any]:
    """Copia un evento de bus (Vacio/Parada/Recarga) con formato para eventos completos."""
    dur_min = max(0, fin_min - inicio_min)
    tipo_raw = (str(ev.get("evento", "")) or "").strip()
    tipo_upper = tipo_raw.upper()
    tipo_canonico = tipo_raw
    if tipo_upper == "VACIO":
        tipo_canonico = "Vacio"
    elif tipo_upper == "PARADA":
        tipo_canonico = "Parada"
    elif tipo_upper == "RECARGA":
        tipo_canonico = "Recarga"
    copia = {
        "evento": tipo_canonico,
        "bus": bus_id,
        "conductor": "",
        "inicio": formatear_hora_deltatime(inicio_min),
        "fin": formatear_hora_deltatime(fin_min),
        "duracion": formatear_hora_deltatime(dur_min),
        "origen": ev.get("origen", ""),
        "destino": ev.get("destino", ""),
        "linea": ev.get("linea", ""),
        "kilometros": ev.get("kilometros", 0),
        "desc": ev.get("desc", ""),
        "tipo_bus": ev.get("tipo_bus", ""),
    }
    if tipo_canonico == "Recarga":
        copia["porcentaje_bateria"] = ev.get("porcentaje_bateria", "")
        copia["posicion_recarga"] = ev.get("posicion_recarga", "")
    return copia
