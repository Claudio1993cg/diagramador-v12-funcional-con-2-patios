# -*- coding: utf-8 -*-
"""
Utilidades compartidas de tiempo y conectividad.

Fuente única de verdad para:
- Depósito: es_deposito
- Tiempo: duracion_minutos, tiempo_a_minutos
- Conectividad: obtener_tiempo_traslado, calcular_fin_turno

Usado por Fase 2, Fase 3, eventos_completos y exportación.
Sin parches: cada función hace una cosa y la hace bien.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from diagramador_optimizado.utils.time_utils import _to_minutes

if TYPE_CHECKING:
    from diagramador_optimizado.core.domain.logistica import GestorDeLogistica


# ---------------------------------------------------------------------------
# Depósito
# ---------------------------------------------------------------------------

def es_deposito(nodo: str, deposito: str) -> bool:
    """
    Indica si el nodo es el depósito (o alias).
    """
    a = (nodo or "").strip().upper()
    b = (deposito or "").strip().upper()
    if not a or not b:
        return False
    if a == b:
        return True
    if "DEPOSITO" in a and "DEPOSITO" in b and (a in b or b in a):
        return True
    return False


# ---------------------------------------------------------------------------
# Tiempo
# ---------------------------------------------------------------------------

def tiempo_a_minutos(valor: Any) -> int:
    """Convierte HH:MM, int o float a minutos enteros."""
    return _to_minutes(valor)


def duracion_minutos(inicio_min: int, fin_min: int) -> int:
    """
    Duración en minutos entre dos instantes (0-1439).
    Si fin < inicio se asume cruce de medianoche (+1440).
    """
    d = int(fin_min) - int(inicio_min)
    if d < 0:
        d += 1440
    return d


# ---------------------------------------------------------------------------
# Conectividad (desplazamiento + vacío)
# ---------------------------------------------------------------------------

def obtener_tiempo_traslado(
    nodo_a: str,
    nodo_b: str,
    ref_time: int,
    gestor: "GestorDeLogistica",
) -> int:
    """
    Tiempo de traslado entre nodos (desplazamiento o vacío habilitado).
    Devuelve 0 si mismo nodo canónico o sin conexión.
    """
    if not nodo_a or not nodo_b:
        return 0
    canon_a = (gestor.nodo_canonico_para_conectividad(nodo_a) or "").strip().upper()
    canon_b = (gestor.nodo_canonico_para_conectividad(nodo_b) or "").strip().upper()
    if canon_a and canon_b and canon_a == canon_b:
        return 0

    hab, tm = gestor.buscar_info_desplazamiento(nodo_a, nodo_b, ref_time)
    if hab and tm is not None and int(tm) > 0:
        return int(tm)

    res = gestor.buscar_tiempo_vacio(nodo_a, nodo_b, ref_time)
    t_vac = res[0] if isinstance(res, (tuple, list)) else res
    if t_vac is not None and int(t_vac) > 0:
        return int(t_vac)

    return 0


def calcular_fin_turno(
    ultimo_viaje: Dict[str, Any],
    deposito: str,
    gestor: "GestorDeLogistica",
) -> Tuple[int, bool]:
    """
    Fin de jornada = cuando el conductor llega al depósito.
    Retorna (minutos, termina_en_deposito).
    Usa la misma lógica que obtener_tiempo_traslado para consistencia.
    """
    destino = (ultimo_viaje.get("destino") or "").strip()
    fin_viaje = int(ultimo_viaje.get("fin", 0) or 0)

    if es_deposito(destino, deposito):
        return fin_viaje, True

    t = obtener_tiempo_traslado(destino, deposito, fin_viaje, gestor)
    if t > 0:
        return fin_viaje + t, True

    return fin_viaje, False


def _es_relevo_valido(
    nodo: str,
    deposito: str,
    gestor: "GestorDeLogistica",
) -> Tuple[bool, int, int]:
    """Relevo válido: (puede_relevo, t_ida, t_vuelta)."""
    if es_deposito(nodo, deposito):
        return True, 0, 0
    puede_relevo, _ = gestor.puede_hacer_relevo_en_nodo(nodo)
    if not puede_relevo:
        return False, 0, 0
    hab_ida, t_ida = gestor.buscar_info_desplazamiento(deposito, nodo, 0)
    hab_vuelta, t_vuelta = gestor.buscar_info_desplazamiento(nodo, deposito, 0)
    if hab_ida and hab_vuelta and t_ida is not None and t_vuelta is not None:
        return True, int(t_ida), int(t_vuelta)
    return False, 0, 0


# ---------------------------------------------------------------------------
# Rango cubierto (para asignación vacíos)
# ---------------------------------------------------------------------------

def evento_cubierto_por_turno(
    e_ini: int,
    e_fin: int,
    t_ini: int,
    t_fin: int,
    margen_min: int = 15,
) -> bool:
    """
    Indica si el evento [e_ini, e_fin] está cubierto por el turno [t_ini, t_fin].
    Maneja cruce de medianoche.
    """
    tf = t_fin if t_fin >= t_ini else t_fin + 1440
    ef = e_fin if e_fin >= e_ini else e_fin + 1440
    ei = e_ini
    if ei < t_ini and ei + 1440 <= tf + margen_min:
        ei += 1440
        ef += 1440
    return (ei >= t_ini - margen_min) and (ef <= tf + margen_min)
