"""
Lógica de tiempos de vacío para construcción de eventos de bus.
Extraída de eventos_bus.py para reducir tamaño del orquestador.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional, Tuple

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica


def _log_verbose(mensaje: str, verbose: bool) -> None:
    if verbose:
        print(f"[CONSTRUCCION] {mensaje}")


def _construir_cache_vacio(gestor: GestorDeLogistica):
    @lru_cache(maxsize=30000)
    def _cached(origen: str, destino: str, referencia: int):
        try:
            referencia_int = int(referencia)
        except Exception:
            referencia_int = 0
        return gestor.buscar_tiempo_vacio(origen, destino, referencia_int)

    return _cached


def _buscar_tiempo_vacio_con_respaldo(
    gestor: GestorDeLogistica,
    origen: str,
    destino: str,
    minutos_actuales: int,
    verbose: bool = False,
    contexto: str = "",
    buscar_vacio_fn=None,
) -> Tuple[Optional[int], int]:
    def _canon(n: str) -> str:
        try:
            return (gestor.nodo_canonico_para_conectividad(n) or "").strip().upper()
        except Exception:
            return str(n or "").strip().upper()

    def _directo(o: str, d: str) -> Tuple[Optional[int], int]:
        # Si es exactamente el mismo nodo textual, no hay traslado.
        # OJO: no forzar 0 para alias depósito<->terminal,
        # porque puede existir tiempo configurado válido (p. ej. 1 min).
        if _canon(o) == _canon(d) and str(o or "").strip().upper() == str(d or "").strip().upper():
            return 0, 0
        t0, km0 = buscar(o, d, minutos_actuales)
        if t0 is not None and not (t0 <= 1 and (km0 or 0) > 0 and o != d):
            return int(t0), int(km0 or 0)
        t_rev, km_rev = buscar(d, o, minutos_actuales)
        if t_rev is not None and int(t_rev) > 1:
            return int(t_rev), int(km_rev or 0)
        hab_desp, t_desp = gestor.buscar_info_desplazamiento(o, d, minutos_actuales)
        if hab_desp and t_desp is not None and int(t_desp) > 0:
            return int(t_desp), 0
        return None, 0

    buscar = buscar_vacio_fn or gestor.buscar_tiempo_vacio
    tiempo, km = _directo(origen, destino)
    if tiempo is not None:
        return tiempo, km

    # Respaldo adicional sin inventar: ruta de 2 tramos por nodos configurados.
    nodos = []
    try:
        nodos.extend(list((getattr(gestor, "config", {}) or {}).get("nodos", []) or []))
    except Exception:
        pass
    try:
        if hasattr(gestor, "_nombres_depositos"):
            nodos.extend(list(gestor._nombres_depositos() or []))
    except Exception:
        pass
    nodos.extend([origen, destino])
    # Deduplicar preservando orden
    vistos = set()
    candidatos = []
    for n in nodos:
        k = _canon(n)
        if not k or k in vistos:
            continue
        vistos.add(k)
        candidatos.append(n)

    mejor_t = None
    mejor_km = 0
    for mid in candidatos:
        if _canon(mid) in (_canon(origen), _canon(destino)):
            continue
        t1, km1 = _directo(origen, mid)
        if t1 is None:
            continue
        t2, km2 = _directo(mid, destino)
        if t2 is None:
            continue
        total = int(t1) + int(t2)
        if mejor_t is None or total < mejor_t:
            mejor_t = total
            mejor_km = int(km1 or 0) + int(km2 or 0)

    if mejor_t is not None:
        if verbose:
            _log_verbose(
                f"Sin conexión directa {origen}->{destino}. "
                f"Usando ruta 2-tramos ({mejor_t} min). {contexto}",
                verbose,
            )
        return int(mejor_t), int(mejor_km)

    return None, 0

