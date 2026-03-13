"""
Helpers de dominio para normalización de lugares, paradas y depósitos.
Usado por construccion_eventos y ensamblador_conductores.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica


def normalizar_nombre_lugar(nombre: str, gestor: Optional[GestorDeLogistica] = None) -> str:
    """
    Normaliza nombres de lugares para tratar variaciones como el mismo lugar.
    Ejemplo: nombre del depósito (config) y su forma terminal son el mismo lugar.
    """
    if not nombre:
        return ""
    nombre_norm = str(nombre).strip()
    if not nombre_norm:
        return ""
    if gestor:
        nombres_depositos = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else []
        nombre_upper = nombre_norm.upper()
        nombre_sin_prefijo = nombre_norm.replace("Deposito", "").replace("Depósito", "").replace("DEPOSITO", "").replace("DEPÓSITO", "").strip().upper()
        for dep_nombre in nombres_depositos:
            dep_upper = dep_nombre.upper()
            if dep_upper == nombre_upper:
                return dep_nombre
        for dep_nombre in nombres_depositos:
            dep_sin_prefijo = dep_nombre.replace("Deposito", "").replace("Depósito", "").replace("DEPOSITO", "").replace("DEPÓSITO", "").strip().upper()
            if dep_sin_prefijo == nombre_sin_prefijo and dep_sin_prefijo:
                return dep_nombre
            if dep_sin_prefijo == nombre_upper and dep_sin_prefijo:
                return dep_nombre
            if nombre_sin_prefijo == dep_upper and nombre_sin_prefijo:
                return dep_nombre
    return nombre_norm


def obtener_regla_parada(nodo: str, gestor: Optional[Any]) -> Tuple[int, int]:
    """Obtiene parada_min y parada_max para un nodo. Returns (parada_min, parada_max)."""
    if not nodo or not gestor or not hasattr(gestor, "paradas_dict"):
        return 0, 9999
    paradas_dict = gestor.paradas_dict
    regla = buscar_regla_parada_por_nodo(nodo, paradas_dict)
    if regla:
        return regla.get("min", 0), regla.get("max", 9999)
    return 0, 9999


def destino_es_deposito(dest: str, gestor: Optional[Any]) -> bool:
    """Verifica si dest es un depósito (incluyendo terminal en depósito)."""
    if not dest or not gestor:
        return False
    dest_n = (dest or "").strip().upper()
    nombres_dep = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else []
    if not nombres_dep and hasattr(gestor, "deposito_base"):
        nombres_dep = [gestor.deposito_base]
    for dep in (nombres_dep or []):
        dep_n = (dep or "").strip().upper()
        if dep_n == dest_n:
            return True
        # Terminal = nombre del depósito sin prefijo "Deposito"/"Depósito" (desde configuración)
        dep_sin_prefijo = dep_n.replace("DEPOSITO", "").replace("DEPÓSITO", "").strip()
        if dep_sin_prefijo and dep_sin_prefijo == dest_n:
            return True
        if "DEPOSITO" in dep_n and dest_n in dep_n:
            return True
        if "DEPOSITO" in dest_n and dep_n in dest_n:
            return True
    return False


def es_mismo_deposito(a: str, b: str, gestor: Optional[Any]) -> bool:
    """True si a y b son el mismo depósito."""
    if not a or not b:
        return False
    if normalizar_nombre_lugar(a, gestor) == normalizar_nombre_lugar(b, gestor):
        return True
    if destino_es_deposito(a, gestor) and destino_es_deposito(b, gestor):
        nombres = gestor._nombres_depositos() if gestor and hasattr(gestor, "_nombres_depositos") else []
        if len(nombres) <= 1:
            return True
        return obtener_nodo_canonico(a, gestor) == obtener_nodo_canonico(b, gestor)
    return False


def obtener_nodo_canonico(place: str, gestor: Optional[Any]) -> str:
    """Devuelve clave canónica para comparar si dos lugares son el mismo nodo."""
    if not place or not gestor:
        return ""
    p = str(place).strip().upper()
    if not p:
        return ""
    paradas_dict = getattr(gestor, "paradas_dict", {}) or {}
    if p in paradas_dict:
        return p
    sin_dep = p.replace("DEPOSITO", "").replace("DEPÓSITO", "").strip()
    if sin_dep and sin_dep in paradas_dict:
        return sin_dep
    nodos = gestor.config.get("nodos", []) if hasattr(gestor, "config") else []
    for n in nodos:
        if str(n).strip().upper() == p or (sin_dep and str(n).strip().upper() == sin_dep):
            return str(n).strip().upper()
    for clave in paradas_dict:
        if clave in p or p in clave:
            return clave
    return p


def buscar_regla_parada_por_nodo(nodo: str, paradas_dict: dict) -> Optional[Dict]:
    if not nodo or not paradas_dict:
        return None
    p = str(nodo).strip().upper()
    if p in paradas_dict:
        return paradas_dict[p]
    sin_dep = p.replace("DEPOSITO", "").replace("DEPÓSITO", "").strip()
    if sin_dep and sin_dep in paradas_dict:
        return paradas_dict[sin_dep]
    for clave in paradas_dict:
        if clave in p or p in clave:
            return paradas_dict[clave]
    return None


def completar_paradas_conductores(
    eventos_conductores: List[Dict[str, Any]],
    gestor: Any,
    verbose: bool = False,
) -> None:
    """Valida y ajusta paradas existentes para que cumplan min/max."""
    for ev in eventos_conductores:
        if str(ev.get("evento", "")).strip().upper() != "PARADA":
            continue
        nodo_parada = ev.get("destino", "") or ev.get("origen", "")
        parada_min, parada_max = obtener_regla_parada(nodo_parada, gestor)
        if destino_es_deposito(nodo_parada, gestor):
            continue
        if parada_min == 0 and parada_max == 9999:
            continue
        ini = ev.get("inicio", 0)
        fin = ev.get("fin", ini)
        duracion = fin - ini
        if duracion < 0:
            duracion += 1440
        if duracion < parada_min:
            siguiente_inicio = None
            for o in eventos_conductores:
                if o is ev:
                    continue
                if (o.get("conductor"), o.get("bus")) != (ev.get("conductor"), ev.get("bus")):
                    continue
                oini = o.get("inicio", 0)
                if oini > fin and (siguiente_inicio is None or oini < siguiente_inicio):
                    siguiente_inicio = oini
            if siguiente_inicio is not None and siguiente_inicio >= ini + parada_min:
                ev["fin"] = ini + parada_min
                ev["desc"] = (ev.get("desc", "") or "").rstrip() + f" (extendida a mínimo {parada_min}min)"
            else:
                print(
                    f"[PARADAS] ERROR: Parada en {ev.get('destino') or ev.get('origen')} con {duracion}min < mínimo {parada_min}min."
                )
        elif parada_max < 9999 and duracion > parada_max:
            ev["fin"] = ini + parada_max
            ev["desc"] = (ev.get("desc", "") or "").rstrip() + f" (ajustada a máximo {parada_max}min)"
