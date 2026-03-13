"""
Helpers de recarga usados en construccion de eventos de bus.
Extraido de eventos_bus.py para reducir tamaño del orquestador.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from diagramador_optimizado.core.builders.recarga import _buscar_oportunidad_recarga
from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.domain.tipos_vehiculo import ParametrosElectricos


def _calcular_recarga_disponible(
    parametros: Optional[ParametrosElectricos],
    bateria_actual: Optional[float],
    inicio_minimo: int,
    fin_maximo: int,
    bateria_objetivo: Optional[float] = None,
) -> Optional[Tuple[int, int, float]]:
    if parametros is None or bateria_actual is None:
        return None
    if fin_maximo <= inicio_minimo:
        return None
    if bateria_objetivo is None:
        bateria_objetivo = 100.0

    ventana_inicio = parametros.ventana_recarga.inicio
    ventana_fin = parametros.ventana_recarga.fin
    inicio_disponible = max(inicio_minimo, ventana_inicio)
    fin_disponible = min(fin_maximo, ventana_fin)
    if fin_disponible <= inicio_disponible:
        return None

    tiempo_minimo = parametros.tiempo_minimo_recarga
    if fin_disponible - inicio_disponible < tiempo_minimo:
        return None

    delta_bateria_necesario = bateria_objetivo - bateria_actual
    if delta_bateria_necesario <= 0:
        return None

    tiempo_necesario = math.ceil(delta_bateria_necesario / parametros.tasa_recarga_pct_por_min)
    tiempo_recarga = max(tiempo_minimo, tiempo_necesario)
    if fin_disponible - inicio_disponible < tiempo_recarga:
        tiempo_recarga = fin_disponible - inicio_disponible
        if tiempo_recarga < tiempo_minimo:
            return None

    delta = tiempo_recarga * parametros.tasa_recarga_pct_por_min
    bateria_final = min(100.0, bateria_actual + delta)
    if bateria_final <= bateria_actual:
        return None

    fin_recarga = inicio_disponible + tiempo_recarga
    return inicio_disponible, fin_recarga, bateria_final


def _agregar_evento_recarga(
    eventos: List[Dict[str, Any]],
    deposito: str,
    inicio: int,
    fin: int,
    gestor: GestorDeLogistica,
    bateria_inicial: Optional[float] = None,
    bateria_final: Optional[float] = None,
    tipo_bus: Optional[str] = None,
) -> None:
    if fin <= inicio:
        return
    if not isinstance(deposito, str):
        deposito = str(deposito) if deposito else gestor.deposito_base

    porcentaje_texto: Optional[str] = None
    if bateria_inicial is not None or bateria_final is not None:
        porcentaje_texto = f"{bateria_inicial or 0:.0f}% -> {bateria_final or bateria_inicial or 0:.0f}%"

    posicion_recarga = None
    if hasattr(gestor, "posiciones_recarga_en_deposito"):
        posicion_recarga = gestor.posiciones_recarga_en_deposito(deposito)

    eventos.append(
        {
            "evento": "Recarga",
            "origen": deposito,
            "destino": deposito,
            "inicio": inicio,
            "fin": fin,
            "kilometros": 0,
            "desc": f"Recarga en {deposito}",
            "porcentaje_bateria": porcentaje_texto,
            "posicion_recarga": posicion_recarga,
            "tipo_bus": tipo_bus,
        }
    )


def _requiere_recarga(
    parametros: Optional[Any],
    bateria_actual: Optional[float],
    consumo_proyectado: float,
) -> bool:
    if parametros is None or bateria_actual is None:
        return False
    minimo = getattr(parametros, "minimo_para_circular_pct", 30.0)
    bateria_despues = bateria_actual - (consumo_proyectado or 0)
    return bateria_actual < minimo or bateria_despues < minimo


def _planificar_recarga_si_requiere(
    eventos: List[Dict[str, Any]],
    gestor: GestorDeLogistica,
    parametros_electricos: Any,
    bateria_actual: Optional[float],
    bus_id: Optional[int] = None,
    tipo_bus: Optional[str] = None,
    destino_actual: Optional[str] = None,
    inicio_disponible: int = 0,
    fin_disponible: int = 0,
    contexto: str = "",
    verbose: bool = False,
    consumo_proyectado: float = 0.0,
    autonomia_km: Optional[float] = None,
) -> Optional[float]:
    if not _requiere_recarga(parametros_electricos, bateria_actual, consumo_proyectado):
        return bateria_actual
    if not destino_actual or fin_disponible <= inicio_disponible:
        return bateria_actual

    tiempo_disponible = fin_disponible - inicio_disponible
    ev_recarga = _buscar_oportunidad_recarga(
        destino_actual,
        destino_actual,
        fin_disponible,
        bateria_actual or 0.0,
        parametros_electricos,
        gestor,
        gestor.buscar_tiempo_vacio,
        tiempo_disponible,
        verbose,
        inicio_ventana=inicio_disponible,
    )
    if ev_recarga:
        bateria_final = ev_recarga.get("bateria_final", bateria_actual)
        vacio_ida = ev_recarga.get("vacio_ida")
        vacio_vuelta = ev_recarga.get("vacio_vuelta")
        if vacio_ida:
            v = dict(vacio_ida)
            v["evento"] = "Vacio"
            v["tipo_bus"] = tipo_bus
            eventos.append(v)
        _agregar_evento_recarga(
            eventos,
            ev_recarga.get("destino", gestor.deposito_base),
            ev_recarga.get("inicio", inicio_disponible),
            ev_recarga.get("fin", fin_disponible),
            gestor,
            bateria_inicial=bateria_actual,
            bateria_final=bateria_final,
            tipo_bus=tipo_bus,
        )
        if vacio_vuelta:
            v = dict(vacio_vuelta)
            v["evento"] = "Vacio"
            v["tipo_bus"] = tipo_bus
            eventos.append(v)
        return bateria_final
    return bateria_actual

