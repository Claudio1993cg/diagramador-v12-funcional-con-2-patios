"""
Lógica de consumo/energía para construcción de eventos de bus.
Extraída de eventos_bus.py para reducir tamaño del orquestador.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.domain.tipos_vehiculo import ParametrosElectricos


def _log_verbose(mensaje: str, verbose: bool) -> None:
    if verbose:
        print(f"[CONSTRUCCION] {mensaje}")


def _obtener_parametros_electricos(
    tipo_bus: Optional[str],
    gestor: Optional[GestorDeLogistica],
) -> Optional[ParametrosElectricos]:
    if not tipo_bus or gestor is None or not hasattr(gestor, "obtener_tipo_bus"):
        return None
    config_tipo = gestor.obtener_tipo_bus(tipo_bus)
    if config_tipo and config_tipo.es_electrico:
        return config_tipo.parametros_electricos
    return None


def _consumo_estimado_evento(
    evento: Dict[str, Any],
    parametros: Optional[ParametrosElectricos],
) -> float:
    if parametros is None:
        return 0.0
    kilometros = evento.get("kilometros", 0) or 0
    if kilometros <= 0:
        return 0.0
    linea = evento.get("linea")
    consumo_linea = parametros.obtener_consumo_linea(linea) if linea else None
    clave_arco = None
    if evento.get("origen") and evento.get("destino"):
        clave_arco = f"{evento['origen']}_{evento['destino']}"
    consumo_arco = parametros.obtener_consumo_arco(clave_arco) if clave_arco else None
    factor = consumo_linea or consumo_arco or parametros.consumo_pct_por_km
    return kilometros * factor


def _aplicar_consumo_evento(
    evento: Dict[str, Any],
    parametros: Optional[ParametrosElectricos],
    bateria_actual: Optional[float],
    verbose: bool,
    contexto: str,
    autonomia_km: Optional[float] = None,
) -> Optional[float]:
    if parametros is None or bateria_actual is None:
        return bateria_actual
    consumo_total = _consumo_estimado_evento(evento, parametros)
    if consumo_total <= 0:
        evento["porcentaje_bateria"] = f"{bateria_actual:.1f}%"
        return bateria_actual
    bateria_actual = max(0.0, bateria_actual - consumo_total)
    evento["consumo"] = round(consumo_total, 2)
    evento["porcentaje_bateria"] = f"{bateria_actual:.1f}%"
    if autonomia_km:
        evento["autonomia"] = round(autonomia_km * (bateria_actual / 100.0), 1)

    minimo_circular = parametros.minimo_para_circular_pct
    if bateria_actual < minimo_circular:
        if verbose:
            _log_verbose(
                f"ERROR: Batería por debajo del mínimo ({contexto}): {evento.get('desc', evento.get('evento'))} -> {bateria_actual:.1f}% < {minimo_circular}%",
                True,
            )
    elif bateria_actual <= minimo_circular + 5.0 and verbose:
        _log_verbose(
            f"ADVERTENCIA: Batería cerca del mínimo ({contexto}): {evento.get('desc', evento.get('evento'))} -> {bateria_actual:.1f}%",
            True,
        )
    return bateria_actual


def _consumo_proyectado_restante(
    bloque: List[Dict[str, Any]],
    indice_inicio: int,
    parametros: Optional[ParametrosElectricos],
    max_eventos: int = 2,
) -> float:
    if parametros is None or indice_inicio >= len(bloque):
        return 0.0
    consumo = 0.0
    tomados = 0
    for idx in range(indice_inicio, len(bloque)):
        viaje = bloque[idx]
        consumo += _consumo_estimado_evento(
            {
                "kilometros": viaje.get("kilometros", 0),
                "linea": viaje.get("linea"),
                "origen": viaje.get("origen"),
                "destino": viaje.get("destino"),
            },
            parametros,
        )
        tomados += 1
        if tomados >= max_eventos:
            break
    return consumo

