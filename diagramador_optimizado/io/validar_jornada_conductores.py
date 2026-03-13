"""
Validación de jornada de conductores:
- Sin teletransportaciones: en la secuencia de eventos de cada conductor,
  el nodo de inicio de cada evento debe ser igual al nodo de fin del evento anterior.
  El último evento debe terminar en el depósito configurado (dinámico).
- InS, FnS y Desplazamiento no deben tener bus asociado.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

# Eventos que no deben tener bus asignado
EVENTOS_SIN_BUS = {"INS", "FNS", "DESPLAZAMIENTO"}


def _nombres_depositos(gestor: Any) -> List[str]:
    if not gestor:
        return []
    nombres = getattr(gestor, "_nombres_depositos", lambda: None)()
    if nombres:
        return list(nombres)
    if hasattr(gestor, "deposito_base") and gestor.deposito_base:
        return [gestor.deposito_base]
    return []


def _es_deposito(place: str, gestor: Any) -> bool:
    """True si place es el depósito configurado.
    Usa comparación exacta (case-insensitive) para evitar falsos positivos.
    'PIE ANDINO' (terminal) ≠ 'DEPOSITO PIE ANDINO' (depósito)."""
    if not place or not gestor:
        return False
    p = (place or "").strip().upper()
    for dep in _nombres_depositos(gestor):
        d = (dep or "").strip().upper()
        if not d:
            continue
        if p == d:
            return True
    return False


def _es_deposito_o_punto_relevo(place: str, gestor: Any) -> bool:
    """True si place es depósito configurado o punto de relevo (nodo con desplazamiento habilitado al depósito)."""
    if not place or not gestor:
        return False
    if _es_deposito(place, gestor):
        return True
    puede_relevo, _ = getattr(gestor, "puede_hacer_relevo_en_nodo", lambda x: (False, None))(place)
    return bool(puede_relevo)


def _mismo_nodo(a: str, b: str, gestor: Any) -> bool:
    """True si a y b representan el mismo nodo (incl. variantes de depósito).
    Usa comparación exacta (case-insensitive) para evitar falsos positivos entre
    nodos con nombres similares como 'PIE ANDINO' y 'DEPOSITO PIE ANDINO'."""
    if not a and not b:
        return True
    if not a or not b:
        return False
    a_u = (a or "").strip().upper()
    b_u = (b or "").strip().upper()
    if _es_deposito(a, gestor) and _es_deposito(b, gestor):
        return True
    return a_u == b_u


def validar_continuidad_nodos_y_deposito_final(
    eventos: List[Dict[str, Any]],
    gestor: Any,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Valida que no existan teletransportaciones: para cada conductor, en todos los eventos
    de su jornada el nodo de inicio debe ser igual al nodo de fin del evento anterior.
    El último evento debe terminar en el depósito configurado (dinámico).

    Args:
        eventos: Lista de eventos con conductor, inicio, fin, origen, destino, evento.
        gestor: GestorDeLogistica (para depósitos y comparación de nodos).
        verbose: Si True, imprime detalles.

    Returns:
        Lista de errores. Cada error es un dict con conductor, mensaje, evento_ant, evento_act, etc.
    """
    errores: List[Dict[str, Any]] = []
    depositos = _nombres_depositos(gestor)
    deposito_ref = (depositos[0] if depositos else "") or getattr(gestor, "deposito_base", "")

    # Agrupar por conductor
    por_conductor: Dict[int, List[Dict[str, Any]]] = {}
    for ev in eventos:
        c = ev.get("conductor")
        if c is None:
            continue
        try:
            cid = int(c)
        except (TypeError, ValueError):
            continue
        if cid not in por_conductor:
            por_conductor[cid] = []
        por_conductor[cid].append(dict(ev))

    for cid, evs in por_conductor.items():
        evs_ord = sorted(evs, key=lambda e: (e.get("inicio", 0), e.get("fin", 0)))
        if len(evs_ord) < 2:
            continue
        for i in range(1, len(evs_ord)):
            ant = evs_ord[i - 1]
            act = evs_ord[i]
            dest_ant = (ant.get("destino") or "").strip()
            orig_act = (act.get("origen") or "").strip()
            if _mismo_nodo(dest_ant, orig_act, gestor):
                continue
            # En nodos de relevo el conductor puede bajar y el siguiente evento empezar en otro nodo de relevo (relevo entre conductores)
            if dest_ant and orig_act and _es_deposito_o_punto_relevo(dest_ant, gestor) and _es_deposito_o_punto_relevo(orig_act, gestor):
                continue
            errores.append({
                "conductor": cid,
                "tipo": "teletransportacion",
                "mensaje": f"Conductor {cid}: nodo fin del evento anterior ({dest_ant or 'N/A'}) != nodo inicio del siguiente ({orig_act or 'N/A'})",
                "evento_ant": ant.get("evento"),
                "evento_act": act.get("evento"),
                "destino_anterior": dest_ant,
                "origen_actual": orig_act,
                "inicio_actual": act.get("inicio"),
                "fin_anterior": ant.get("fin"),
            })
            if verbose:
                print(f"  [VALIDACION] {errores[-1]['mensaje']}")

        # Último evento del conductor: destino debe ser el depósito configurado (FnS)
        ultimo = evs_ord[-1]
        dest_ultimo = (ultimo.get("destino") or "").strip()
        if not _es_deposito(dest_ultimo, gestor):
            errores.append({
                "conductor": cid,
                "tipo": "fin_no_deposito",
                "mensaje": f"Conductor {cid}: el último evento debe terminar en el depósito configurado ({deposito_ref}), destino actual: '{dest_ultimo or 'N/A'}'",
                "evento": ultimo.get("evento"),
                "destino": dest_ultimo,
            })
            if verbose:
                print(f"  [VALIDACION] {errores[-1]['mensaje']}")

        # REGLA: El evento inmediatamente anterior al FnS debe terminar en depósito o punto de relevo (nunca en no-relevo como LA PIRAMIDE)
        # Excepción: si el nodo tiene vacío habilitado al depósito, no se considera error (el retorno existe en la red).
        eventos_sin_fns = [e for e in evs_ord if str(e.get("evento", "")).strip().upper() != "FNS"]
        if eventos_sin_fns:
            evento_antes_fns = eventos_sin_fns[-1]
            dest_antes_fns = (evento_antes_fns.get("destino") or "").strip()
            if not _es_deposito_o_punto_relevo(dest_antes_fns, gestor):
                tiempo_vacio = None
                if gestor and dest_antes_fns and deposito_ref:
                    try:
                        tiempo_vacio, _ = gestor.buscar_tiempo_vacio(
                            dest_antes_fns, deposito_ref, evento_antes_fns.get("fin", evento_antes_fns.get("inicio", 0))
                        )
                    except Exception:
                        pass
                if not (tiempo_vacio is not None and tiempo_vacio > 0):
                    errores.append({
                        "conductor": cid,
                        "tipo": "evento_final_antes_fns_no_relevo",
                        "mensaje": f"Conductor {cid}: el evento final antes del FnS termina en '{dest_antes_fns or 'N/A'}' (debe ser depósito o punto de relevo habilitado, no un nodo como LA PIRAMIDE)",
                        "evento": evento_antes_fns.get("evento"),
                        "destino": dest_antes_fns,
                    })
                    if verbose:
                        print(f"  [VALIDACION] {errores[-1]['mensaje']}")

    return errores


def validar_eventos_sin_bus(
    eventos: List[Dict[str, Any]],
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Valida que InS, FnS y Desplazamiento no tengan bus asociado.

    Returns:
        Lista de errores. Cada error es un dict con conductor, evento, bus_actual.
    """
    errores: List[Dict[str, Any]] = []
    for ev in eventos:
        tipo = str(ev.get("evento", "")).strip().upper()
        if tipo not in EVENTOS_SIN_BUS:
            continue
        bus = ev.get("bus")
        tiene_bus = bus is not None and str(bus).strip() != "" and bus != 0
        if tiene_bus:
            errores.append({
                "tipo": "bus_no_permitido",
                "mensaje": f"Evento {tipo} no debe tener bus asignado (conductor {ev.get('conductor')}, bus={bus})",
                "conductor": ev.get("conductor"),
                "evento": tipo,
                "bus_actual": bus,
                "inicio": ev.get("inicio"),
            })
            if verbose:
                print(f"  [VALIDACION] {errores[-1]['mensaje']}")
    return errores


def validar_eventos_despues_fns(
    eventos: List[Dict[str, Any]],
    gestor: Any,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Valida que NO existan eventos después del FnS para el mismo conductor.
    El FnS marca el fin del turno, por lo que no debe haber más eventos asignados al conductor después de él.
    
    También valida que el FnS tenga duración 0 (inicio == fin).

    Returns:
        Lista de errores. Cada error es un dict con conductor, mensaje, evento_fns, evento_despues, etc.
    """
    errores: List[Dict[str, Any]] = []
    
    # Agrupar por conductor
    por_conductor: Dict[int, List[Dict[str, Any]]] = {}
    for ev in eventos:
        c = ev.get("conductor")
        if c is None:
            continue
        try:
            cid = int(c)
        except (TypeError, ValueError):
            continue
        if cid not in por_conductor:
            por_conductor[cid] = []
        por_conductor[cid].append(dict(ev))
    
    for cid, evs in por_conductor.items():
        evs_ord = sorted(evs, key=lambda e: (e.get("inicio", 0), e.get("fin", 0)))
        
        # Buscar FnS del conductor
        fns_eventos = [e for e in evs_ord if str(e.get("evento", "")).strip().upper() == "FNS"]
        
        if not fns_eventos:
            continue  # Sin FnS, no se puede validar eventos después
        
        # Tomar el último FnS (por si hay múltiples)
        fns = fns_eventos[-1]
        fin_fns = fns.get("fin", fns.get("inicio", 0))
        inicio_fns = fns.get("inicio", 0)
        
        # REGLA CRÍTICA: FnS debe tener duración 0 (inicio == fin)
        if inicio_fns != fin_fns:
            errores.append({
                "conductor": cid,
                "tipo": "fns_duracion_incorrecta",
                "mensaje": f"Conductor {cid}: FnS debe tener duración 0 (inicio == fin). Actual: inicio={inicio_fns}, fin={fin_fns}, duración={fin_fns - inicio_fns}",
                "evento_fns": "FnS",
                "inicio_fns": inicio_fns,
                "fin_fns": fin_fns,
                "duracion_actual": fin_fns - inicio_fns,
            })
            if verbose:
                print(f"  [VALIDACION] {errores[-1]['mensaje']}")
        
        # Buscar eventos después del FnS para el mismo conductor
        eventos_despues_fns = [
            e for e in evs_ord
            if e is not fns and e.get("inicio", 0) >= fin_fns
        ]
        
        for ev_despues in eventos_despues_fns:
            tipo_ev = ev_despues.get("evento", "")
            inicio_ev = ev_despues.get("inicio", 0)
            errores.append({
                "conductor": cid,
                "tipo": "evento_despues_fns",
                "mensaje": f"Conductor {cid}: evento '{tipo_ev}' después del FnS (inicio={inicio_ev} >= fin FnS={fin_fns}). El FnS marca el fin del turno, no debe haber más eventos.",
                "evento_fns": "FnS",
                "fin_fns": fin_fns,
                "evento_despues": tipo_ev,
                "inicio_evento_despues": inicio_ev,
                "origen": ev_despues.get("origen"),
                "destino": ev_despues.get("destino"),
            })
            if verbose:
                print(f"  [VALIDACION] {errores[-1]['mensaje']}")
    
    return errores


def validar_todos_viajes_comerciales(
    eventos: List[Dict[str, Any]],
    viajes_comerciales: List[Dict[str, Any]],
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    REGLA DURA: TODOS los viajes comerciales DEBEN estar asignados en la salida.
    Si falta alguno, se reporta como error.

    Returns:
        Lista de errores. Cada error es un dict con viaje_id, mensaje, etc.
    """
    errores: List[Dict[str, Any]] = []
    viajes_ids_en_eventos = {
        ev.get("viaje_id") for ev in eventos
        if str(ev.get("evento", "")).strip().upper() == "COMERCIAL" and ev.get("viaje_id") is not None
    }
    viajes_ids_en_eventos |= {str(x) for x in viajes_ids_en_eventos if x is not None}

    for v in viajes_comerciales or []:
        vid = v.get("id") or v.get("_tmp_id")
        if vid is None:
            continue
        if vid in viajes_ids_en_eventos or str(vid) in viajes_ids_en_eventos:
            continue
        errores.append({
            "tipo": "viaje_comercial_faltante",
            "mensaje": f"Viaje comercial faltante: id={vid} (origen={v.get('origen')} -> destino={v.get('destino')}, "
                      f"inicio={v.get('inicio')} fin={v.get('fin')}). REGLA DURA: todos los viajes deben figurar.",
            "viaje_id": vid,
            "origen": v.get("origen"),
            "destino": v.get("destino"),
            "inicio": v.get("inicio"),
            "fin": v.get("fin"),
            "linea": v.get("linea"),
        })
        if verbose:
            print(f"  [VALIDACION] {errores[-1]['mensaje']}")

    return errores


def validar_jornada_completa(
    eventos: List[Dict[str, Any]],
    gestor: Any,
    verbose: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Ejecuta todas las validaciones de jornada:
    1) Continuidad de nodos y último evento en depósito.
    2) InS, FnS y Desplazamiento sin bus.
    3) No eventos después del FnS (fin de turno).
    4) FnS con duración 0 (inicio == fin).

    Returns:
        (errores_continuidad, errores_sin_bus)
    """
    err_cont = validar_continuidad_nodos_y_deposito_final(eventos, gestor, verbose=verbose)
    err_bus = validar_eventos_sin_bus(eventos, verbose=verbose)
    err_despues_fns = validar_eventos_despues_fns(eventos, gestor, verbose=verbose)
    
    # Agregar errores de eventos después del FnS a los errores de continuidad
    err_cont.extend(err_despues_fns)
    
    return err_cont, err_bus
