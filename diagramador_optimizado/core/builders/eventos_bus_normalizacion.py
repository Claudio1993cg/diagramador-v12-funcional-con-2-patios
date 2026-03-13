"""
Normalizacion de secuencias de eventos de bus/conductor.
Extraido de eventos_bus.py para reducir acoplamiento.

IMPORTANTE: esta lógica se mantiene funcionalmente idéntica
al comportamiento histórico para evitar regresiones.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _fusionar_paradas_consecutivas(eventos: List[Dict[str, Any]], gestor: Optional[Any] = None) -> List[Dict[str, Any]]:
    """
    Fusiona paradas consecutivas en el mismo nodo en un solo evento.
    Evita paradas duplicadas o continuas (varios eventos Parada seguidos en el mismo lugar).
    """
    if not eventos:
        return []
    def _canon_nodo(nodo: Any) -> str:
        txt = str(nodo or "").strip()
        if not txt:
            return ""
        if gestor and hasattr(gestor, "nodo_canonico_para_conectividad"):
            try:
                return str(gestor.nodo_canonico_para_conectividad(txt) or "").strip().upper()
            except Exception:
                pass
        return " ".join(txt.upper().replace("DEPOSITO", "").split())
    # Ordenar por inicio para procesar en orden cronológico
    ordenados = sorted(eventos, key=lambda e: (e.get("inicio", 0), e.get("fin", 0)))
    salida: List[Dict[str, Any]] = []
    for ev in ordenados:
        tipo = str(ev.get("evento", "")).strip()
        if tipo != "Parada":
            salida.append(ev)
            continue
        origen = (ev.get("origen") or "").strip()
        destino = (ev.get("destino") or "").strip()
        if not salida or str(salida[-1].get("evento", "")).strip() != "Parada":
            salida.append(dict(ev))
            continue
        ultimo = salida[-1]
        ultimo_orig = (ultimo.get("origen") or "").strip()
        ultimo_dest = (ultimo.get("destino") or "").strip()
        mismo_nodo = (
            _canon_nodo(origen) == _canon_nodo(ultimo_orig)
            and _canon_nodo(destino) == _canon_nodo(ultimo_dest)
        )
        # Misma ubicación y parada contigua (fin anterior == inicio actual, o solapamiento)
        if (mismo_nodo and
                ev.get("inicio", 0) <= ultimo.get("fin", 0) + 1):  # +1 tolerancia redondeo
            # Fusionar: extender la parada anterior hasta el fin de la actual
            ultimo["fin"] = max(ultimo.get("fin", 0), ev.get("fin", ev.get("inicio", 0)))
            if "desc" in ultimo:
                ultimo["desc"] = f"Parada en {origen} ({ultimo['fin'] - ultimo.get('inicio', 0)} min)"
            continue
        salida.append(dict(ev))
    return salida


def _normalizar_eventos_bus(eventos: List[Dict[str, Any]], verbose: bool = False, gestor: Optional[Any] = None) -> List[Dict[str, Any]]:
    """
    Normaliza la secuencia de eventos (de bus o conductor):
    - Fusiona paradas consecutivas en el mismo nodo (evita paradas duplicadas/continuas).
    - Falla en duplicados exactos (modo estricto, no autocorrección).
    - Resuelve solapamientos priorizando Comerciales > Recargas > Vacíos > Paradas.
    """
    if not eventos:
        return []

    def _canon_nodo(nodo: Any) -> str:
        txt = str(nodo or "").strip()
        if not txt:
            return ""
        if gestor and hasattr(gestor, "nodo_canonico_para_conectividad"):
            try:
                return str(gestor.nodo_canonico_para_conectividad(txt) or "").strip().upper()
            except Exception:
                pass
        return " ".join(txt.upper().replace("DEPOSITO", "").split())

    # Limpieza dura de vacíos inconsistentes:
    # - eliminar VACIO con duración <= 0
    # - eliminar VACIO con origen/destino canónicamente iguales
    # - eliminar duplicados de VACIO por misma clave temporal/espacial
    eventos_filtrados: List[Dict[str, Any]] = []
    vacios_vistos = set()
    for ev in eventos:
        tipo = str(ev.get("evento", "")).strip().upper()
        if tipo != "VACIO":
            eventos_filtrados.append(ev)
            continue
        ini = int(ev.get("inicio", 0) or 0)
        fin = int(ev.get("fin", ini) or ini)
        if fin <= ini:
            continue
        origen = ev.get("origen", "")
        destino = ev.get("destino", "")
        o_can = _canon_nodo(origen)
        d_can = _canon_nodo(destino)
        origen_txt = str(origen or "").strip().upper()
        destino_txt = str(destino or "").strip().upper()
        conecta_deposito = ("DEPOSITO" in origen_txt) or ("DEPOSITO" in destino_txt)
        # Mantener vacíos depósito<->terminal aunque el canónico coincida (alias),
        # siempre que tengan duración positiva (son conexiones operativas válidas).
        if o_can and d_can and o_can == d_can and not (conecta_deposito and fin > ini):
            continue
        bus = str(ev.get("bus", "") or "").strip()
        clave = (bus, ini, fin, o_can, d_can)
        if clave in vacios_vistos:
            continue
        vacios_vistos.add(clave)
        eventos_filtrados.append(ev)
    eventos = eventos_filtrados
    if not eventos:
        return []

    # Primero fusionar paradas consecutivas para no tener eventos Parada duplicados o continuos
    eventos = _fusionar_paradas_consecutivas(eventos, gestor=gestor)
    if not eventos:
        return []

    # Ordenar eventos por inicio y prioridad de tipo
    def _prioridad_tipo(ev):
        tipo = ev.get("evento", "")
        if tipo == "Comercial": return 0
        if tipo == "Recarga": return 1
        # CRÍTICO: Vacíos que conectan con FnS deben tener alta prioridad para no ser recortados
        # Verificar si hay un FnS después de este vacío
        tipo_str = str(tipo).strip().upper()
        if tipo_str == "VACIO":
            # Los vacíos que conectan con FnS deben tener prioridad similar a InS/FnS
            # Esto se manejará en la lógica de solapamiento
            return 2
        if tipo == "Desplazamiento": return 3
        if tipo == "InS" or tipo == "FnS": return 4
        return 5  # Parada y otros

    eventos_ordenados = sorted(
        eventos,
        key=lambda x: (x.get("inicio", 0), _prioridad_tipo(x), x.get("fin", 0))
    )

    eventos_salida: List[Dict[str, Any]] = []
    for actual in eventos_ordenados:
        if not eventos_salida:
            eventos_salida.append(actual)
            continue

        anterior = eventos_salida[-1]
        
        # REGLA DURA: no se permite deduplicación automática.
        if (actual.get("evento") == anterior.get("evento") and
            actual.get("inicio") == anterior.get("inicio") and
            actual.get("fin") == anterior.get("fin") and
            actual.get("origen") == anterior.get("origen") and
            actual.get("destino") == anterior.get("destino")):
            raise ValueError(
                "[NORMALIZACION - REGLA DURA] Evento duplicado exacto detectado. "
                f"evento={actual.get('evento')} {actual.get('origen')}->{actual.get('destino')} "
                f"{actual.get('inicio')}-{actual.get('fin')}"
            )

        inicio_act = actual.get("inicio", 0)
        fin_act = actual.get("fin", inicio_act)
        inicio_ant = anterior.get("inicio", 0)
        fin_ant = anterior.get("fin", inicio_ant)

        if inicio_act < fin_ant:
            # Hay solapamiento
            tipo_act = str(actual.get("evento", "")).strip().upper()
            tipo_ant = str(anterior.get("evento", "")).strip().upper()
            if actual.get("conductor") == 1 or anterior.get("conductor") == 1:
                print(f"[DEBUG Conductor 1] Solapamiento detectado: actual={tipo_act} ({inicio_act}-{fin_act}), anterior={tipo_ant} ({inicio_ant}-{fin_ant})")
            
            # CRÍTICO: InS/FnS nunca deben eliminarse ni recortarse - siempre agregarlos
            if tipo_act in ["INS", "FNS"] or tipo_ant in ["INS", "FNS"]:
                # Si alguno de los eventos es InS/FnS, agregarlo sin modificar
                eventos_salida.append(actual)
                continue
            
            # CRÍTICO: Vacíos que conectan con FnS no deben recortarse
            # Si el evento actual es un Vacio, verificar si hay un FnS después del mismo conductor
            if tipo_act == "VACIO":
                # Verificar si hay un FnS después de este vacío (en los eventos restantes)
                idx_actual = eventos_ordenados.index(actual)
                eventos_restantes = eventos_ordenados[idx_actual + 1:]
                conductor_vacio = actual.get("conductor")
                destino_vacio = actual.get("destino", "")
                hay_fns_despues = any(str(ev.get("evento", "")).strip().upper() == "FNS" and 
                                     ev.get("conductor") == conductor_vacio for ev in eventos_restantes)
                # CRÍTICO: Preservar vacíos que terminan en un depósito (probablemente conectan con FnS)
                # Obtener lista de depósitos del gestor si está disponible, o usar lista vacía
                nombres_depositos_vacio = []
                if gestor and hasattr(gestor, "_nombres_depositos"):
                    nombres_depositos_vacio = gestor._nombres_depositos()
                elif gestor:
                    nombres_depositos_vacio = [gestor.deposito_base] if hasattr(gestor, "deposito_base") else []
                es_vacio_a_deposito = destino_vacio and destino_vacio in nombres_depositos_vacio
                if conductor_vacio == 1:
                    print(f"[DEBUG Conductor 1] Verificando vacío: destino={destino_vacio}, depósitos={nombres_depositos_vacio}, es_vacio_a_deposito={es_vacio_a_deposito}, hay_fns_despues={hay_fns_despues}, gestor_disponible={gestor is not None}")
                if hay_fns_despues or es_vacio_a_deposito:
                    # Hay un FnS después del mismo conductor o el vacío termina en depósito - no recortar el vacío
                    if conductor_vacio == 1:
                        print(f"[DEBUG Conductor 1] Preservando vacío: hay_fns_despues={hay_fns_despues}, es_vacio_a_deposito={es_vacio_a_deposito}")
                    eventos_salida.append(actual)
                    continue
            # CRÍTICO: Si el evento anterior es un Vacio que conecta con FnS, no recortarlo
            if tipo_ant == "VACIO":
                idx_anterior = eventos_ordenados.index(anterior)
                eventos_restantes_ant = eventos_ordenados[idx_anterior + 1:]
                conductor_vacio_ant = anterior.get("conductor")
                destino_vacio_ant = anterior.get("destino", "")
                hay_fns_despues_ant = any(str(ev.get("evento", "")).strip().upper() == "FNS" and 
                                          ev.get("conductor") == conductor_vacio_ant for ev in eventos_restantes_ant)
                # También verificar si el vacío anterior termina en un depósito
                nombres_depositos_vacio_ant = []
                if gestor and hasattr(gestor, "_nombres_depositos"):
                    nombres_depositos_vacio_ant = gestor._nombres_depositos()
                elif gestor:
                    nombres_depositos_vacio_ant = [gestor.deposito_base] if hasattr(gestor, "deposito_base") else []
                es_vacio_ant_a_deposito = destino_vacio_ant and destino_vacio_ant in nombres_depositos_vacio_ant
                if hay_fns_despues_ant or es_vacio_ant_a_deposito:
                    # El anterior es un vacío que conecta con FnS - no recortarlo, agregar el actual sin modificar
                    if conductor_vacio_ant == 1:
                        print(f"[DEBUG Conductor 1] Preservando vacío anterior: hay_fns_despues={hay_fns_despues_ant}, es_vacio_a_deposito={es_vacio_ant_a_deposito}, destino={destino_vacio_ant}")
                    eventos_salida.append(actual)
                    continue

            # CRÍTICO: Antes de recortar, verificar si alguno de los eventos es un vacío que termina en depósito
            # Estos vacíos no deben recortarse porque conectan con FnS
            nombres_depositos_check = []
            if gestor and hasattr(gestor, "_nombres_depositos"):
                nombres_depositos_check = gestor._nombres_depositos()
            elif gestor:
                nombres_depositos_check = [gestor.deposito_base] if hasattr(gestor, "deposito_base") else []
            
            es_vacio_act_a_deposito = tipo_act == "VACIO" and actual.get("destino", "") in nombres_depositos_check
            es_vacio_ant_a_deposito = tipo_ant == "VACIO" and anterior.get("destino", "") in nombres_depositos_check
            
            if (actual.get("conductor") == 1 or anterior.get("conductor") == 1) and (tipo_act == "VACIO" or tipo_ant == "VACIO"):
                print(f"[DEBUG Conductor 1] Verificando preservación: tipo_act={tipo_act}, tipo_ant={tipo_ant}, "
                      f"destino_act={actual.get('destino')}, destino_ant={anterior.get('destino')}, "
                      f"depósitos={nombres_depositos_check}, es_vacio_act={es_vacio_act_a_deposito}, es_vacio_ant={es_vacio_ant_a_deposito}")
            
            if es_vacio_act_a_deposito:
                # El actual es un vacío que termina en depósito - preservarlo sin recortar
                if actual.get("conductor") == 1:
                    print(f"[DEBUG Conductor 1] Preservando vacío actual (termina en depósito)")
                eventos_salida.append(actual)
                continue
            if es_vacio_ant_a_deposito:
                # El anterior es un vacío que termina en depósito - preservarlo SIN RECORTAR
                # Agregar el actual sin modificar (puede solaparse, pero el vacío tiene prioridad)
                if anterior.get("conductor") == 1:
                    print(f"[DEBUG Conductor 1] Preservando vacío anterior (termina en depósito) SIN RECORTAR")
                # NO recortar el anterior - mantenerlo completo
                eventos_salida.append(actual)
                continue
            
            if _prioridad_tipo(actual) < _prioridad_tipo(anterior):
                # El actual tiene más prioridad. Recortar o eliminar el anterior.
                # CRÍTICO: Si el anterior es un vacío que termina en depósito, NO recortarlo
                if not es_vacio_ant_a_deposito:
                    anterior["fin"] = inicio_act
                    if anterior["fin"] <= anterior["inicio"]:
                        eventos_salida.pop()
                        # Re-evaluar contra el nuevo "anterior"
                        if eventos_salida:
                            # Recursión simple para re-chequear contra el anterior del anterior
                            temp_list = eventos_salida + [actual]
                            eventos_salida = _normalizar_eventos_bus(temp_list, verbose, gestor)
                            continue
                eventos_salida.append(actual)
            else:
                # El anterior tiene más prioridad o igual. Recortar el actual.
                if fin_act <= fin_ant:
                    # El actual está contenido en el anterior, omitir.
                    continue
                actual["inicio"] = fin_ant
                if actual["fin"] > actual["inicio"]:
                    eventos_salida.append(actual)
        else:
            eventos_salida.append(actual)

    return eventos_salida


def _ordenar_eventos_para_normalizar(eventos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _prioridad(ev: Dict[str, Any]) -> int:
        return 1 if ev.get("evento") == "Parada" else 0

    return sorted(
        eventos,
        key=lambda ev: (
            ev.get("inicio", 0),
            _prioridad(ev),
            ev.get("fin", ev.get("inicio", 0)),
            ev.get("evento", ""),
        ),
    )


def _normalizar_eventos_por_clave(
    eventos: List[Dict[str, Any]],
    clave_func,
    verbose: bool = False,
    gestor: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    grupos: Dict[Any, List[Dict[str, Any]]] = {}
    for ev in eventos:
        clave = clave_func(ev)
        grupos.setdefault(clave, []).append(ev)

    resultado: List[Dict[str, Any]] = []
    for clave in sorted(grupos.keys(), key=lambda k: str(k)):
        eventos_ordenados = _ordenar_eventos_para_normalizar(grupos[clave])
        resultado.extend(_normalizar_eventos_bus(eventos_ordenados, verbose, gestor))

    return resultado

