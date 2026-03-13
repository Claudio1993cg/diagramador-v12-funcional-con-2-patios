"""
Validaciones OBLIGATORIAS por fase. REGLA DURA: no se negocian.
Si alguna falla, se lanza ValueError para detener el flujo.

- JORNADA MÁXIMA: REGLA EN EXTREMO DURA. Nunca superar el límite, ni por error.
- Fase 1: Ningún bus puede tener viajes solapados (mismo bus, mismo tiempo).
- Fase 2/3: Ningún conductor puede tener dos viajes solapados en el mismo bus.
- Eventos: Ningún conductor+bus puede tener dos Comerciales solapados.
"""
from __future__ import annotations

import collections
from typing import Any, Dict, List, Optional, Tuple


def _minutos_val(ev: Dict, key: str) -> int:
    """Extrae minutos de un evento (inicio/fin pueden ser int o string HH:MM)."""
    val = ev.get(key, 0) or 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str) and ":" in val:
        try:
            h, m = val.strip().split(":")
            return int(h) * 60 + int(m)
        except (ValueError, TypeError):
            pass
    return 0


def _inicio_fin_viaje(v: Dict) -> Tuple[int, int]:
    """Retorna (inicio, fin) en minutos."""
    ini = v.get("inicio", 0) or 0
    fin = v.get("fin", ini) or ini
    return int(ini), int(fin)


def _duracion_minutos(inicio_min: int, fin_min: int) -> int:
    """Duración en minutos entre dos instantes (0-1439). Si fin < inicio se asume cruce de medianoche (+1440)."""
    d = int(fin_min) - int(inicio_min)
    if d < 0:
        d += 1440
    return d


def validar_turnos_limite_jornada(
    turnos: List[Dict[str, Any]],
    limite_jornada: int,
) -> None:
    """
    REGLA EN EXTREMO DURA: Ningún turno puede superar limite_jornada (minutos).
    Nunca se debe superar, ni por error. Lanza ValueError y detiene el flujo si hay violación.
    """
    if limite_jornada <= 0:
        return
    limite_jornada = int(limite_jornada)
    violaciones: List[Tuple[int, int, int, int, int]] = []  # (idx_turno, inicio, fin, duracion, limite_aplicable)
    for t_idx, turno in enumerate(turnos):
        try:
            inicio = int(float(turno.get("inicio", 0) or 0))
            fin = int(float(turno.get("fin", 0) or 0))
        except (TypeError, ValueError):
            inicio, fin = 0, 0
        limite_aplicable = int(turno.get("limite_jornada_aplicable", limite_jornada) or limite_jornada)
        duracion = _duracion_minutos(inicio, fin)
        if duracion > limite_aplicable:
            violaciones.append((t_idx + 1, inicio, fin, duracion, limite_aplicable))
    if violaciones:
        print("\n[DEBUG JORNADA] DONDE FALLA: VALIDACIÓN TURNOS (validar_turnos_limite_jornada)")
        print(f"  Límite: {limite_jornada} min. Violaciones: {len(violaciones)}")
        for idx, ini, fin, dur, lim in violaciones[:20]:
            print(f"  Turno {idx}: inicio={ini} min, fin={fin} min -> duracion={dur} min (límite {lim})")
        if len(violaciones) > 20:
            print(f"  ... y {len(violaciones) - 20} más.")
        print("[DEBUG JORNADA] ---\n")
        lineas = [
            f"  Turno {idx}: inicio={ini} min, fin={fin} min -> duracion={dur} min (límite {lim} min)"
            for idx, ini, fin, dur, lim in violaciones[:20]
        ]
        if len(violaciones) > 20:
            lineas.append(f"  ... y {len(violaciones) - 20} más.")
        raise ValueError(
            f"[REGLA EN EXTREMO DURA - LÍMITE JORNADA] {len(violaciones)} conductor(es) superan el límite máximo de jornada ({limite_jornada} min). NUNCA se debe superar.\n"
            + "\n".join(lineas)
        )


def validar_eventos_limite_jornada(
    eventos: List[Dict[str, Any]],
    limite_jornada: int,
    limites_por_conductor: Optional[Dict[Any, int]] = None,
) -> None:
    """
    REGLA EN EXTREMO DURA: Ningún segmento de jornada (cada par InS->FnS) puede superar
    limite_jornada minutos. Un mismo conductor puede tener varios segmentos (varios InS/FnS)
    si se partió por límite de jornada; se valida cada segmento por separado.
    Lanza ValueError si algún segmento supera el límite.
    """
    if limite_jornada <= 0 or not eventos:
        return
    limite_jornada = int(limite_jornada)
    por_conductor: Dict[Any, List[Dict[str, Any]]] = collections.defaultdict(list)
    for ev in eventos:
        c = ev.get("conductor")
        if c is not None and c != "":
            por_conductor[c].append(ev)
    violaciones: List[Tuple[Any, int, int, int, int]] = []  # (conductor, min_ini, max_fin, duracion, limite_aplicable)
    for cid, lista in por_conductor.items():
        lista_ord = sorted(lista, key=lambda e: _minutos_val(e, "inicio"))
        ins_list = [e for e in lista_ord if (str(e.get("evento", "")).strip().upper() == "INS")]
        fns_list = [e for e in lista_ord if (str(e.get("evento", "")).strip().upper() == "FNS")]
        if not ins_list or not fns_list:
            # Sin InS/FnS: usar span total como antes (compatibilidad)
            min_ini = min(_minutos_val(e, "inicio") for e in lista_ord)
            max_fin = max(_minutos_val(e, "fin") for e in lista_ord)
            duracion = _duracion_minutos(min_ini, max_fin)
            lim_cid = int((limites_por_conductor or {}).get(cid, (limites_por_conductor or {}).get(str(cid), limite_jornada)) or limite_jornada)
            if duracion > lim_cid:
                violaciones.append((cid, min_ini, max_fin, duracion, lim_cid))
            continue
        # Validar cada segmento (cada par InS -> FnS siguiente)
        ins_ord = sorted(ins_list, key=lambda e: _minutos_val(e, "inicio"))
        fns_ord = sorted(fns_list, key=lambda e: _minutos_val(e, "fin"))
        for i, ins_ev in enumerate(ins_ord):
            if i >= len(fns_ord):
                break
            fns_ev = fns_ord[i]
            # La jornada validada inicia en InS.inicio para alinear con lo exportado.
            ini_seg = _minutos_val(ins_ev, "inicio")
            fin_seg = _minutos_val(fns_ev, "fin")
            duracion = _duracion_minutos(ini_seg, fin_seg)
            lim_cid = int((limites_por_conductor or {}).get(cid, (limites_por_conductor or {}).get(str(cid), limite_jornada)) or limite_jornada)
            if duracion > lim_cid:
                violaciones.append((cid, ini_seg, fin_seg, duracion, lim_cid))
    if violaciones:
        print("\n[DEBUG JORNADA] DONDE FALLA: VALIDACIÓN EVENTOS (validar_eventos_limite_jornada)")
        print(f"  Límite: {limite_jornada} min. Violaciones: {len(violaciones)}")
        for cid, mi, mf, dur, lim in violaciones[:20]:
            print(f"  Conductor {cid}: min_ini={mi} min, max_fin={mf} min -> duracion={dur} min (límite {lim})")
        if len(violaciones) > 20:
            print(f"  ... y {len(violaciones) - 20} más.")
        print("[DEBUG JORNADA] ---\n")
        lineas = [
            f"  Conductor {cid}: min_ini={mi} min, max_fin={mf} min -> duracion={dur} min (límite {lim} min)"
            for cid, mi, mf, dur, lim in violaciones[:20]
        ]
        if len(violaciones) > 20:
            lineas.append(f"  ... y {len(violaciones) - 20} más.")
        raise ValueError(
            f"[REGLA EN EXTREMO DURA - LÍMITE JORNADA EN EVENTOS] {len(violaciones)} conductor(es) superan el límite en eventos ({limite_jornada} min). NUNCA se debe superar.\n"
            + "\n".join(lineas)
        )


def validar_comerciales_todos_asignados(eventos_base: List[Dict[str, Any]]) -> None:
    """
    REGLA DURA: Todo evento Comercial debe tener conductor asignado.
    Lanza RuntimeError si hay alguno sin asignar.
    """
    sin_conductor = [e for e in eventos_base if not e.get("conductor")]
    comerciales_sin = [e for e in sin_conductor if str(e.get("evento", "")).strip().upper() == "COMERCIAL"]
    if comerciales_sin:
        detalles = [
            f"  {e.get('evento')} {e.get('linea','')} {e.get('origen','')}->{e.get('destino','')} {e.get('inicio','')}-{e.get('fin','')}"
            for e in comerciales_sin[:10]
        ]
        raise RuntimeError(
            "Hay Comerciales sin asignar a conductor (obligatorio). "
            "Revise Fase 2/3 o duplicados en Fase 1.\n" + "\n".join(detalles)
        )


def validar_conductores_con_comercial(eventos: List[Dict[str, Any]]) -> None:
    """
    REGLA DURA: todo conductor exportado debe tener al menos un Comercial.
    Evita "conductores fantasma" con solo InS/FnS/Desplazamiento/Parada.
    """
    por_conductor: Dict[Any, List[Dict[str, Any]]] = collections.defaultdict(list)
    for ev in eventos or []:
        c = ev.get("conductor")
        if c is None or c == "":
            continue
        por_conductor[c].append(ev)

    invalidos: List[Any] = []
    for cid, lista in por_conductor.items():
        tiene_comercial = any(
            str(e.get("evento", "")).strip().upper() == "COMERCIAL"
            for e in lista
        )
        if not tiene_comercial:
            invalidos.append(cid)

    if invalidos:
        muestra = ", ".join(str(x) for x in invalidos[:20])
        if len(invalidos) > 20:
            muestra += f" ... (+{len(invalidos)-20})"
        raise RuntimeError(
            "Se detectaron conductores sin viaje Comercial, lo cual no está permitido. "
            f"Conductores: {muestra}"
        )


def validar_fase1_sin_solapamiento_bloques(bloques_bus: List[List[Dict[str, Any]]]) -> None:
    """
    REGLA DURA: En cada bloque (bus), ningún par de viajes puede solaparse.
    Lanza ValueError si se detecta solapamiento.
    """
    for bus_idx, bloque in enumerate(bloques_bus):
        if not bloque:
            continue
        ordenados = sorted(bloque, key=lambda x: (_inicio_fin_viaje(x)[0], _inicio_fin_viaje(x)[1]))
        for i in range(1, len(ordenados)):
            _, fin_ant = _inicio_fin_viaje(ordenados[i - 1])
            ini_act, fin_act = _inicio_fin_viaje(ordenados[i])
            if ini_act < fin_ant:
                vid_ant = ordenados[i - 1].get("id") or ordenados[i - 1].get("_tmp_id", "?")
                vid_act = ordenados[i].get("id") or ordenados[i].get("_tmp_id", "?")
                raise ValueError(
                    f"[FASE 1 - REGLA DURA] Bus {bus_idx + 1}: solapamiento detectado. "
                    f"Viaje {vid_ant} termina {fin_ant} min, viaje {vid_act} empieza {ini_act} min (diff={ini_act - fin_ant}). "
                    f"Un bus no puede hacer dos viajes simultáneamente."
                )


def validar_fase2_sin_solapamiento_turnos(
    turnos: List[Dict[str, Any]],
    mapa_viajes: Dict[Any, Dict[str, Any]],
) -> None:
    """
    REGLA DURA: En cada turno, para cada bus, ningún par de tareas (viajes) puede solaparse.
    Lanza ValueError si se detecta solapamiento.
    """
    for t_idx, turno in enumerate(turnos):
        tareas = turno.get("tareas_con_bus", [])
        por_bus: Dict[int, List[Tuple[Any, int, int]]] = {}
        todos: List[Tuple[Any, int, int, int]] = []  # (tid, ini, fin, bus)
        for tid, bus in tareas:
            v = mapa_viajes.get(tid) or mapa_viajes.get(str(tid))
            if not v:
                continue
            ini, fin = _inicio_fin_viaje(v)
            por_bus.setdefault(bus, []).append((tid, ini, fin))
            todos.append((tid, ini, fin, bus))
        for bus, lst in por_bus.items():
            lst.sort(key=lambda x: (x[1], x[2]))
            for i in range(1, len(lst)):
                _, _, fin_ant = lst[i - 1]
                tid_act, ini_act, _ = lst[i]
                if ini_act < fin_ant:
                    raise ValueError(
                        f"[FASE 2 - REGLA DURA] Turno {t_idx + 1} bus {bus + 1}: solapamiento detectado. "
                        f"Viaje anterior termina {fin_ant} min, viaje {tid_act} empieza {ini_act} min. "
                        f"Un conductor no puede tener dos viajes simultáneos en el mismo bus."
                    )

        # REGLA DURA adicional: dentro de un mismo turno, un conductor no puede
        # tener dos viajes solapados aunque sean de buses distintos.
        if len(todos) > 1:
            todos.sort(key=lambda x: (x[1], x[2]))  # ordenar por inicio, fin
            for i in range(1, len(todos)):
                tid_ant, ini_ant, fin_ant, bus_ant = todos[i - 1]
                tid_act, ini_act, fin_act, bus_act = todos[i]
                if ini_act < fin_ant:
                    raise ValueError(
                        f"[FASE 2 - REGLA DURA] Turno {t_idx + 1}: solapamiento global detectado entre buses."
                        f" Viaje {tid_ant} (bus {bus_ant + 1}) termina {fin_ant} min,"
                        f" viaje {tid_act} (bus {bus_act + 1}) empieza {ini_act} min (diff={ini_act - fin_ant}). "
                        f"Un conductor no puede operar dos viajes simultáneos en buses distintos."
                    )


def validar_fase3_sin_solapamiento_turnos(
    turnos: List[Dict[str, Any]],
    mapa_viajes: Dict[Any, Dict[str, Any]],
) -> None:
    """
    REGLA DURA: Igual que Fase 2. Los turnos unidos no pueden tener solapamientos.
    Lanza ValueError si se detecta solapamiento.
    """
    validar_fase2_sin_solapamiento_turnos(turnos, mapa_viajes)


def _to_min(val: Any) -> int:
    """Convierte HH:MM o número a minutos."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip()
    if not s:
        return 0
    try:
        if ":" in s:
            parts = s.split(":")
            return int(parts[0]) * 60 + int(parts[1]) if len(parts) > 1 else int(parts[0]) * 60
        return int(s)
    except Exception:
        return 0


def validar_eventos_sin_solapamiento_conductor_bus(eventos: List[Dict[str, Any]]) -> None:
    """
    REGLA DURA: Para cada (conductor, bus), ningún par de eventos COMERCIAL puede solaparse.
    Lanza ValueError si se detecta solapamiento.
    """
    por_clave: Dict[Tuple[Any, Any], List[Dict]] = {}
    for ev in eventos:
        if str(ev.get("evento", "")).strip().upper() != "COMERCIAL":
            continue
        c, b = ev.get("conductor"), ev.get("bus")
        if c is None or b is None or str(b) == "":
            continue
        try:
            bid = int(b)
        except (TypeError, ValueError):
            continue
        clave = (c, bid)
        por_clave.setdefault(clave, []).append(ev)
    for (c, bid), evs in por_clave.items():
        ordenados = sorted(evs, key=lambda x: (_to_min(x.get("inicio", 0)), _to_min(x.get("fin", 0))))
        for i in range(1, len(ordenados)):
            fin_ant = _to_min(ordenados[i - 1].get("fin", ordenados[i - 1].get("inicio", 0)))
            ini_act = _to_min(ordenados[i].get("inicio", 0))
            if ini_act < fin_ant:
                raise ValueError(
                    f"[EVENTOS - REGLA DURA] Conductor {c} bus {bid}: solapamiento Comercial-Comercial. "
                    f"Anterior termina {fin_ant} min, siguiente empieza {ini_act} min (diff={ini_act - fin_ant}). "
                    f"Un conductor no puede operar dos Comerciales simultáneos en el mismo bus."
                )


def validar_vacios_con_duracion_valida(eventos: List[Dict[str, Any]]) -> None:
    """
    REGLA DURA: un vacío entre nodos distintos debe tener duración > 0.
    """
    invalidos: List[Tuple[str, str, int, int, Any, Any]] = []
    def _mismo_nodo_flexible(a: str, b: str) -> bool:
        aa = (a or "").strip().upper()
        bb = (b or "").strip().upper()
        if not aa or not bb:
            return False
        if aa == bb:
            return True
        aa_s = " ".join(aa.replace("DEPOSITO", "").split())
        bb_s = " ".join(bb.replace("DEPOSITO", "").split())
        return bool(aa_s and bb_s and aa_s == bb_s)

    for ev in eventos or []:
        if str(ev.get("evento", "")).strip().upper() != "VACIO":
            continue
        origen = str(ev.get("origen", "") or "").strip()
        destino = str(ev.get("destino", "") or "").strip()
        ini = _to_min(ev.get("inicio", 0))
        fin = _to_min(ev.get("fin", 0))
        if origen and destino and (not _mismo_nodo_flexible(origen, destino)) and fin <= ini:
            invalidos.append((origen, destino, ini, fin, ev.get("bus"), ev.get("conductor")))

    if invalidos:
        ejemplos = "\n".join(
            f"  {o}->{d} {i}-{f} (bus={b}, conductor={c})"
            for o, d, i, f, b, c in invalidos[:20]
        )
        if len(invalidos) > 20:
            ejemplos += f"\n  ... y {len(invalidos)-20} más."
        raise ValueError(
            "[EVENTOS - REGLA DURA] Se detectaron vacíos con duración inválida (0 o negativa) "
            "entre nodos distintos.\n"
            + ejemplos
        )
