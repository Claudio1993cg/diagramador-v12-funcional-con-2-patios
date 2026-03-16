from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from diagramador_optimizado.utils.time_utils import _to_minutes


class ConfigValidationError(ValueError):
    """Error específico de validación de configuración."""


def autocompletar_configuracion(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Autocompleta estructura mínima para escenarios nuevos sin inventar lógica operativa.
    - Agrega nodos/depósitos faltantes en catálogos.
    - Completa matrices de vacíos/desplazamientos faltantes como deshabilitadas.
    - Completa líneas y límites por grupo faltantes con defaults seguros.
    """
    if not isinstance(config, dict):
        return config

    config.setdefault("nodos", [])
    config.setdefault("depositos", [])
    config.setdefault("deposito", "")
    config.setdefault("paradas", {})
    config.setdefault("vacios", {})
    config.setdefault("desplazamientos", {})
    config.setdefault("lineas", {})
    config.setdefault("tipos_bus", {})
    config.setdefault("grupos_lineas", {})
    config.setdefault("limite_jornada_por_grupo_linea", {})

    # Normalizar nodos/depositos
    nodos = _nombres_desde_iterable(config.get("nodos") or [])
    depositos_cfg = config.get("depositos") or []
    depositos_nombres: List[str] = []
    if isinstance(depositos_cfg, list):
        for dep in depositos_cfg:
            if isinstance(dep, dict):
                nombre = str(dep.get("nombre", "")).strip()
                if nombre:
                    depositos_nombres.append(nombre)
    deposito_base = str(config.get("deposito", "") or "").strip()
    if deposito_base:
        depositos_nombres.append(deposito_base)
    if not depositos_nombres and nodos:
        # fallback seguro: usar primer nodo existente como depósito base explícito.
        depositos_nombres.append(nodos[0])
    if depositos_nombres:
        config["deposito"] = depositos_nombres[0]

    # Unificar nodos + depósitos en catálogo
    todos_nombres = sorted(set(nodos + depositos_nombres))
    config["nodos"] = todos_nombres

    # Paradas mínimas por nodo (sin tocar valores existentes)
    paradas = config.get("paradas")
    if not isinstance(paradas, dict):
        paradas = {}
    depositos_set = {d.upper() for d in depositos_nombres}
    for nodo in todos_nombres:
        if nodo.upper() in depositos_set:
            continue
        paradas.setdefault(nodo, {"min": 5, "max": 120})
    config["paradas"] = paradas

    # Completar matrices de conectividad sin habilitar por defecto.
    vacios = config.get("vacios")
    if not isinstance(vacios, dict):
        vacios = {}
    desplaz = config.get("desplazamientos")
    if not isinstance(desplaz, dict):
        desplaz = {}

    for origen in todos_nombres:
        for destino in todos_nombres:
            if origen == destino:
                continue
            key = f"{origen}_{destino}"
            vacios.setdefault(
                key,
                {
                    "habilitado": False,
                    "franjas": [],
                },
            )
            desplaz.setdefault(
                key,
                {
                    "habilitado": False,
                    "tiempo": 0,
                },
            )
    config["vacios"] = vacios
    config["desplazamientos"] = desplaz

    # Completar líneas faltantes referenciadas por grupos.
    lineas = config.get("lineas")
    if not isinstance(lineas, dict):
        lineas = {}
    tipos_disponibles = sorted([str(k).strip() for k in (config.get("tipos_bus") or {}).keys() if str(k).strip()]) or ["A", "B", "BE", "BPAL", "C"]
    grupos = config.get("grupos_lineas")
    if not isinstance(grupos, dict):
        grupos = {}
    for _, lineas_grupo in grupos.items():
        if not isinstance(lineas_grupo, (list, tuple, set)):
            continue
        for linea in lineas_grupo:
            linea_s = str(linea).strip()
            if not linea_s:
                continue
            lineas.setdefault(linea_s, {"tipos_permitidos": tipos_disponibles})
    config["lineas"] = lineas

    # Completar límite de jornada por grupo
    limite_global = int(config.get("limite_jornada", 720) or 720)
    lj_grupo = config.get("limite_jornada_por_grupo_linea")
    if not isinstance(lj_grupo, dict):
        lj_grupo = {}
    for grupo in grupos.keys():
        if str(grupo).strip():
            lj_grupo.setdefault(str(grupo).strip(), limite_global)
    config["limite_jornada_por_grupo_linea"] = lj_grupo

    return config


def _es_entero_no_negativo(valor: Any) -> bool:
    try:
        return int(valor) >= 0
    except Exception:
        return False


def _es_entero_positivo(valor: Any) -> bool:
    try:
        return int(valor) > 0
    except Exception:
        return False


def _nombres_desde_iterable(valores: Iterable[Any]) -> List[str]:
    return [str(v).strip() for v in valores if v is not None and str(v).strip()]


def _validar_nombre_clave(clave: str, errores: List[str], contexto: str) -> Optional[Tuple[str, str]]:
    """
    Valida y normaliza claves ORIGEN_DESTINO de vacíos/desplazamientos.
    Devuelve (origen, destino) en mayúsculas si es válida; agrega error y devuelve None en caso contrario.
    """
    partes = clave.split("_", 1)
    if len(partes) != 2:
        errores.append(f"{contexto} '{clave}' debe tener formato ORIGEN_DESTINO")
        return None
    origen, destino = partes[0].strip().upper(), partes[1].strip().upper()
    if not origen or not destino:
        errores.append(f"{contexto} '{clave}' requiere ORIGEN y DESTINO no vacíos")
        return None
    return origen, destino


def _validar_franjas(
    franjas: List[Dict[str, Any]],
    errores: List[str],
    contexto: str,
) -> None:
    if not franjas:
        errores.append(f"{contexto}: debe contener al menos una franja")
        return
    for idx, franja in enumerate(franjas, 1):
        if not isinstance(franja, dict):
            errores.append(f"{contexto}: franja #{idx} debe ser un objeto")
            continue
        inicio = _to_minutes(franja.get("inicio"))
        fin = _to_minutes(franja.get("fin"))
        if fin <= inicio:
            fin = inicio + 1  # permitir cruces de medianoche ajustando al menos 1 min
        tiempo = franja.get("tiempo")
        km = franja.get("km", 0)
        if not _es_entero_positivo(tiempo):
            errores.append(f"{contexto}: franja #{idx} debe tener 'tiempo' > 0")
        if km is not None:
            try:
                km_val = float(km)
                if km_val < 0:
                    errores.append(f"{contexto}: franja #{idx} km no puede ser negativo ({km})")
            except Exception:
                errores.append(f"{contexto}: franja #{idx} km inválido ({km})")
        if inicio < 0 or fin < 0:
            errores.append(f"{contexto}: franja #{idx} con tiempos negativos")
        if inicio >= 24 * 60 * 2 or fin >= 24 * 60 * 2:
            errores.append(f"{contexto}: franja #{idx} fuera de rango horario razonable")


def validar_configuracion(config: Dict[str, Any]) -> None:
    """
    Valida la configuración sin modificar valores existentes.

    Reglas duras:
    - Estructura mínima correcta (dicts/listas)
    - nodos/paradas/vacíos/desplazamientos consistentes
    - franjas con tiempos válidos
    - depósitos definidos con cupos no negativos

    Lanza ConfigValidationError/ValueError ante cualquier infracción.
    """
    if not isinstance(config, dict):
        raise ConfigValidationError("La configuración debe ser un diccionario.")

    errores: List[str] = []

    # Parámetros básicos
    for campo, checker in (
        ("limite_jornada", _es_entero_positivo),
        ("tiempo_toma", _es_entero_positivo),
    ):
        valor = config.get(campo)
        if not checker(valor):
            errores.append(f"'{campo}' debe ser entero positivo (actual: {valor})")
    # Compatibilidad: max_buses a nivel raíz es opcional.
    # En configuración moderna el cupo vive en depositos[].max_buses.
    max_buses_root = config.get("max_buses")
    if max_buses_root is not None and not _es_entero_positivo(max_buses_root):
        errores.append(f"'max_buses' debe ser entero positivo si se define (actual: {max_buses_root})")

    # Nodos
    nodos = config.get("nodos") or []
    if not isinstance(nodos, list):
        errores.append("'nodos' debe ser una lista")
        nodos = []
    nodos_norm = {n.upper() for n in _nombres_desde_iterable(nodos)}
    if not nodos_norm:
        errores.append("Debe haber al menos un nodo en 'nodos'")
    if len(nodos_norm) != len(_nombres_desde_iterable(nodos)):
        errores.append("Existen nodos duplicados en 'nodos'")

    # Depósitos
    depositos_config = config.get("depositos") or []
    depositos_nombres: List[str] = []
    if depositos_config and not isinstance(depositos_config, list):
        errores.append("'depositos' debe ser una lista de objetos")
        depositos_config = []
    if depositos_config:
        for idx, dep in enumerate(depositos_config, 1):
            if not isinstance(dep, dict):
                errores.append(f"Deposito #{idx} debe ser un objeto")
                continue
            nombre = str(dep.get("nombre", "")).strip()
            if not nombre:
                errores.append(f"Deposito #{idx} requiere 'nombre'")
                continue
            depositos_nombres.append(nombre)
            max_buses = dep.get("max_buses")
            if not _es_entero_positivo(max_buses):
                errores.append(f"Deposito {nombre}: 'max_buses' debe ser entero positivo")
            flota_por_tipo = dep.get("flota_por_tipo", {})
            if flota_por_tipo and isinstance(flota_por_tipo, dict):
                for tipo, cantidad in flota_por_tipo.items():
                    if not _es_entero_no_negativo(cantidad):
                        errores.append(f"Deposito {nombre}: flota {tipo} debe ser >= 0")
    else:
        deposito_unico = config.get("deposito")
        if deposito_unico:
            depositos_nombres.append(str(deposito_unico).strip())
        else:
            errores.append("Se requiere al menos un depósito en 'depositos' o 'deposito'")
    if len(depositos_nombres) != len(set([d.upper() for d in depositos_nombres])):
        errores.append("Existen depósitos duplicados en 'depositos'")

    nodos_y_depositos = nodos_norm.union({d.upper() for d in depositos_nombres})

    # Paradas
    paradas = config.get("paradas", {}) or {}
    if not isinstance(paradas, dict):
        errores.append("'paradas' debe ser un diccionario")
        paradas = {}
    for nodo, regla in paradas.items():
        if not isinstance(regla, dict):
            errores.append(f"Parada '{nodo}' debe ser un objeto")
            continue
        if str(nodo).strip().upper() not in nodos_y_depositos:
            errores.append(f"Parada '{nodo}' no corresponde a ningún nodo/deposito configurado")
        pmin = regla.get("min", 0)
        pmax = regla.get("max", 0)
        if not _es_entero_no_negativo(pmin) or not _es_entero_no_negativo(pmax):
            errores.append(f"Parada '{nodo}' debe tener min/max enteros no negativos")
        if _es_entero_no_negativo(pmin) and _es_entero_no_negativo(pmax):
            if int(pmin) > int(pmax):
                errores.append(f"Parada '{nodo}' min ({pmin}) no puede ser > max ({pmax})")
        if regla and "habilitado" in regla and not isinstance(regla.get("habilitado"), bool):
            errores.append(f"Parada '{nodo}' campo 'habilitado' debe ser booleano si se define")

    # Vacíos
    vacios = config.get("vacios", {}) or {}
    if not isinstance(vacios, dict):
        errores.append("'vacios' debe ser un diccionario")
        vacios = {}
    for clave, entrada in vacios.items():
        if not isinstance(entrada, dict):
            errores.append(f"Vacío '{clave}' debe ser un objeto")
            continue
        if "habilitado" in entrada and not isinstance(entrada.get("habilitado"), bool):
            errores.append(f"Vacío '{clave}' campo 'habilitado' debe ser booleano si se define")
        origen_destino = _validar_nombre_clave(clave, errores, "Vacío")
        if origen_destino is None:
            continue
        origen, destino = origen_destino
        if origen not in nodos_y_depositos or destino not in nodos_y_depositos:
            errores.append(f"Vacío '{clave}' usa nodos no configurados")
        habilitado = entrada.get("habilitado", True)
        franjas = entrada.get("franjas", entrada if isinstance(entrada, list) else [])
        if habilitado:
            if not isinstance(franjas, list):
                errores.append(f"Vacío '{clave}' franjas debe ser lista")
            else:
                _validar_franjas(franjas, errores, f"Vacío '{clave}'")
        km = entrada.get("km")
        if km is not None:
            try:
                if float(km) < 0:
                    errores.append(f"Vacío '{clave}' km no puede ser negativo")
            except Exception:
                errores.append(f"Vacío '{clave}' km inválido ({km})")

    # Desplazamientos
    desplazamientos = config.get("desplazamientos", {}) or {}
    if not isinstance(desplazamientos, dict):
        errores.append("'desplazamientos' debe ser un diccionario")
        desplazamientos = {}
    for clave, entrada in desplazamientos.items():
        if not isinstance(entrada, dict):
            errores.append(f"Desplazamiento '{clave}' debe ser un objeto")
            continue
        if "habilitado" in entrada and not isinstance(entrada.get("habilitado"), bool):
            errores.append(f"Desplazamiento '{clave}' campo 'habilitado' debe ser booleano si se define")
        origen_destino = _validar_nombre_clave(clave, errores, "Desplazamiento")
        if origen_destino is None:
            continue
        origen, destino = origen_destino
        if origen not in nodos_y_depositos or destino not in nodos_y_depositos:
            errores.append(f"Desplazamiento '{clave}' usa nodos no configurados")
        habilitado = entrada.get("habilitado", False)
        franjas = entrada.get("franjas", [])
        tiempo_directo = entrada.get("tiempo")
        if habilitado:
            if franjas:
                if not isinstance(franjas, list):
                    errores.append(f"Desplazamiento '{clave}' franjas debe ser lista")
                else:
                    _validar_franjas(franjas, errores, f"Desplazamiento '{clave}'")
            elif not _es_entero_positivo(tiempo_directo):
                errores.append(
                    f"Desplazamiento '{clave}' habilitado requiere 'tiempo' > 0 o franjas válidas"
                )
        if tiempo_directo is not None:
            try:
                tiempo_directo_num = int(tiempo_directo)
            except Exception:
                errores.append(f"Desplazamiento '{clave}' 'tiempo' debe ser entero positivo si se define")
            else:
                if tiempo_directo_num < 0:
                    errores.append(f"Desplazamiento '{clave}' 'tiempo' no puede ser negativo")
                # Si está habilitado y no hay franjas, debe ser > 0
                if habilitado and not franjas and tiempo_directo_num <= 0:
                    errores.append(
                        f"Desplazamiento '{clave}' habilitado requiere 'tiempo' > 0 o franjas válidas"
                    )
        km = entrada.get("km")
        if km is not None:
            try:
                if float(km) < 0:
                    errores.append(f"Desplazamiento '{clave}' km no puede ser negativo")
            except Exception:
                errores.append(f"Desplazamiento '{clave}' km inválido ({km})")

    # Tipos de conductor: rangos de ingreso y fin de jornada
    tipos_conductor = config.get("tipos_conductor") or []
    if isinstance(tipos_conductor, list) and tipos_conductor:
        ids_tipo = set()
        for idx, tc in enumerate(tipos_conductor, 1):
            if not isinstance(tc, dict):
                errores.append(f"Tipo conductor #{idx} debe ser un objeto")
                continue
            tid = str(tc.get("id", "") or tc.get("nombre", "")).strip()
            if not tid:
                errores.append(f"Tipo conductor #{idx} requiere 'id' o 'nombre'")
                continue
            if tid in ids_tipo:
                errores.append(f"Tipo conductor duplicado: '{tid}'")
            ids_tipo.add(tid)
            ri = tc.get("rango_ingreso")
            rf = tc.get("rango_fin_jornada")
            if not isinstance(ri, dict) or "min" not in ri or "max" not in ri:
                errores.append(f"Tipo conductor '{tid}': 'rango_ingreso' debe tener min y max (HH:MM)")
            else:
                min_ing = _to_minutes(ri.get("min"))
                max_ing = _to_minutes(ri.get("max"))
                if min_ing < 0 or max_ing < 0:
                    errores.append(f"Tipo conductor '{tid}': rango_ingreso con horarios inválidos")
                elif min_ing > max_ing:
                    errores.append(f"Tipo conductor '{tid}': rango_ingreso min no puede ser mayor que max")
            if not isinstance(rf, dict) or "min" not in rf or "max" not in rf:
                errores.append(f"Tipo conductor '{tid}': 'rango_fin_jornada' debe tener min y max (HH:MM)")
            else:
                min_fin = _to_minutes(rf.get("min"))
                max_fin = _to_minutes(rf.get("max"))
                if min_fin < 0 or max_fin < 0:
                    errores.append(f"Tipo conductor '{tid}': rango_fin_jornada con horarios inválidos")
                elif min_fin > max_fin:
                    errores.append(f"Tipo conductor '{tid}': rango_fin_jornada min no puede ser mayor que max")

    # Grupos de líneas vs líneas declaradas
    lineas_config = config.get("lineas", {}) or {}
    lineas_definidas = {str(k).strip().upper() for k in lineas_config.keys()}
    grupos = config.get("grupos_lineas", {}) or {}
    if grupos and not isinstance(grupos, dict):
        errores.append("'grupos_lineas' debe ser un diccionario de listas")
        grupos = {}
    if grupos and not lineas_definidas:
        errores.append("Se definieron 'grupos_lineas' pero no hay 'lineas' declaradas")
    for grupo, lineas in grupos.items():
        if not isinstance(lineas, (list, set, tuple)):
            errores.append(f"Grupo '{grupo}' debe ser lista o conjunto")
            continue
        if not lineas:
            errores.append(f"Grupo '{grupo}' no puede estar vacío")
        for linea in lineas:
            linea_norm = str(linea).strip().upper()
            if linea_norm and lineas_definidas and linea_norm not in lineas_definidas:
                errores.append(
                    f"Grupo '{grupo}' referencia línea '{linea_norm}' no definida en 'lineas'"
                )

    # Puntos de relevo (opcional): lista de nodos donde se puede hacer relevo (configurable en web/template)
    puntos_relevo = config.get("puntos_relevo")
    if puntos_relevo is not None:
        if not isinstance(puntos_relevo, (list, tuple)):
            errores.append("'puntos_relevo' debe ser una lista")
        else:
            for pr in puntos_relevo:
                pr_norm = str(pr).strip().upper()
                if not pr_norm:
                    continue
                if pr_norm not in nodos_y_depositos:
                    errores.append(f"Punto de relevo '{pr}' no corresponde a ningún nodo configurado")

    if errores:
        raise ConfigValidationError("; ".join(errores))

