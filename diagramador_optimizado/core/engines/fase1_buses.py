from __future__ import annotations

import bisect
import collections
import math
import random
import uuid
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple

from diagramador_optimizado.core.builders.eventos_bus import construir_eventos_bus
from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.validaciones_fase import validar_fase1_sin_solapamiento_bloques
from diagramador_optimizado.core.builders.recarga import planificar_recargas_bloques
from diagramador_optimizado.utils.time_utils import formatear_hora


def _vid(v: Dict[str, Any]) -> Any:
    """ID canónico para deduplicación de viajes. Maneja id=0 correctamente (no usa 'or')."""
    vid = v.get("id")
    if vid is None:
        vid = v.get("_tmp_id")
    return vid


def _intentar_agregar_viaje_a_bus(
    bus: Dict[str, Any], viaje: Dict[str, Any], viajes_asignados: Set[Any]
) -> bool:
    """
    Intenta agregar un viaje a un bus. Valida inicio/fin, duplicados y solapamiento.
    Usado por resolver_diagramacion_buses; expuesto para tests unitarios.

    NOTA IMPORTANTE:
    - No se debe usar el patrón ``viaje.get("id") or viaje.get("_tmp_id")`` porque
      considera el identificador ``0`` como falso y provoca que todos los viajes con
      id=0 compartan el mismo `_tmp_id` (o queden sin id), permitiendo que se asignen
      varias veces a distintos buses.
    - Aquí tratamos explícitamente el caso ``id == 0`` como un identificador válido.

    Returns:
        True si se agregó correctamente, False si se rechazó (duplicado, inválido o solapamiento).
    """
    # Respetar id=0 como identificador válido; solo caer a _tmp_id si id es None
    viaje_id = viaje.get("id")
    if viaje_id is None:
        viaje_id = viaje.get("_tmp_id")
    if viaje_id in viajes_asignados:
        print(f"    ERROR: Viaje {viaje_id} ya asignado - DUPLICACIÓN DETECTADA")
        return False

    inicio_viaje = viaje.get("inicio")
    fin_viaje = viaje.get("fin")
    if inicio_viaje is None or fin_viaje is None:
        print(f"    ERROR: Viaje {viaje_id} sin inicio o fin definido")
        return False
    if inicio_viaje >= fin_viaje:
        print(f"    ERROR: Viaje {viaje_id} inicio >= fin ({inicio_viaje} >= {fin_viaje})")
        return False

    for evento_existente in bus.get("viajes", []):
        inicio_existente = evento_existente.get("inicio")
        fin_existente = evento_existente.get("fin")
        if inicio_existente is None or fin_existente is None:
            continue
        if not (fin_existente <= inicio_viaje or inicio_existente >= fin_viaje):
            print(
                f"    ERROR: Viaje {viaje_id} RECHAZADO por solapamiento "
                f"(viaje {inicio_viaje}-{fin_viaje}, existente {inicio_existente}-{fin_existente})"
            )
            return False

    bus["viajes"].append(viaje)
    bus["ultimo_viaje"] = viaje
    viajes_asignados.add(viaje_id)
    return True


def _obtener_regla_parada(paradas_dict: Dict[str, Dict], nombre_nodo: str) -> Optional[Dict]:
    """Obtiene la regla de parada para un nodo, con matching flexible (ej. PLAZA PUENTE ALTO -> PLAZA PUENTE)."""
    if not nombre_nodo or not paradas_dict:
        return None
    clave = str(nombre_nodo).strip().upper()
    regla = paradas_dict.get(clave)
    if regla is not None:
        return regla
    for k, v in paradas_dict.items():
        if k in clave or clave in k:
            return v
    return None


def _analizar_demanda_horaria(
    viajes_comerciales: List[Dict[str, Any]],
    ventana_minutos: int = 60,
) -> Dict[int, int]:
    """
    Analiza la demanda horaria agrupando viajes en ventanas de tiempo.
    
    Args:
        viajes_comerciales: Lista de viajes comerciales
        ventana_minutos: Tamaño de la ventana en minutos (default: 60 min = 1 hora)
    
    Returns:
        Diccionario con {inicio_ventana: cantidad_viajes} donde inicio_ventana está en minutos desde medianoche
    """
    demanda: Dict[int, int] = collections.defaultdict(int)
    
    for viaje in viajes_comerciales:
        inicio = viaje.get("inicio", 0)
        # Agrupar en ventanas de tiempo
        ventana = (inicio // ventana_minutos) * ventana_minutos
        demanda[ventana] += 1
    
    return dict(demanda)


def _identificar_periodos_baja_demanda(
    demanda_horaria: Dict[int, int],
    umbral_baja_demanda: int = 2,
    duracion_minima_periodo: int = 120,  # 2 horas mínimo
) -> List[Tuple[int, int]]:
    """
    Identifica períodos de baja demanda donde se pueden dejar buses en standby.
    
    Args:
        demanda_horaria: Diccionario con demanda por ventana de tiempo
        umbral_baja_demanda: Cantidad máxima de viajes por ventana para considerar baja demanda
        duracion_minima_periodo: Duración mínima del período en minutos para considerar standby
    
    Returns:
        Lista de tuplas (inicio_periodo, fin_periodo) en minutos desde medianoche
    """
    periodos: List[Tuple[int, int]] = []
    
    if not demanda_horaria:
        return periodos
    
    # Obtener todas las ventanas ordenadas
    ventanas_ordenadas = sorted(demanda_horaria.keys())
    
    # Buscar períodos consecutivos de baja demanda
    inicio_periodo = None
    for ventana in ventanas_ordenadas:
        cantidad = demanda_horaria[ventana]
        
        if cantidad <= umbral_baja_demanda:
            if inicio_periodo is None:
                inicio_periodo = ventana
        else:
            # Fin del período de baja demanda
            if inicio_periodo is not None:
                fin_periodo = ventana
                duracion = fin_periodo - inicio_periodo
                if duracion >= duracion_minima_periodo:
                    periodos.append((inicio_periodo, fin_periodo))
                inicio_periodo = None
    
    # Si el período continúa hasta el final del día
    if inicio_periodo is not None:
        fin_periodo = 1440  # Fin del día
        duracion = fin_periodo - inicio_periodo
        if duracion >= duracion_minima_periodo:
            periodos.append((inicio_periodo, fin_periodo))
    
    return periodos


def _es_periodo_standby(
    hora_actual: int,
    periodos_standby: List[Tuple[int, int]],
) -> bool:
    """
    Verifica si la hora actual está en un período de standby.
    
    Args:
        hora_actual: Hora actual en minutos desde medianoche
        periodos_standby: Lista de períodos de standby (inicio, fin)
    
    Returns:
        True si está en período de standby
    """
    for inicio, fin in periodos_standby:
        if inicio <= hora_actual < fin:
            return True
    return False


def _preparar_indice_viajes_por_inicio(
    viajes_ordenados: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Precalcula índices por hora de inicio para acelerar búsquedas sucesivas.
    Mantiene compatibilidad con la firma existente de _buscar_siguiente_viaje_desde_hora.
    """
    inicios = [int(v.get("inicio", 0) or 0) for v in viajes_ordenados]
    return {"inicios": inicios}


def _buscar_siguiente_viaje_desde_hora(
    viajes_comerciales: List[Dict[str, Any]],
    hora_desde: int,
    tipo_bus: Optional[str] = None,
    indices: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Busca el siguiente viaje disponible desde una hora específica.
    
    Args:
        viajes_comerciales: Lista de viajes comerciales
        hora_desde: Hora desde la cual buscar
        tipo_bus: Tipo de bus requerido (opcional)
    
    Returns:
        El siguiente viaje disponible o None
    """
    # Usar índice precalculado (O(log n) + delta) si está disponible
    if indices and indices.get("inicios") and len(indices["inicios"]) == len(viajes_comerciales):
        posicion = bisect.bisect_left(indices["inicios"], hora_desde)
        for idx in range(posicion, len(viajes_comerciales)):
            viaje = viajes_comerciales[idx]
            if tipo_bus and viaje.get("tipo_bus") != tipo_bus:
                continue
            return viaje
        return None

    # Fallback original O(n) si no hay índice
    mejor_viaje = None
    mejor_hora = None
    
    for viaje in viajes_comerciales:
        inicio = viaje.get("inicio", 0)
        if inicio < hora_desde:
            continue
        
        # Verificar tipo de bus si se especifica
        if tipo_bus and viaje.get("tipo_bus") != tipo_bus:
            continue
        
        if mejor_hora is None or inicio < mejor_hora:
            mejor_hora = inicio
            mejor_viaje = viaje
    
    return mejor_viaje

try:
    from ortools.sat.python import cp_model

    _HAVE_CP_SAT = True
except Exception:
    cp_model = None  # type: ignore
    _HAVE_CP_SAT = False


def resolver_diagramacion_buses(
    config: Dict[str, Any],
    viajes_comerciales: List[Dict[str, Any]],
    gestor: GestorDeLogistica,
    random_seed: Optional[int] = None,
    verbose: bool = False,
) -> Tuple[List[List[Dict[str, Any]]], List[List[Dict[str, Any]]], str]:
    """
    Enrutador principal. Produce bloques y eventos_bus completos (InS, Vacio, Comercial, Parada, Recarga, FnS).
    Por defecto usa la lógica greedy heredada. Si config['usar_cp_sat_fase1'] usa CP-SAT.
    Returns: (bloques_bus, eventos_bus, status)
    """
    config.pop("_diagnostico_flota_fase1", None)
    permitir_exceso_flota = True
    if config.get("usar_cp_sat_fase1"):
        return _resolver_diagramacion_buses_cp_sat(
            config,
            viajes_comerciales,
            gestor,
            random_seed=random_seed,
            verbose=verbose,
        )

    print("\n" + "=" * 80)
    print("--- FASE 1: Diagramación de Buses POR LÍNEA (Greedy Seguro) ---")
    print("=" * 80)

    n = len(viajes_comerciales)
    if n == 0:
        print("No hay viajes para diagramar.")
        return [], [], "SIN_VIAJES"

    # REGLA DURA: no se aceptan viajes comerciales duplicados de entrada.
    _clave_viaje = lambda v: (
        v.get("inicio"), v.get("fin"), v.get("linea"), v.get("sentido"),
        (v.get("origen") or "").strip(), (v.get("destino") or "").strip(),
    )
    _vistos_idx: Dict[Tuple[Any, Any, Any, Any, Any, Any], int] = {}
    _duplicados: List[Tuple[int, int, Tuple[Any, Any, Any, Any, Any, Any]]] = []
    for idx_v, v in enumerate(viajes_comerciales):
        clave = _clave_viaje(v)
        if clave in _vistos_idx:
            _duplicados.append((_vistos_idx[clave] + 1, idx_v + 1, clave))
        else:
            _vistos_idx[clave] = idx_v
    if _duplicados:
        ejemplos = "\n".join(
            f"  viaje[{actual}] duplicado de viaje[{previo}] -> {clave}"
            for previo, actual, clave in _duplicados[:10]
        )
        raise ValueError(
            f"[FASE 1 - REGLA DURA] Se detectaron {len(_duplicados)} viajes duplicados. "
            "No se permite deduplicación automática.\n"
            + ejemplos
        )

    depositos_config = getattr(gestor, "depositos_config", None) or []
    if not depositos_config and config.get("deposito"):
        depositos_config = [
            type(
                "DepositoTemporal",
                (),
                {
                    "nombre": config.get("deposito"),
                    "max_buses": config.get("max_buses", 200),
                    "flota_por_tipo": {},
                },
            )
        ]
    if not depositos_config:
        print("ERROR: No hay depósitos configurados. Imposible continuar.")
        return [], [], "ERROR_CONFIGURACION"

    nombres_depositos = [dep.nombre for dep in depositos_config if getattr(dep, "nombre", None)]
    nombres_depositos_upper = {nombre.upper() for nombre in nombres_depositos}
    deposito_a_terminal = _terminal_upper_por_deposito(nombres_depositos)
    paradas_dict = getattr(gestor, "paradas_dict", {k.upper(): v for k, v in config.get("paradas", {}).items()})
    tiempo_min_deposito = getattr(gestor, "tiempo_min_deposito", 5)
    aproximacion_salida_dep = getattr(gestor, "_t_de_dep_aprox", 30)
    # REGLA DURA: usar el total de max_buses de los depósitos configurados,
    # o el valor raíz del config como fallback.
    max_buses_depositos = sum(getattr(d, "max_buses", 0) for d in depositos_config if getattr(d, "max_buses", 0))
    max_buses = max_buses_depositos if max_buses_depositos > 0 else config.get("max_buses", 200)

    print(f"Depósitos disponibles: {sorted(nombres_depositos)}")
    if len(nombres_depositos) > 1:
        print(f"[INFO] MÚLTIPLES DEPÓSITOS configurados - El sistema seleccionará automáticamente el mejor depósito para cada viaje")
    print(f"Límite máximo de buses (REGLA DURA): {max_buses}")

    @lru_cache(maxsize=50000)
    def _buscar_vacio_cached(origen: str, destino: str, referencia: int) -> Tuple[Optional[int], int]:
        try:
            referencia_int = int(referencia)
        except Exception:
            referencia_int = 0
        return gestor.buscar_tiempo_vacio(origen, destino, referencia_int)

    def _buscar_vacio(origen: str, destino: str, referencia: int) -> Tuple[Optional[int], int]:
        """
        Wrapper cacheado para minimizar llamadas repetidas a tiempos de vacío.
        """
        return _buscar_vacio_cached(str(origen).strip(), str(destino).strip(), int(referencia))

    cache_ruta_deposito: Dict[Tuple[str, str, int, int], Optional[Dict[str, Any]]] = {}

    def _ruta_via_deposito(
        viaje_origen: Dict[str, Any],
        viaje_destino: Dict[str, Any],
        referencia: int,
    ) -> Optional[Dict[str, Any]]:
        clave_cache = (
            str(viaje_origen.get("destino", "")).upper(),
            str(viaje_destino.get("origen", "")).upper(),
            int(viaje_destino.get("inicio", 0) or 0),
            int(referencia),
        )
        if clave_cache in cache_ruta_deposito:
            return cache_ruta_deposito[clave_cache]
        mejor: Optional[Dict[str, Any]] = None
        for dep in depositos_config:
            nombre = getattr(dep, "nombre", None)
            if not nombre:
                continue
            t_ida, km_ida = _buscar_vacio(viaje_origen["destino"], nombre, referencia)
            if t_ida is None:
                continue
            t_vuelta, km_vuelta = _buscar_vacio(
                nombre,
                viaje_destino["origen"],
                max(viaje_destino["inicio"] - aproximacion_salida_dep, 0),
            )
            if t_vuelta is None:
                continue
            llegada = referencia + t_ida
            salida_min = llegada + tiempo_min_deposito
            if salida_min + t_vuelta > viaje_destino["inicio"]:
                continue
            km_totales = (km_ida or 0) + (km_vuelta or 0)
            opcion = {
                "deposito": nombre,
                "km_totales": km_totales,
                "tiempo_total": t_ida + tiempo_min_deposito + t_vuelta,
            }
            if mejor is None or km_totales < mejor["km_totales"]:
                mejor = opcion
        cache_ruta_deposito[clave_cache] = mejor
        return mejor

    # Verificar configuración de grupos de líneas para interlineados
    interlineado_global = getattr(gestor, "interlineado_global", False)
    respetar_grupos = getattr(gestor, "respetar_grupos_lineas", False)
    grupos_lineas = getattr(gestor, "grupos_lineas", {})
    
    # REGLA DURA: respetar_grupos_lineas obliga procesamiento por grupo (bus solo opera en su grupo)
    if respetar_grupos and grupos_lineas:
        print(f"Respetando grupos de línea (regla dura): Procesando por grupo")
        viajes_por_grupo: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        for viaje in viajes_comerciales:
            linea = viaje.get("linea") or "SIN_LINEA"
            grupo = gestor.obtener_grupo_linea(linea)
            if grupo:
                viajes_por_grupo[grupo].append(viaje)
            else:
                viajes_por_grupo[f"LINEA_INDIVIDUAL_{linea}"].append(viaje)
    elif interlineado_global and not (respetar_grupos and grupos_lineas):
        # Procesar todas las líneas juntas (interlineado global) solo si no hay regla de grupos
        print(f"Interlineado GLOBAL habilitado: Procesando todas las líneas juntas")
        viajes_por_grupo: Dict[str, List[Dict[str, Any]]] = {"TODAS_LAS_LINEAS": viajes_comerciales}
    elif grupos_lineas:
        # Procesar por grupos de líneas
        print(f"Grupos de líneas configurados: {len(grupos_lineas)} grupos")
        viajes_por_grupo: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        
        # Agrupar viajes por grupo de líneas
        for viaje in viajes_comerciales:
            linea = viaje.get("linea") or "SIN_LINEA"
            grupo = gestor.obtener_grupo_linea(linea)
            
            if grupo:
                # La línea pertenece a un grupo
                viajes_por_grupo[grupo].append(viaje)
            else:
                # La línea no pertenece a ningún grupo, procesarla individualmente
                viajes_por_grupo[f"LINEA_INDIVIDUAL_{linea}"].append(viaje)
    else:
        # Procesar por línea individual (comportamiento original)
        print(f"Sin grupos configurados: Procesando por línea individual")
        viajes_por_grupo: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        for viaje in viajes_comerciales:
            linea = viaje.get("linea") or "SIN_LINEA"
            viajes_por_grupo[linea].append(viaje)

    print(f"Grupos/Líneas a procesar: {len(viajes_por_grupo)}")

    # Garantizar ids únicos: si un viaje no tiene "id", generar _tmp_id temporal para deduplicación
    for viaje in viajes_comerciales:
        if viaje.get("id") is None and viaje.get("_tmp_id") is None:
            viaje["_tmp_id"] = str(uuid.uuid4())

    todos_los_bloques: List[List[Dict[str, Any]]] = []
    total_reutilizaciones = 0

    # REGLA CRÍTICA: Rastrear viajes asignados para evitar duplicaciones
    viajes_asignados: Set[Any] = set()
    diagnostico_factibilidad: List[Dict[str, Any]] = []

    def _agregar_viaje_a_bus(bus: Dict[str, Any], viaje: Dict[str, Any]) -> bool:
        """Wrapper que delega en _intentar_agregar_viaje_a_bus."""
        return _intentar_agregar_viaje_a_bus(bus, viaje, viajes_asignados)

    def _registrar_rechazo_factibilidad(
        viaje: Dict[str, Any],
        grupo: str,
        total_buses_actual: int,
        motivo: str,
    ) -> None:
        inicio = int(viaje.get("inicio", 0) or 0)
        fin = int(viaje.get("fin", inicio) or inicio)
        bucket = (inicio // 30) * 30
        franja = f"{formatear_hora(bucket)}-{formatear_hora(bucket + 30)}"
        candidatos_dep: List[Tuple[str, Optional[int], int]] = []
        for dep in depositos_config:
            nombre_dep = getattr(dep, "nombre", None) or (dep if isinstance(dep, dict) else {}).get("nombre")
            if not nombre_dep:
                continue
            t_dep, km_dep = _buscar_vacio(nombre_dep, viaje.get("origen", ""), inicio)
            candidatos_dep.append((str(nombre_dep), t_dep, km_dep or 0))

        candidatos_dep.sort(key=lambda x: (x[1] is None, x[1] if x[1] is not None else 10**9))
        mejor_dep = next((c for c in candidatos_dep if c[1] is not None), None)
        diagnostico_factibilidad.append(
            {
                "viaje_id": _vid(viaje),
                "grupo": grupo,
                "linea": viaje.get("linea", "SIN_LINEA"),
                "origen": viaje.get("origen", ""),
                "destino": viaje.get("destino", ""),
                "inicio": inicio,
                "fin": fin,
                "franja": franja,
                "total_buses_actual": total_buses_actual,
                "max_buses": max_buses,
                "mejor_deposito": (mejor_dep[0] if mejor_dep else None),
                "mejor_t_vacio": (mejor_dep[1] if mejor_dep else None),
                "candidatos_deposito": candidatos_dep[:5],
                "motivo": motivo,
            }
        )

    # Aleatorizar orden de grupos cuando hay seed (permite explorar diferentes soluciones por iteración)
    items_grupos = list(viajes_por_grupo.items())
    if random_seed is not None:
        rng = random.Random(random_seed)
        rng.shuffle(items_grupos)
        print(f"[SEED] Orden de grupos aleatorizado con seed={random_seed}")

    for grupo_nombre, viajes_grupo in items_grupos:
        print(f"\nProcesando grupo/línea '{grupo_nombre}' ({len(viajes_grupo)} viajes)...")
        # Filtrar viajes ya asignados: usar _vid() para deduplicación (maneja id=0).
        viajes_grupo_filtrados = [
            v for v in viajes_grupo
            if _vid(v) not in viajes_asignados
        ]
        if len(viajes_grupo_filtrados) != len(viajes_grupo):
            print(f"  ADVERTENCIA: {len(viajes_grupo) - len(viajes_grupo_filtrados)} viajes ya fueron asignados, filtrando...")
        viajes_ordenados = sorted(viajes_grupo_filtrados, key=lambda v: v["inicio"])
        indice_viajes = _preparar_indice_viajes_por_inicio(viajes_ordenados)
        
        # OPTIMIZACIÓN: Analizar demanda horaria para identificar períodos de standby
        demanda_horaria = _analizar_demanda_horaria(viajes_ordenados, ventana_minutos=60)
        periodos_standby = _identificar_periodos_baja_demanda(
            demanda_horaria,
            umbral_baja_demanda=2,  # Máximo 2 viajes por hora para considerar baja demanda
            duracion_minima_periodo=120,  # Mínimo 2 horas para considerar standby
        )
        if periodos_standby and verbose:
            print(f"  Períodos de baja demanda identificados: {len(periodos_standby)}")
            for inicio, fin in periodos_standby:
                print(f"    - {formatear_hora(inicio)} a {formatear_hora(fin)}")
        
        buses_grupo: List[Dict[str, Any]] = []
        buses_standby: List[Dict[str, Any]] = []  # Buses en standby (en depósito esperando)
        reutilizaciones_grupo = 0

        def _primer_comercial(viajes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            for v in viajes:
                if v.get("evento") not in ("recarga", "vacio"):
                    return v
            return None

        def _ultimo_comercial(viajes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            for v in reversed(viajes):
                if v.get("evento") not in ("recarga", "vacio"):
                    return v
            return None
        
        # Obtener tipos permitidos: partir de tipos disponibles e intersectar por línea.
        # Fallback: si gestor.tipos_bus_disponibles no existe, usar set().
        tipos_permitidos_grupo: Set[str] = set(
            getattr(gestor, "tipos_bus_disponibles", None) or []
        )
        for viaje in viajes_grupo:
            linea_viaje = viaje.get("linea") or "SIN_LINEA"
            tipos_linea = gestor.tipos_permitidos_para_linea(linea_viaje) if hasattr(gestor, "tipos_permitidos_para_linea") else []
            tipos_permitidos_grupo &= set(tipos_linea or [])
        
        tipo_grupo_estimado = list(tipos_permitidos_grupo)[0] if tipos_permitidos_grupo else None

        for viaje in viajes_ordenados:
            # OPTIMIZACIÓN: Intentar reactivar buses desde standby primero
            mejor_bus_standby: Optional[Dict[str, Any]] = None
            mejor_costo_standby: Optional[float] = None
            
            for bus_standby in buses_standby:
                hora_standby = bus_standby.get("hora_disponible", 0)
                if hora_standby > viaje["inicio"]:
                    continue
                
                # Verificar si el bus en standby puede tomar este viaje
                tipo_bus_standby = bus_standby.get("tipo_bus") or tipo_grupo_estimado
                if tipo_bus_standby:
                    viaje_temp_standby = {**viaje, "tipo_bus": tipo_bus_standby}
                else:
                    viaje_temp_standby = viaje
                
                # El bus está en depósito, calcular costo desde depósito
                deposito_standby = bus_standby.get("deposito_standby", gestor.deposito_base)
                tiempo_vacio_standby, km_vacio_standby = _buscar_vacio(
                    deposito_standby,
                    viaje["origen"],
                    hora_standby
                )
                
                # Validar tiempo de vacío: debe existir, ser >0 y razonable (< 24h)
                tiempo_ok = (
                    tiempo_vacio_standby is not None
                    and tiempo_vacio_standby > 0
                    and tiempo_vacio_standby < 24 * 60
                )
                if tiempo_ok and hora_standby + tiempo_vacio_standby <= viaje["inicio"]:
                    costo_standby = float(km_vacio_standby or 0)
                    if mejor_bus_standby is None or (mejor_costo_standby is None or costo_standby < mejor_costo_standby):
                        mejor_bus_standby = bus_standby
                        mejor_costo_standby = costo_standby
            
            mejor_idx: Optional[int] = None
            mejor_costo: Optional[float] = None

            for idx, bus in enumerate(buses_grupo):
                ultimo = bus.get("ultimo_viaje")
                # Unificar hora_disponible: usar el mayor entre el campo y el fin del último evento
                ultimo_evento = bus["viajes"][-1] if bus.get("viajes") else None
                hora_disp = bus.get("hora_disponible", 0)
                if ultimo_evento and ultimo_evento.get("fin") is not None:
                    hora_disp = max(hora_disp, ultimo_evento["fin"])

                if hora_disp > viaje["inicio"]:
                    continue

                # REGLA: Verificar si las líneas pueden interlinear
                # Si el bus ya tiene viajes asignados, verificar que la línea del nuevo viaje
                # pueda interlinear con la línea del último viaje del bus
                if bus.get("ultimo_viaje"):
                    linea_ultimo_viaje = bus["ultimo_viaje"].get("linea")
                    linea_nuevo_viaje = viaje.get("linea")
                    
                    # Si las líneas son diferentes, verificar si pueden interlinear
                    if linea_ultimo_viaje and linea_nuevo_viaje and linea_ultimo_viaje != linea_nuevo_viaje:
                        pueden = gestor.pueden_interlinear(linea_ultimo_viaje, linea_nuevo_viaje)
                        if not pueden:
                            # No pueden interlinear - saltar este bus
                            if verbose:
                                print(f"    [INTERLINEADO] Viaje {viaje.get('id')} línea {linea_nuevo_viaje} "
                                      f"NO puede interlinear con línea {linea_ultimo_viaje} del bus {idx}")
                            continue
                        elif verbose:
                            print(f"    [INTERLINEADO] Viaje {viaje.get('id')} línea {linea_nuevo_viaje} "
                                  f"PUEDE interlinear con línea {linea_ultimo_viaje} del bus {idx}")

                # Verificar si el bus puede tomar el viaje considerando recarga eléctrica
                # Usar tipo de línea si el viaje no tiene tipo asignado aún
                tipo_bus_eval = viaje.get("tipo_bus") or bus.get("tipo_bus") or tipo_grupo_estimado
                if tipo_bus_eval:
                    viaje_temp = {**viaje, "tipo_bus": tipo_bus_eval}
                else:
                    viaje_temp = viaje
                
                conexion_valida, costo, necesita_recarga = _evaluar_conexion_con_recarga(
                    bus,
                    viaje_temp,
                    hora_disp,
                    paradas_dict,
                    nombres_depositos_upper,
                    _buscar_vacio,
                    _ruta_via_deposito,
                    gestor,
                    depositos_config,
                )

                if conexion_valida:
                    detalle_conexion = None
                    if bus.get("ultimo_viaje"):
                        detalle_conexion = gestor.evaluar_conexion_bus(
                            bus["ultimo_viaje"],
                            viaje_temp,
                            devolver_detalle=True,
                        )
                        if isinstance(detalle_conexion, tuple):
                            detalle_conexion = detalle_conexion[1] if len(detalle_conexion) > 1 else {}
                    espera = detalle_conexion.get("espera", 0) if detalle_conexion else 0
                    parada_min = detalle_conexion.get("parada_min", 0) if detalle_conexion else 0
                    parada_max = detalle_conexion.get("parada_max", 9999) if detalle_conexion else 9999
                    parada_optima = (parada_min + parada_max) / 2.0 if (parada_min or parada_max) else 0
                    desviacion_optimo = abs(espera - parada_optima) if (parada_optima and espera > 0) else 0

                    es_mejor = False
                    if mejor_idx is None:
                        es_mejor = True
                    else:
                        mejor_detalle = None
                        if buses_grupo[mejor_idx].get("ultimo_viaje"):
                            mejor_detalle = gestor.evaluar_conexion_bus(
                                buses_grupo[mejor_idx]["ultimo_viaje"],
                                viaje_temp,
                                devolver_detalle=True,
                            )
                            if isinstance(mejor_detalle, tuple):
                                mejor_detalle = mejor_detalle[1] if len(mejor_detalle) > 1 else {}
                        mejor_espera = mejor_detalle.get("espera", 0) if mejor_detalle else 0
                        mejor_parada_min = mejor_detalle.get("parada_min", 0) if mejor_detalle else 0
                        mejor_parada_max = mejor_detalle.get("parada_max", 9999) if mejor_detalle else 9999
                        mejor_optima = (mejor_parada_min + mejor_parada_max) / 2.0 if (mejor_parada_min or mejor_parada_max) else 0
                        mejor_desviacion = abs(mejor_espera - mejor_optima) if mejor_optima else 0
                        if desviacion_optimo < mejor_desviacion:
                            es_mejor = True
                        elif desviacion_optimo == mejor_desviacion and costo is not None and (mejor_costo is None or costo < mejor_costo):
                            es_mejor = True
                        elif desviacion_optimo == mejor_desviacion and (
                            (costo is None and mejor_costo is None) or
                            (costo is not None and mejor_costo is not None and abs((costo or 0) - (mejor_costo or 0)) <= 5.0)
                        ):
                            # Desempate: priorizar reutilizar buses disponibles más temprano.
                            # Esto evita dejar buses con muy pocos viajes cuando ya hay capacidad
                            # disponible para absorber nuevos servicios.
                            hora_disp_actual = bus.get("hora_disponible", 0)
                            ultimo_ev_actual = bus["viajes"][-1] if bus.get("viajes") else None
                            if ultimo_ev_actual and ultimo_ev_actual.get("fin") is not None:
                                hora_disp_actual = max(hora_disp_actual, ultimo_ev_actual["fin"])

                            hora_disp_mejor = buses_grupo[mejor_idx].get("hora_disponible", 0)
                            ultimo_ev_mejor = buses_grupo[mejor_idx]["viajes"][-1] if buses_grupo[mejor_idx].get("viajes") else None
                            if ultimo_ev_mejor and ultimo_ev_mejor.get("fin") is not None:
                                hora_disp_mejor = max(hora_disp_mejor, ultimo_ev_mejor["fin"])

                            # Preferir el bus con hora_disponible más antigua (menor valor)
                            if hora_disp_actual < hora_disp_mejor:
                                es_mejor = True
                            elif hora_disp_actual == hora_disp_mejor:
                                # Desempate final: más viajes = mayor concentración = menos buses totales
                                viajes_actual = len(bus.get("viajes", []))
                                viajes_mejor = len(buses_grupo[mejor_idx].get("viajes", []))
                                if viajes_actual > viajes_mejor:
                                    es_mejor = True

                    if es_mejor:
                        mejor_idx = idx
                        mejor_costo = costo
            
            # OPTIMIZACIÓN: Priorizar reactivar bus desde standby si es mejor opción
            usar_standby = False
            if mejor_bus_standby is not None:
                if mejor_idx is None:
                    usar_standby = True
                elif mejor_costo_standby is not None and (mejor_costo is None or mejor_costo_standby < mejor_costo):
                    usar_standby = True
            
            if usar_standby and mejor_bus_standby:
                # Validar tiempo_vacio antes de reactivar: must be not None, >0 y < 24*60
                deposito_standby = mejor_bus_standby.get("deposito_standby", gestor.deposito_base)
                tiempo_vacio_reactivar, km_vacio_reactivar = _buscar_vacio(
                    deposito_standby,
                    viaje["origen"],
                    mejor_bus_standby.get("hora_disponible", 0),
                )
                tiempo_ok = (
                    tiempo_vacio_reactivar is not None
                    and tiempo_vacio_reactivar > 0
                    and tiempo_vacio_reactivar < 24 * 60
                )
                if not tiempo_ok:
                    # No reactivar: tiempo inválido (buscar_tiempo_vacio devuelve None o fuera de rango)
                    if verbose:
                        print(
                            f"    [STANDBY] No reactivar: tiempo vacío inválido "
                            f"(tiempo={tiempo_vacio_reactivar}) para viaje {viaje.get('id')}"
                        )
                    usar_standby = False
                    mejor_bus_standby = None
                else:
                    # Reactivar bus desde standby
                    bus = mejor_bus_standby
                    buses_standby.remove(bus)
                    buses_grupo.append(bus)
                    mejor_idx = len(buses_grupo) - 1
                    mejor_costo = mejor_costo_standby
                    evento_vacio_reactivar = {
                        "evento": "vacio",
                        "origen": deposito_standby,
                        "destino": viaje["origen"],
                        "inicio": bus.get("hora_disponible", 0),
                        "fin": bus.get("hora_disponible", 0) + tiempo_vacio_reactivar,
                        "kilometros": km_vacio_reactivar or 0,
                        "tipo_bus": bus.get("tipo_bus") or tipo_grupo_estimado,
                        "desc": f"Vacio desde {deposito_standby} (reactivación desde standby)",
                    }
                    bus["viajes"].append(evento_vacio_reactivar)
                    bus["hora_disponible"] = evento_vacio_reactivar["fin"]
                
                if tiempo_ok and verbose:
                    print(f"    [STANDBY] Reactivando bus desde standby para viaje {viaje.get('id')}")
            
            # Si no se encontró bus, intentar forzar reutilización antes de crear uno nuevo
            tipo_bus_eval = viaje.get("tipo_bus") or tipo_grupo_estimado
            if mejor_idx is None and buses_grupo:
                viaje_para_forzar = {**viaje, "tipo_bus": tipo_bus_eval}
                mejor_idx_forzado = _buscar_bus_forzar_reutilizacion(
                    buses_grupo,
                    viaje_para_forzar,
                    paradas_dict,
                    nombres_depositos_upper,
                    _buscar_vacio,
                    _ruta_via_deposito,
                    gestor,
                    depositos_config,
                    max_buses or 9999,
                )
                if mejor_idx_forzado is not None:
                    mejor_idx = mejor_idx_forzado
                    if verbose:
                        print(f"    [REUTILIZACIÓN] Forzada para viaje {viaje.get('id')} en bus existente")

            if mejor_idx is not None:
                bus = buses_grupo[mejor_idx]
                # Verificar si necesita recarga: usar tipo del bus o grupo (robusto cuando viene de standby)
                tipo_bus_para_recarga = bus.get("tipo_bus") or tipo_bus_eval or tipo_grupo_estimado
                necesita_recarga_antes = False
                if tipo_bus_para_recarga:
                    necesita_recarga_antes = _necesita_recarga_antes_viaje(
                        bus,
                        {**viaje, "tipo_bus": tipo_bus_para_recarga},
                        bus.get("hora_disponible", 0),
                        gestor,
                        depositos_config,
                    )
                
                if necesita_recarga_antes:
                    # Agregar evento de recarga al bloque antes del viaje
                    evento_recarga = _planificar_recarga_antes_viaje(
                        bus,
                        {**viaje, "tipo_bus": tipo_bus_para_recarga},
                        bus.get("hora_disponible", 0),
                        gestor,
                        depositos_config,
                    )
                    if evento_recarga:
                        # Agregar vacío de ida, recarga, y vacío de vuelta
                        vacio_ida = evento_recarga.get("vacio_ida")
                        vacio_vuelta = evento_recarga.get("vacio_vuelta")
                        
                        if vacio_ida:
                            evento_vacio_ida = {
                                "evento": "vacio",
                                "origen": vacio_ida["origen"],
                                "destino": vacio_ida["destino"],
                                "inicio": vacio_ida["inicio"],
                                "fin": vacio_ida["fin"],
                                "kilometros": vacio_ida["kilometros"],
                                "tipo_bus": tipo_bus_para_recarga,
                                "desc": f"Vacio a {vacio_ida['destino']} (recarga)",
                            }
                            bus["viajes"].append(evento_vacio_ida)
                            bus["hora_disponible"] = vacio_ida["fin"]
                        
                        # Agregar evento de recarga
                        evento_recarga["tipo_bus"] = tipo_bus_para_recarga
                        bus["viajes"].append(evento_recarga)
                        bus["hora_disponible"] = evento_recarga.get("fin", bus.get("hora_disponible", 0))
                        bus["bateria_actual"] = evento_recarga.get("bateria_final", bus.get("bateria_actual", 100.0))
                        bus["tipo_bus"] = tipo_bus_para_recarga
                        
                        if vacio_vuelta:
                            evento_vacio_vuelta = {
                                "evento": "vacio",
                                "origen": vacio_vuelta["origen"],
                                "destino": vacio_vuelta["destino"],
                                "inicio": vacio_vuelta["inicio"],
                                "fin": vacio_vuelta["fin"],
                                "kilometros": vacio_vuelta["kilometros"],
                                "tipo_bus": tipo_bus_para_recarga,
                                "desc": f"Vacio desde {vacio_vuelta['origen']} (post-recarga)",
                            }
                            bus["viajes"].append(evento_vacio_vuelta)
                            bus["hora_disponible"] = vacio_vuelta["fin"]
                
                # ANTITELETRANSPORTACIÓN: Si ultimo.destino != viaje.origen, insertar vacíos antes del viaje
                ultimo_v = bus.get("ultimo_viaje")
                if ultimo_v and ultimo_v["destino"] != viaje["origen"] and not necesita_recarga_antes:
                    if not _insertar_vacios_conexion(
                        bus, ultimo_v, viaje,
                        bus.get("hora_disponible", 0),
                        _buscar_vacio, _ruta_via_deposito,
                        paradas_dict, nombres_depositos_upper,
                        tiempo_min_deposito,
                        tipo_bus_para_recarga,
                    ):
                        if verbose:
                            print(f"    [ANTITELETRANSP] Rechazado: no hay vacío válido {ultimo_v['destino']} -> {viaje['origen']}")
                        continue
                
                # Usar _agregar_viaje_a_bus para validar y registrar en viajes_asignados (evitar duplicados)
                if not _agregar_viaje_a_bus(bus, viaje):
                    continue
                regla_dest = paradas_dict.get(str(viaje["destino"]).upper())
                parada_min_dest = regla_dest.get("min", 0) if regla_dest else 0
                bus["hora_disponible"] = viaje["fin"] + parada_min_dest
                
                # Actualizar batería después del viaje (usar tipo estimado)
                if tipo_bus_para_recarga:
                    parametros_electricos = _obtener_parametros_electricos_bloque(tipo_bus_para_recarga, gestor)
                    if parametros_electricos:
                        consumo = _calcular_consumo_viaje_electrico(viaje, parametros_electricos)
                        bateria_actual = bus.get("bateria_actual", parametros_electricos.carga_inicial_pct)
                        bus["bateria_actual"] = max(0.0, bateria_actual - consumo)
                        bus["tipo_bus"] = tipo_bus_para_recarga  # Guardar tipo en el bus
                
                # OPTIMIZACIÓN: Verificar si el bus debe ir a standby después de este viaje
                siguiente_viaje = _buscar_siguiente_viaje_desde_hora(
                    viajes_ordenados,
                    bus["hora_disponible"],
                    tipo_bus_para_recarga,
                    indices=indice_viajes,
                )
                
                if siguiente_viaje:
                    regla_dest = _obtener_regla_parada(paradas_dict, viaje["destino"])
                    parada_max_dest = regla_dest.get("max", 1440) if regla_dest else 1440
                    fin_parada_max = viaje["fin"] + parada_max_dest
                    tiempo_hasta_siguiente = siguiente_viaje["inicio"] - fin_parada_max
                    if tiempo_hasta_siguiente > 0:
                        # Hay hueco después de la parada máxima: enviar bus a standby en depósito
                        nodo_actual = viaje["destino"]
                        tiempo_vacio_standby = None
                        deposito_standby = None
                        km_vacio_standby = 0
                        for dep in depositos_config:
                            nombre_dep = getattr(dep, "nombre", None) or (dep if isinstance(dep, dict) else {}).get("nombre")
                            if not nombre_dep:
                                continue
                            t_vacio, km_vacio = _buscar_vacio(nodo_actual, nombre_dep, fin_parada_max)
                            if t_vacio is not None:
                                deposito_standby = nombre_dep
                                tiempo_vacio_standby = t_vacio
                                km_vacio_standby = km_vacio or 0
                                break
                        if deposito_standby is None:
                            deposito_standby = bus.get("deposito_asignado") or gestor.deposito_base
                            tiempo_vacio_standby, km_vacio_standby = _buscar_vacio(nodo_actual, deposito_standby, fin_parada_max)
                            if tiempo_vacio_standby is not None:
                                km_vacio_standby = km_vacio_standby or 0
                        
                        if tiempo_vacio_standby is not None and deposito_standby:
                            # Crear evento de vacío al depósito para standby (después de parada máxima)
                            evento_standby = {
                                "evento": "vacio",
                                "origen": viaje["destino"],
                                "destino": deposito_standby,
                                "inicio": fin_parada_max,
                                "fin": fin_parada_max + tiempo_vacio_standby,
                                "kilometros": km_vacio_standby,
                                "tipo_bus": tipo_bus_para_recarga,
                                "desc": f"Vacio a {deposito_standby} (standby)",
                            }
                            bus["viajes"].append(evento_standby)
                            bus["hora_disponible"] = fin_parada_max + tiempo_vacio_standby
                            bus["deposito_standby"] = deposito_standby
                            
                            # Mover bus a lista de standby
                            buses_grupo.remove(bus)
                            buses_standby.append(bus)
                            
                            if verbose:
                                print(f"    [STANDBY] Enviando bus a standby en {deposito_standby} "
                                      f"hasta {formatear_hora(siguiente_viaje['inicio'])}")
                
                reutilizaciones_grupo += 1
            else:
                # Verificar límite de buses antes de crear uno nuevo
                total_buses_actual = len(todos_los_bloques) + len(buses_grupo)
                if max_buses and total_buses_actual >= max_buses:
                    # REGLA DURA: No se puede superar el máximo de buses
                    # Intentar forzar reutilización de un bus existente
                    if verbose:
                        print(
                            f"    ADVERTENCIA: Límite de buses alcanzado ({max_buses}). "
                            f"Intentando forzar reutilización para viaje {viaje.get('id')}..."
                        )
                    # Buscar el bus más cercano que pueda tomar este viaje (aunque tenga que recargar)
                    mejor_idx_forzado = _buscar_bus_forzar_reutilizacion(
                        buses_grupo,
                        viaje,
                        paradas_dict,
                        nombres_depositos_upper,
                        _buscar_vacio,
                        _ruta_via_deposito,
                        gestor,
                        depositos_config,
                        max_buses,
                    )
                    if mejor_idx_forzado is not None:
                        bus = buses_grupo[mejor_idx_forzado]
                        # Similar a la lógica anterior pero forzando
                        tipo_bus_para_recarga = tipo_grupo_estimado
                        necesita_recarga_antes = False
                        if tipo_bus_para_recarga:
                            necesita_recarga_antes = _necesita_recarga_antes_viaje(
                                bus,
                                {**viaje, "tipo_bus": tipo_bus_para_recarga},
                                bus.get("hora_disponible", 0),
                                gestor,
                                depositos_config,
                            )
                        if necesita_recarga_antes:
                            evento_recarga = _planificar_recarga_antes_viaje(
                                bus,
                                {**viaje, "tipo_bus": tipo_bus_para_recarga},
                                bus.get("hora_disponible", 0),
                                gestor,
                                depositos_config,
                            )
                            if evento_recarga:
                                # Agregar vacío de ida, recarga, y vacío de vuelta
                                vacio_ida = evento_recarga.get("vacio_ida")
                                vacio_vuelta = evento_recarga.get("vacio_vuelta")
                                
                                if vacio_ida:
                                    evento_vacio_ida = {
                                        "evento": "vacio",
                                        "origen": vacio_ida["origen"],
                                        "destino": vacio_ida["destino"],
                                        "inicio": vacio_ida["inicio"],
                                        "fin": vacio_ida["fin"],
                                        "kilometros": vacio_ida["kilometros"],
                                        "tipo_bus": tipo_bus_para_recarga,
                                        "desc": f"Vacio a {vacio_ida['destino']} (recarga)",
                                    }
                                    bus["viajes"].append(evento_vacio_ida)
                                    bus["hora_disponible"] = vacio_ida["fin"]
                                
                                evento_recarga["tipo_bus"] = tipo_bus_para_recarga
                                bus["viajes"].append(evento_recarga)
                                bus["hora_disponible"] = evento_recarga.get("fin", bus.get("hora_disponible", 0))
                                bus["bateria_actual"] = evento_recarga.get("bateria_final", bus.get("bateria_actual", 100.0))
                                bus["tipo_bus"] = tipo_bus_para_recarga
                                
                                if vacio_vuelta:
                                    evento_vacio_vuelta = {
                                        "evento": "vacio",
                                        "origen": vacio_vuelta["origen"],
                                        "destino": vacio_vuelta["destino"],
                                        "inicio": vacio_vuelta["inicio"],
                                        "fin": vacio_vuelta["fin"],
                                        "kilometros": vacio_vuelta["kilometros"],
                                        "tipo_bus": tipo_bus_para_recarga,
                                        "desc": f"Vacio desde {vacio_vuelta['origen']} (post-recarga)",
                                    }
                                    bus["viajes"].append(evento_vacio_vuelta)
                                    bus["hora_disponible"] = vacio_vuelta["fin"]
                        if not _agregar_viaje_a_bus(bus, viaje):
                            continue
                        regla_dest = paradas_dict.get(str(viaje["destino"]).upper())
                        parada_min_dest = regla_dest.get("min", 0) if regla_dest else 0
                        bus["hora_disponible"] = viaje["fin"] + parada_min_dest
                        if tipo_bus_para_recarga:
                            parametros_electricos = _obtener_parametros_electricos_bloque(tipo_bus_para_recarga, gestor)
                            if parametros_electricos:
                                consumo = _calcular_consumo_viaje_electrico(viaje, parametros_electricos)
                                bateria_actual = bus.get("bateria_actual", parametros_electricos.carga_inicial_pct)
                                bus["bateria_actual"] = max(0.0, bateria_actual - consumo)
                                bus["tipo_bus"] = tipo_bus_para_recarga
                        reutilizaciones_grupo += 1
                        continue
                    else:
                        _registrar_rechazo_factibilidad(
                            viaje=viaje,
                            grupo=grupo_nombre,
                            total_buses_actual=total_buses_actual,
                            motivo="sin_bus_reutilizable_y_limite_max_buses",
                        )
                        print(
                            f"    ERROR CRÍTICO: No se puede asignar viaje {viaje.get('id')} sin superar "
                            f"el límite de {max_buses} buses. El sistema debe encontrar una solución."
                        )
                        # Forzar creación de un bus adicional (viola el límite pero es necesario)
                        if verbose:
                            print(f"    FORZANDO creación de bus adicional (límite excedido)")
                
                regla_dest = paradas_dict.get(str(viaje["destino"]).upper())
                parada_min_dest = regla_dest.get("min", 0) if regla_dest else 0
                
                # IMPORTANTE: Buscar el mejor depósito para iniciar este nuevo bus
                # Considerar todos los depósitos configurados, no solo el depósito base
                nombres_depositos_disponibles = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else [gestor.deposito_base]
                mejor_deposito_inicio = gestor.deposito_base
                mejor_tiempo_vacio_inicio = None
                todos_tiempos_disponibles = {}  # Para logging
                
                for dep in nombres_depositos_disponibles:
                    # Usar gestor.buscar_tiempo_vacio directamente (no la función local _buscar_vacio)
                    t_vacio, km_vacio = gestor.buscar_tiempo_vacio(dep, viaje["origen"], viaje["inicio"])
                    if t_vacio is not None:
                        todos_tiempos_disponibles[dep] = t_vacio
                        if mejor_tiempo_vacio_inicio is None or t_vacio < mejor_tiempo_vacio_inicio:
                            mejor_tiempo_vacio_inicio = t_vacio
                            mejor_deposito_inicio = dep
                
                # Logging detallado cuando hay múltiples depósitos
                if len(nombres_depositos_disponibles) > 1:
                    if verbose:
                        print(f"    [NUEVO BUS] Viaje {viaje.get('id', 'N/A')} desde {viaje['origen']}:")
                        print(f"      Depósitos considerados: {sorted(nombres_depositos_disponibles)}")
                        if todos_tiempos_disponibles:
                            tiempos_str = ", ".join([f"{d}: {t}min" for d, t in sorted(todos_tiempos_disponibles.items(), key=lambda x: x[1])])
                            print(f"      Tiempos de vacío encontrados: {tiempos_str}")
                        print(f"      Depósito seleccionado: {mejor_deposito_inicio} (tiempo vacío: {mejor_tiempo_vacio_inicio} min)")
                    elif mejor_tiempo_vacio_inicio is not None and mejor_deposito_inicio != gestor.deposito_base:
                        # Logging mínimo cuando se selecciona un depósito diferente al base
                        print(f"    [NUEVO BUS] Viaje {viaje.get('id', 'N/A')}: Depósito seleccionado: {mejor_deposito_inicio} (más cercano que {gestor.deposito_base})")
                
                linea_bus = viaje.get("linea") or grupo_nombre or "SIN_LINEA"
                nuevo_bus = {
                    "linea": linea_bus,
                    "viajes": [],
                    "ultimo_viaje": None,
                    "hora_disponible": viaje["fin"] + parada_min_dest,
                    "deposito_asignado": mejor_deposito_inicio,
                }
                # Inicializar batería si es eléctrico (usar tipo estimado del grupo)
                if tipo_grupo_estimado:
                    parametros_electricos = _obtener_parametros_electricos_bloque(tipo_grupo_estimado, gestor)
                    if parametros_electricos:
                        nuevo_bus["bateria_actual"] = parametros_electricos.carga_inicial_pct
                        nuevo_bus["tipo_bus"] = tipo_grupo_estimado
                # ANTITELETRANSPORTACIÓN: Bus nuevo viene de depósito - insertar vacío dep->origen antes del viaje
                if mejor_tiempo_vacio_inicio is not None and mejor_deposito_inicio:
                    inicio_vacio = viaje["inicio"] - mejor_tiempo_vacio_inicio
                    evento_vacio_ini = {
                        "evento": "vacio",
                        "origen": mejor_deposito_inicio,
                        "destino": viaje["origen"],
                        "inicio": inicio_vacio,
                        "fin": viaje["inicio"],
                        "kilometros": gestor.buscar_tiempo_vacio(mejor_deposito_inicio, viaje["origen"], viaje["inicio"])[1] or 0,
                        "tipo_bus": tipo_grupo_estimado,
                        "desc": f"Vacio desde {mejor_deposito_inicio} (inicio bloque)",
                    }
                    nuevo_bus["viajes"].append(evento_vacio_ini)
                    nuevo_bus["hora_disponible"] = viaje["inicio"]
                # Usar _agregar_viaje_a_bus para registrar en viajes_asignados (evitar duplicados)
                if _agregar_viaje_a_bus(nuevo_bus, viaje):
                    nuevo_bus["hora_disponible"] = viaje["fin"] + parada_min_dest
                    buses_grupo.append(nuevo_bus)

        total_reutilizaciones += reutilizaciones_grupo
        # OPTIMIZACIÓN: Incluir buses en standby en los bloques finales
        # Los buses en standby deben estar en depósito al final del día
        for bus_standby in buses_standby:
            buses_grupo.append(bus_standby)

        # OPTIMIZACIÓN: Intentar reducir buses uniendo bloques compatibles
        # respetando las reglas existentes de conexión.
        cambios = True
        while cambios:
            cambios = False
            buses_grupo.sort(key=lambda b: (_primer_comercial(b.get("viajes", [])) or {}).get("inicio", 0))
            for i in range(len(buses_grupo)):
                bus_i = buses_grupo[i]
                ultimo_i = _ultimo_comercial(bus_i.get("viajes", []))
                if not ultimo_i:
                    continue
                mejor_j = None
                for j in range(len(buses_grupo)):
                    if i == j:
                        continue
                    bus_j = buses_grupo[j]
                    primer_j = _primer_comercial(bus_j.get("viajes", []))
                    if not primer_j:
                        continue
                    if primer_j["inicio"] <= ultimo_i["fin"]:
                        continue
                    # Usar el mismo evaluador robusto de conexión de Fase 1 (incluye vía depósito)
                    # para compactar bloques y evitar quedar con buses de una sola vuelta.
                    bus_tmp = {
                        "ultimo_viaje": ultimo_i,
                        "viajes": bus_i.get("viajes", []),
                        "hora_disponible": ultimo_i.get("fin", 0),
                        "tipo_bus": bus_i.get("tipo_bus"),
                    }
                    es_conexion, _costo_tmp, _recarga_tmp = _evaluar_conexion_con_recarga(
                        bus_tmp,
                        primer_j,
                        ultimo_i.get("fin", 0),
                        paradas_dict,
                        nombres_depositos_upper,
                        _buscar_vacio,
                        _ruta_via_deposito,
                        gestor,
                        depositos_config,
                    )
                    if es_conexion:
                        # Validar orden temporal por comerciales (no por eventos auxiliares),
                        # porque vacíos/paradas se recalculan luego en la contigüidad del bloque.
                        # La conectividad espacial ya está validada por gestor.evaluar_conexion_bus (incluye vía depósito).
                        viajes_j = bus_j.get("viajes", [])
                        viajes_i = bus_i.get("viajes", [])
                        fin_i = ultimo_i.get("fin") if ultimo_i else None
                        inicio_j = primer_j.get("inicio") if primer_j else None
                        if fin_i is not None and inicio_j is not None and inicio_j < fin_i:
                            continue  # Solapamiento temporal: probar siguiente j
                        mejor_j = j
                        break
                if mejor_j is not None:
                    bus_j = buses_grupo[mejor_j]
                    viajes_j = bus_j.get("viajes", [])
                    bus_i["viajes"].extend(viajes_j)
                    bus_i["ultimo_viaje"] = _ultimo_comercial(bus_i.get("viajes", [])) or bus_i.get("ultimo_viaje")
                    # hora_disponible: derivar del último evento (evitar saltos/teletransportaciones)
                    ultimo_ev = bus_i["viajes"][-1] if bus_i.get("viajes") else None
                    fin_ultimo = (ultimo_ev.get("fin") if (ultimo_ev and isinstance(ultimo_ev, dict)) else None) or 0
                    bus_i["hora_disponible"] = max(
                        bus_i.get("hora_disponible", 0),
                        bus_j.get("hora_disponible", 0),
                        fin_ultimo,
                    )
                    del buses_grupo[mejor_j]
                    cambios = True
                    break
        
        for bus in buses_grupo:
            # Los bloques incluyen tanto viajes comerciales como eventos de recarga
            # Filtrar solo los viajes comerciales para el bloque (los eventos de recarga
            # se manejarán en la exportación o se pueden incluir si tienen campo "evento")
            bloque_final = bus["viajes"].copy()
            
            # IMPORTANTE: Si el bus tiene un depósito asignado, propagarlo al primer viaje del bloque
            # para que esté disponible en la exportación
            deposito_asignado = bus.get("deposito_asignado")
            if deposito_asignado and bloque_final:
                # Agregar el depósito asignado al primer viaje del bloque
                bloque_final[0]["deposito_asignado"] = deposito_asignado
            
            todos_los_bloques.append(bloque_final)

        print(f"  -> Buses asignados: {len(buses_grupo)} | Reutilizaciones: {reutilizaciones_grupo}")

    # REGLA DURA: 100% cobertura - TODOS los viajes comerciales DEBEN estar en bloques
    ids_en_bloques: Set[Any] = set()
    for bloque in todos_los_bloques:
        for item in bloque:
            vid = _vid(item)
            if vid is not None:
                ids_en_bloques.add(vid)
                ids_en_bloques.add(str(vid))
    faltantes_bloques = [
        v for v in viajes_comerciales
        if _vid(v) not in ids_en_bloques
        and str(_vid(v)) not in ids_en_bloques
    ]
    if faltantes_bloques:
        print(f"\n[REGLA DURA] Agregando {len(faltantes_bloques)} viajes faltantes a bloques (cobertura 100%)...")
        nombres_depositos_disponibles = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else [gestor.deposito_base]
        for viaje in faltantes_bloques:
            mejor_deposito_inicio = gestor.deposito_base
            mejor_tiempo_vacio_inicio = None
            for dep in nombres_depositos_disponibles:
                t_vacio, _ = gestor.buscar_tiempo_vacio(dep, viaje["origen"], viaje["inicio"])
                if t_vacio is not None:
                    if mejor_tiempo_vacio_inicio is None or t_vacio < mejor_tiempo_vacio_inicio:
                        mejor_tiempo_vacio_inicio = t_vacio
                        mejor_deposito_inicio = dep
            bloque_nuevo: List[Dict[str, Any]] = []
            if mejor_tiempo_vacio_inicio is not None and mejor_deposito_inicio:
                inicio_vacio = viaje["inicio"] - mejor_tiempo_vacio_inicio
                km_vacio = 0
                try:
                    _, km_vacio = gestor.buscar_tiempo_vacio(mejor_deposito_inicio, viaje["origen"], viaje["inicio"])
                except Exception:
                    pass
                bloque_nuevo.append({
                    "evento": "vacio",
                    "origen": mejor_deposito_inicio,
                    "destino": viaje["origen"],
                    "inicio": inicio_vacio,
                    "fin": viaje["inicio"],
                    "kilometros": km_vacio or 0,
                    "desc": f"Vacio desde {mejor_deposito_inicio} (cobertura 100%)",
                })
            bloque_nuevo.append(dict(viaje))
            todos_los_bloques.append(bloque_nuevo)
            viajes_asignados.add(_vid(viaje))
            if verbose:
                print(f"  Viaje {viaje.get('id', 'N/A')} agregado a bloque nuevo (origen: {viaje.get('origen')} -> {viaje.get('destino')})")

    print("\n" + "=" * 80)
    print("RESULTADO FASE 1 (Greedy Seguro)")
    print("=" * 80)
    print(f"Total viajes: {n}")
    print(f"Buses generados: {len(todos_los_bloques)}")
    print(f"Reutilizaciones: {total_reutilizaciones}")
    print("=" * 80 + "\n")

    _asignar_tipos_a_bloques(todos_los_bloques, gestor)
    _validar_flota_por_tipo_en_bloques(todos_los_bloques, gestor)
    todos_los_bloques = _insertar_standby_vacios_en_bloques(
        todos_los_bloques, gestor, config, verbose=verbose
    )

    # CONTIGUIDAD: inicio_siguiente - fin_anterior = 0 (paradas min/max, depósito si > max)
    todos_los_bloques = _asegurar_contiguidad_bloques(
        todos_los_bloques, gestor, config, verbose=verbose
    )

    # FASE 1.5: Verificar y ajustar bloques eléctricos para cumplir recargas
    # Esta fase itera hasta que todos los bloques eléctricos puedan cumplir
    # con las restricciones de recarga, dividiendo bloques si es necesario
    print("\nVerificando y ajustando bloques eléctricos para cumplir recargas...")
    todos_los_bloques = _ajustar_bloques_electricos_con_recarga(
        todos_los_bloques,
        gestor,
        verbose=verbose,
    )

    # Contiguidad final (recargas pueden haber añadido eventos)
    todos_los_bloques = _asegurar_contiguidad_bloques(
        todos_los_bloques, gestor, config, verbose=verbose
    )

    # Saneo final: eliminar bloques sin comerciales (solo vacíos/paradas/recargas).
    bloques_validos: List[List[Dict[str, Any]]] = []
    bloques_sin_comercial = 0
    for bloque in todos_los_bloques:
        tiene_comercial = any(
            (item.get("id") is not None or item.get("_tmp_id") is not None)
            and (
                item.get("evento") is None
                or str(item.get("evento", "")).strip().lower() == "comercial"
            )
            for item in (bloque or [])
        )
        if tiene_comercial:
            bloques_validos.append(bloque)
        else:
            bloques_sin_comercial += 1
    if bloques_sin_comercial > 0:
        print(f"[FASE 1] Eliminados {bloques_sin_comercial} bloques sin eventos comerciales.")
    todos_los_bloques = bloques_validos

    # Usar el total de max_buses de los depósitos; si no hay, usar config raíz como fallback.
    _deps_conf = getattr(gestor, "depositos_config", None) or []
    _max_deps = sum(getattr(d, "max_buses", 0) for d in _deps_conf if getattr(d, "max_buses", 0))
    max_buses_check = _max_deps if _max_deps > 0 else config.get("max_buses")
    # REGLA DURA: Sin solapamiento de viajes por bus (no negociable)
    validar_fase1_sin_solapamiento_bloques(todos_los_bloques)

    status_f1 = "OPTIMAL"
    if max_buses_check and len(todos_los_bloques) > max_buses_check:
        resumen_diag = ""
        if diagnostico_factibilidad:
            por_linea = collections.Counter(str(d.get("linea", "SIN_LINEA")) for d in diagnostico_factibilidad)
            por_franja = collections.Counter(str(d.get("franja", "")) for d in diagnostico_factibilidad)
            por_origen = collections.Counter(str(d.get("origen", "")) for d in diagnostico_factibilidad)

            top_lineas = ", ".join(f"{k}:{v}" for k, v in por_linea.most_common(5))
            top_franjas = ", ".join(f"{k}:{v}" for k, v in por_franja.most_common(5))
            top_origenes = ", ".join(f"{k}:{v}" for k, v in por_origen.most_common(5))

            ejemplos = []
            for d in diagnostico_factibilidad[:10]:
                cand = d.get("candidatos_deposito", []) or []
                cand_txt = ", ".join(
                    f"{dep}={'sin_conexion' if t is None else str(t) + 'min'}"
                    for dep, t, _km in cand
                )
                ejemplos.append(
                    f"  viaje={d.get('viaje_id')} linea={d.get('linea')} grupo={d.get('grupo')} "
                    f"{formatear_hora(d.get('inicio', 0))}-{formatear_hora(d.get('fin', 0))} "
                    f"{d.get('origen')}->{d.get('destino')} "
                    f"buses={d.get('total_buses_actual')}/{d.get('max_buses')} "
                    f"mejor_dep={d.get('mejor_deposito') or 'N/A'} "
                    f"t_vacio={('N/A' if d.get('mejor_t_vacio') is None else str(d.get('mejor_t_vacio')) + 'min')} "
                    f"candidatos=[{cand_txt}]"
                )

            resumen_diag = (
                "\nDiagnóstico de factibilidad (trazabilidad operativa):\n"
                f"- Rechazos registrados: {len(diagnostico_factibilidad)}\n"
                f"- Top líneas afectadas: {top_lineas or 'N/A'}\n"
                f"- Top franjas críticas: {top_franjas or 'N/A'}\n"
                f"- Top nodos origen críticos: {top_origenes or 'N/A'}\n"
                "- Ejemplos de rechazo (línea/hora/nodo/depósito):\n"
                + "\n".join(ejemplos)
            )
        detalle_diag = {
            "permitido_continuar": permitir_exceso_flota,
            "status": "FEASIBLE_MAX_BUSES_EXCEDIDO",
            "max_buses_configurado": int(max_buses_check),
            "buses_requeridos": int(len(todos_los_bloques)),
            "buses_exceso": int(len(todos_los_bloques) - max_buses_check),
            "rechazos_factibilidad": list(diagnostico_factibilidad),
        }
        config["_diagnostico_flota_fase1"] = detalle_diag
        if not permitir_exceso_flota:
            raise ValueError(
                f"[FASE 1 - REGLA DURA] Se requieren {len(todos_los_bloques)} buses y el límite configurado es {max_buses_check}. "
                "No se permite continuar en estado FEASIBLE_MAX_BUSES_EXCEDIDO."
                + resumen_diag
            )
        status_f1 = "FEASIBLE_MAX_BUSES_EXCEDIDO"
        print(
            f"[FASE 1] Modo permisivo activo: se continúa con exceso de flota "
            f"({len(todos_los_bloques)} requeridos vs {max_buses_check} configurados)."
        )
        if resumen_diag:
            print(resumen_diag)

    # Fase 1 finaliza construyendo eventos_bus completos (InS, Vacio, Comercial, Parada, Recarga, FnS)
    print("\nConstruyendo secuencia de eventos por bus (validación y tipado)...")
    eventos_bus = construir_eventos_bus(todos_los_bloques, gestor, verbose=verbose)
    return todos_los_bloques, eventos_bus, status_f1


def _resolver_diagramacion_buses_cp_sat(
    config: Dict[str, Any],
    viajes_comerciales: List[Dict[str, Any]],
    gestor: GestorDeLogistica,
    random_seed: Optional[int] = None,
    verbose: bool = False,
) -> Tuple[List[List[Dict[str, Any]]], List[List[Dict[str, Any]]], str]:
    """
    Fase 1 con OR-Tools CP-SAT: considera paradas, vacíos internos y reutilización por depósito.
    Produce bloques y eventos_bus completos. Returns: (bloques_bus, eventos_bus, status)
    """
    print("\n" + "=" * 80)
    print("--- FASE 1: Diagramación de Buses (CP-SAT MEJORADO v2) ---")
    print("--- (Incluye Paradas, Vacíos Internos y Reutilización por Depósito) ---")
    print("=" * 80)

    if not _HAVE_CP_SAT:
        print("Error: OR-Tools CP-SAT no disponible.")
        return [], [], "ERROR"

    max_buses = config.get("max_buses", 200)
    log_detallado = print if verbose else (lambda *args, **kwargs: None)

    depositos_config = config.get("depositos", [])
    if not depositos_config and config.get("deposito"):
        depositos_config = [{"nombre": config.get("deposito"), "max_buses": max_buses}]
        print("[COMPATIBILIDAD] Se convirtió 'deposito' singular en lista 'depositos'.")

    nombres_depositos = {dep.get("nombre") for dep in depositos_config if isinstance(dep, dict) and dep.get("nombre")}
    if not nombres_depositos:
        print("ERROR: No hay depósitos configurados. Imposible resolver Fase 1.")
        return [], [], "ERROR_CONFIGURACION"

    paradas = config.get("paradas", {})
    paradas_dict = {k.upper(): v for k, v in paradas.items()}

    tiempo_min_deposito = gestor.tiempo_min_deposito

    print(f"Límite máximo de buses (REGLA DURA): {max_buses}")
    print(f"Depósitos disponibles para reutilización: {sorted(nombres_depositos)}")

    n = len(viajes_comerciales)
    if n == 0:
        print("No hay viajes para diagramar.")
        return [], [], "SIN_VIAJES"

    # REGLA DURA: no se aceptan viajes comerciales duplicados de entrada.
    _clave_v = lambda v: (v.get("inicio"), v.get("fin"), v.get("linea"), v.get("sentido"), (v.get("origen") or "").strip(), (v.get("destino") or "").strip())
    _seen_idx: Dict[Tuple[Any, Any, Any, Any, Any, Any], int] = {}
    _duplicados_cp: List[Tuple[int, int, Tuple[Any, Any, Any, Any, Any, Any]]] = []
    for idx_v, v in enumerate(viajes_comerciales):
        k = _clave_v(v)
        if k in _seen_idx:
            _duplicados_cp.append((_seen_idx[k] + 1, idx_v + 1, k))
        else:
            _seen_idx[k] = idx_v
    if _duplicados_cp:
        ejemplos = "\n".join(
            f"  viaje[{actual}] duplicado de viaje[{previo}] -> {clave}"
            for previo, actual, clave in _duplicados_cp[:10]
        )
        raise ValueError(
            f"[FASE 1 - REGLA DURA] Se detectaron {len(_duplicados_cp)} viajes duplicados (CP-SAT). "
            "No se permite deduplicación automática.\n"
            + ejemplos
        )

    universo_tipos: Set[str] = set(getattr(gestor, "tipos_bus_config", {}).keys())
    if not universo_tipos:
        universo_tipos = {"A", "B", "C", "BE", "BPAL"}

    def _tipos_para_viaje(viaje: Dict[str, Any]) -> Set[str]:
        linea = viaje.get("linea")
        permitidos = set(gestor.tipos_permitidos_para_linea(linea))
        permitidos = {t for t in permitidos if t in universo_tipos}
        return permitidos if permitidos else set(universo_tipos)

    tipos_por_viaje: List[Set[str]] = [_tipos_para_viaje(viaje) for viaje in viajes_comerciales]

    compat: List[Tuple[int, int]] = []
    contador_validaciones_parada = 0
    contador_paradas_validas = 0
    contador_paradas_rechazadas_min = 0
    contador_paradas_rechazadas_max = 0
    contador_vacios_internos = 0
    contador_deposito = 0

    print("Construyendo arcos compatibles (esto puede tardar)...")
    print("=" * 80)
    print("VALIDACIÓN DE PARADAS EN FASE 1:")
    print("=" * 80)

    for i, viaje_i in enumerate(viajes_comerciales):
        for j, viaje_j in enumerate(viajes_comerciales):
            if i == j:
                continue
            if viaje_j["inicio"] <= viaje_i["fin"]:
                continue

            es_factible, detalle = gestor.evaluar_conexion_bus(
                viaje_i,
                viaje_j,
                indice_origen=i,
                indice_destino=j,
                devolver_detalle=True,
            )

            mismo_lugar = viaje_i["destino"] == viaje_j["origen"]
            if mismo_lugar:
                contador_validaciones_parada += 1

            # Respetar interlineado: solo conectar viajes que puedan compartir bus según grupos de líneas
            if not gestor.pueden_interlinear(viaje_i.get("linea"), viaje_j.get("linea")):
                continue
            if es_factible and (tipos_por_viaje[i] & tipos_por_viaje[j]):
                compat.append((i, j))
                tipo = detalle.get("tipo")
                if tipo == "parada":
                    contador_paradas_validas += 1
                    log_detallado(
                        f"[OK] PARADA VÁLIDA: Viaje {i}->{j} | Nodo: {viaje_i['destino']} | "
                        f"Espera: {detalle.get('espera')} min | "
                        f"Rango: [{detalle.get('parada_min', 0)}, {detalle.get('parada_max', 0)}]"
                    )
                elif tipo == "vacio":
                    contador_vacios_internos += 1
                    log_detallado(
                        f"[OK] VACIO INTERNO: Viaje {i}->{j} | "
                        f"{viaje_i['destino']} -> {viaje_j['origen']} | "
                        f"Tiempo: {detalle.get('tiempo_vacio')} min | "
                        f"{formatear_hora(viaje_i['fin'])} -> {formatear_hora(viaje_j['inicio'])}"
                    )
                elif tipo == "deposito":
                    contador_deposito += 1
                    nombre_deposito = detalle.get("deposito", "N/A")
                    espera_en_dep = (
                        detalle.get("tiempo_salida_deposito", 0) - detalle.get("tiempo_llegada_deposito", 0)
                    )
                    log_detallado(
                        f"[OK] REUTILIZACION (DEPOSITO {nombre_deposito}): Viaje {i}->{j} | "
                        f"{viaje_i['destino']} -> {nombre_deposito} -> {viaje_j['origen']} | "
                        f"Tiempo total: {detalle.get('tiempo_total')} min | "
                        f"KM vacío: {detalle.get('kilometros_totales')} | "
                        f"Espera total: {detalle.get('espera_total')} min | "
                        f"En depósito: {espera_en_dep} min"
                    )
                continue

            # Estadísticas de rechazos de parada
            if mismo_lugar:
                parada_min = paradas_dict.get(viaje_i["destino"].upper(), {}).get("min", 0)
                parada_max = paradas_dict.get(viaje_i["destino"].upper(), {}).get("max", 1440)
                espera = viaje_j["inicio"] - viaje_i["fin"]
                if espera < parada_min:
                    contador_paradas_rechazadas_min += 1
                    log_detallado(
                        f"[X] PARADA RECHAZADA (MIN): Viaje {i}->{j} | Nodo: {viaje_i['destino']} | "
                        f"Espera: {espera} min < MIN: {parada_min} min"
                    )
                elif espera > parada_max:
                    contador_paradas_rechazadas_max += 1
                    log_detallado(
                        f"[X] PARADA RECHAZADA (MAX): Viaje {i}->{j} | Nodo: {viaje_i['destino']} | "
                        f"Espera: {espera} min > MAX: {parada_max} min"
                    )
                else:
                    log_detallado(
                        f"[!] SIN REGLA PARADA: Viaje {i}->{j} | Nodo: {viaje_i['destino']} | "
                        "No existe configuración para este nodo."
                    )

    print("=" * 80)
    print("RESUMEN DE VALIDACIONES FASE 1:")
    print(f"  - Validaciones de parada realizadas: {contador_validaciones_parada}")
    print(f"  - Paradas VÁLIDAS: {contador_paradas_validas}")
    print(f"  - Paradas RECHAZADAS (espera < min): {contador_paradas_rechazadas_min}")
    print(f"  - Paradas RECHAZADAS (espera > max): {contador_paradas_rechazadas_max}")
    print(f"  - Vacíos internos encontrados: {contador_vacios_internos}")
    print(f"  - Reutilizaciones por depósito: {contador_deposito}")
    print(f"  - Total arcos compatibles: {len(compat)}")
    print(f"  - Total viajes comerciales: {n}")
    if n > 1:
        ratio = len(compat) / (n * (n - 1)) * 100
        print(f"  - Ratio de conexiones: {ratio:.2f}%")
    print("=" * 80)

    if len(compat) == 0:
        print("[CRIT] ERROR CRÍTICO: No se encontraron conexiones válidas entre viajes.")
        bloques_solitarios = [[v] for v in viajes_comerciales]
        _asignar_tipos_a_bloques(bloques_solitarios, gestor)
        validar_fase1_sin_solapamiento_bloques(bloques_solitarios)
        eventos_bus = construir_eventos_bus(bloques_solitarios, gestor, verbose=verbose)
        return bloques_solitarios, eventos_bus, "SIN_COMPATIBILIDADES"

    modelo = cp_model.CpModel()
    variables_arcos = {(i, j): modelo.NewBoolVar(f"a_{i}_{j}") for (i, j) in compat}

    for i in range(n):
        salientes = [variables_arcos[(ii, jj)] for (ii, jj) in compat if ii == i]
        if salientes:
            modelo.Add(sum(salientes) <= 1)

    for j in range(n):
        entrantes = [variables_arcos[(ii, jj)] for (ii, jj) in compat if jj == j]
        if entrantes:
            modelo.Add(sum(entrantes) <= 1)

    numero_matches = sum(variables_arcos.values()) if variables_arcos else 0
    matches_minimos = max(0, n - max_buses)

    if len(compat) < matches_minimos:
        print(
            f"[WARN] Se requieren {matches_minimos} emparejamientos pero solo hay {len(compat)} conexiones válidas. "
            "Se relajará la restricción para permitir que el solver explore."
        )
    else:
        matches_relajados = max(0, int(matches_minimos * 0.99))
        if matches_relajados < matches_minimos:
            print(f"Restricción relajada: al menos {matches_relajados} emparejamientos (95% de {matches_minimos}).")
        else:
            print(f"Restricción exacta: al menos {matches_minimos} emparejamientos.")
        modelo.Add(numero_matches >= matches_relajados)

    modelo.Maximize(numero_matches)

    class CallbackMejoras(cp_model.CpSolverSolutionCallback):
        def __init__(self, variables, compatibilidad, viajes):
            super().__init__()
            self.vars = variables
            self.compat = compatibilidad
            self.viajes = viajes
            self.mejor = -1
            self.soluciones = 0
            self.t_inicio = None

        def on_solution_callback(self):
            if self.t_inicio is None:
                import time

                self.t_inicio = time.time()
            matches = 0
            for (i, j) in self.compat:
                if self.Value(self.vars[(i, j)]) == 1:
                    matches += 1
            if matches > self.mejor:
                self.mejor = matches
                self.soluciones += 1
                import time

                transcurrido = time.time() - self.t_inicio
                print(
                    f"[Progreso] Solución #{self.soluciones} a los {transcurrido:.1f}s: "
                    f"{matches} emparejamientos -> {len(self.viajes) - matches} buses"
                )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 240.0
    try:
        solver.parameters.num_search_workers = 8
        solver.parameters.search_branching = cp_model.PORTFOLIO_SEARCH
        solver.parameters.stop_after_first_solution = False
        solver.parameters.absolute_gap_limit = 0.8
        solver.parameters.max_presolve_iterations = 1000
        solver.parameters.use_optional_variables = True
    except Exception:
        pass

    if random_seed is not None:
        try:
            solver.parameters.random_seed = int(abs(random_seed)) % 2_147_483_647
            print(f"[SEED] Ejecutando CP-SAT con random_seed={solver.parameters.random_seed}")
        except Exception:
            print(f"[ADVERTENCIA] No se pudo asignar random_seed={random_seed} al solver CP-SAT")

    print(
        f"Iniciando optimización de buses (límite: {max_buses}, tiempo máximo: "
        f"{solver.parameters.max_time_in_seconds:.0f}s)..."
    )
    print(f"Total arcos compatibles: {len(compat)} de {n * (n - 1)} posibles.")

    callback = CallbackMejoras(variables_arcos, compat, viajes_comerciales)
    estado = solver.Solve(modelo, callback)
    status_str = solver.StatusName(estado)

    if estado not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print(f"[CRIT] Fase 1 sin solución ({status_str}).")
        if len(compat) == 0:
            bloques_solitarios = [[v] for v in viajes_comerciales]
            _asignar_tipos_a_bloques(bloques_solitarios, gestor)
            bloques_solitarios = _insertar_standby_vacios_en_bloques(
                bloques_solitarios, gestor, config, verbose=verbose
            )
            validar_fase1_sin_solapamiento_bloques(bloques_solitarios)
            eventos_bus = construir_eventos_bus(bloques_solitarios, gestor, verbose=verbose)
            return bloques_solitarios, eventos_bus, status_str
        print("Se construirán bloques manuales mediante heurística.")
        bloques = _construir_bloques_por_heuristica(viajes_comerciales, compat)
        _asignar_tipos_a_bloques(bloques, gestor)
        bloques = _insertar_standby_vacios_en_bloques(bloques, gestor, config, verbose=verbose)
        validar_fase1_sin_solapamiento_bloques(bloques)
        eventos_bus = construir_eventos_bus(bloques, gestor, verbose=verbose)
        return bloques, eventos_bus, "FEASIBLE_MANUAL"

    next_of = {i: None for i in range(n)}
    prev_of = {j: None for j in range(n)}
    matches = 0

    for (i, j) in compat:
        if solver.Value(variables_arcos[(i, j)]) == 1:
            next_of[i] = j
            prev_of[j] = i
            matches += 1

    num_buses = n - matches
    porcentaje_reutilizacion = (matches / n * 100) if n > 0 else 0
    print("\n" + "=" * 80)
    print("--- RESULTADO OPTIMIZACIÓN FASE 1 ---")
    print("=" * 80)
    print(f"Total viajes: {n}")
    print(f"Emparejamientos encontrados: {matches}")
    print(f"Buses generados: {num_buses} (objetivo máximo: {max_buses})")
    print(f"Porcentaje de reutilización: {porcentaje_reutilizacion:.1f}%")
    if estado == cp_model.OPTIMAL:
        print("[OK] Solución ÓPTIMA encontrada.")
    else:
        print("[WARN] Solución FACTIBLE encontrada (no necesariamente óptima).")
    print("=" * 80 + "\n")

    if num_buses > max_buses and status_str == "OPTIMAL":
        permitir_exceso_flota = True
        config["_diagnostico_flota_fase1"] = {
            "permitido_continuar": permitir_exceso_flota,
            "status": "FEASIBLE_MAX_BUSES_EXCEDIDO",
            "max_buses_configurado": int(max_buses),
            "buses_requeridos": int(num_buses),
            "buses_exceso": int(num_buses - max_buses),
            "rechazos_factibilidad": [],
        }
        if not permitir_exceso_flota:
            raise ValueError(
                f"[FASE 1 - REGLA DURA] Se generaron {num_buses} buses pero el máximo configurado es {max_buses}. "
                "No se permite continuar en estado FEASIBLE_MAX_BUSES_EXCEDIDO."
            )
        status_str = "FEASIBLE_MAX_BUSES_EXCEDIDO"
        print(
            f"[FASE 1] Modo permisivo activo: se continúa con exceso de flota "
            f"({num_buses} requeridos vs {max_buses} configurados)."
        )

    visitado = [False] * n
    bloques: List[List[Dict[str, Any]]] = []
    for i in range(n):
        if prev_of[i] is None:
            camino = []
            actual = i
            while actual is not None and actual < n and not visitado[actual]:
                visitado[actual] = True
                camino.append(viajes_comerciales[actual])
                actual = next_of[actual]
            if camino:
                bloques.append(camino)
    for i in range(n):
        if not visitado[i]:
            bloques.append([viajes_comerciales[i]])

    # REGLA DURA: 100% cobertura - TODOS los viajes comerciales DEBEN estar en bloques
    ids_en_bloques: Set[Any] = set()
    for bloque in bloques:
        for item in bloque:
            vid = _vid(item)
            if vid is not None:
                ids_en_bloques.add(vid)
                ids_en_bloques.add(str(vid))
    faltantes_bloques = [
        v for v in viajes_comerciales
        if _vid(v) not in ids_en_bloques
        and str(_vid(v)) not in ids_en_bloques
    ]
    if faltantes_bloques:
        print(f"\n[REGLA DURA] Agregando {len(faltantes_bloques)} viajes faltantes a bloques (cobertura 100%)...")
        nombres_depositos_disponibles = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else [gestor.deposito_base]
        for viaje in faltantes_bloques:
            mejor_deposito_inicio = gestor.deposito_base
            mejor_tiempo_vacio_inicio = None
            for dep in nombres_depositos_disponibles:
                t_vacio, _ = gestor.buscar_tiempo_vacio(dep, viaje["origen"], viaje["inicio"])
                if t_vacio is not None:
                    if mejor_tiempo_vacio_inicio is None or t_vacio < mejor_tiempo_vacio_inicio:
                        mejor_tiempo_vacio_inicio = t_vacio
                        mejor_deposito_inicio = dep
            bloque_nuevo: List[Dict[str, Any]] = []
            if mejor_tiempo_vacio_inicio is not None and mejor_deposito_inicio:
                inicio_vacio = viaje["inicio"] - mejor_tiempo_vacio_inicio
                km_vacio = 0
                try:
                    _, km_vacio = gestor.buscar_tiempo_vacio(mejor_deposito_inicio, viaje["origen"], viaje["inicio"])
                except Exception:
                    pass
                bloque_nuevo.append({
                    "evento": "vacio",
                    "origen": mejor_deposito_inicio,
                    "destino": viaje["origen"],
                    "inicio": inicio_vacio,
                    "fin": viaje["inicio"],
                    "kilometros": km_vacio or 0,
                    "desc": f"Vacio desde {mejor_deposito_inicio} (cobertura 100%)",
                })
            bloque_nuevo.append(dict(viaje))
            bloques.append(bloque_nuevo)
            if verbose:
                print(f"  Viaje {viaje.get('id', 'N/A')} agregado a bloque nuevo (origen: {viaje.get('origen')} -> {viaje.get('destino')})")

    _asignar_tipos_a_bloques(bloques, gestor)
    bloques = _insertar_standby_vacios_en_bloques(bloques, gestor, config, verbose=verbose)
    print(f"Total de bloques de bus generados: {len(bloques)}")
    validar_fase1_sin_solapamiento_bloques(bloques)
    print("\nConstruyendo secuencia de eventos por bus (validación y tipado)...")
    eventos_bus = construir_eventos_bus(bloques, gestor, verbose=verbose)
    return bloques, eventos_bus, status_str


def _construir_bloques_por_heuristica(
    viajes: List[Dict[str, Any]],
    compat: List[Tuple[int, int]],
) -> List[List[Dict[str, Any]]]:
    """
    Replica el fallback del script original cuando CP-SAT no produce solución.
    """
    n = len(viajes)
    mejor_solucion: Optional[List[List[Dict[str, Any]]]] = None
    menor_num_buses = n

    for estrategia in range(2):
        next_of = {i: None for i in range(n)}
        prev_of = {j: None for j in range(n)}
        conexiones_priorizadas = []
        for (i, j) in compat:
            espera = viajes[j]["inicio"] - viajes[i]["fin"]
            if estrategia == 0:
                conexiones_priorizadas.append((espera, i, j))
            else:
                conexiones_priorizadas.append((-espera, i, j))
        conexiones_priorizadas.sort()

        conexiones_usadas = 0
        for _, i, j in conexiones_priorizadas:
            if next_of[i] is None and prev_of[j] is None:
                next_of[i] = j
                prev_of[j] = i
                conexiones_usadas += 1
                if conexiones_usadas >= n - 1:
                    break

        visitado = [False] * n
        bloques_temp: List[List[Dict[str, Any]]] = []
        for i in range(n):
            if prev_of[i] is None:
                camino = []
                actual = i
                while actual is not None and actual < n and not visitado[actual]:
                    visitado[actual] = True
                    camino.append(viajes[actual])
                    actual = next_of[actual]
                if camino:
                    bloques_temp.append(camino)
        for i in range(n):
            if not visitado[i]:
                bloques_temp.append([viajes[i]])

        if len(bloques_temp) < menor_num_buses:
            menor_num_buses = len(bloques_temp)
            mejor_solucion = bloques_temp

    return mejor_solucion if mejor_solucion else [[v] for v in viajes]


def _es_deposito_nodo(nodo: str, depositos_upper: Set[str]) -> bool:
    """True si el nodo es un depósito (puede esperar sin límite parada_max)."""
    if not nodo:
        return False
    return str(nodo).strip().upper() in depositos_upper or any(
        p in str(nodo).upper() for p in ("DEPOSITO", "DEPÓSITO")
    )


def _asegurar_contiguidad_bloques(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
    config: Dict[str, Any],
    verbose: bool = False,
) -> List[List[Dict[str, Any]]]:
    """
    REGLA CRÍTICA: inicio_siguiente - fin_anterior = 0 siempre.
    - NUNCA modificar inicio/fin de eventos comerciales.
    - Vacíos: se pueden ajustar pero respetando tiempo_vacío (fin - inicio = constante).
    - Paradas: respetar parada_min y parada_max. Si gap > parada_max en nodo:
      enviar bus al depósito (vacío -> Parada en depósito -> vacío).
    - Único lugar para esperar más que parada_max: depósito.
    """
    paradas_dict = getattr(gestor, "paradas_dict", {}) or {
        k.upper(): v for k, v in config.get("paradas", {}).items()
    }
    depositos_config = getattr(gestor, "depositos_config", []) or config.get("depositos", [])
    if not depositos_config and config.get("deposito"):
        depositos_config = [{"nombre": config.get("deposito")}]
    nombres_depositos = [
        getattr(d, "nombre", None) or (d if isinstance(d, dict) else {}).get("nombre")
        for d in (depositos_config or [])
        if getattr(d, "nombre", None) or (isinstance(d, dict) and d.get("nombre"))
    ]
    depositos_upper = {str(n).upper() for n in nombres_depositos if n}
    tiempo_min_deposito = getattr(gestor, "tiempo_min_deposito", 5)

    def buscar_vacio(o: str, d: str, ref: int) -> Tuple[Optional[int], int]:
        t, km = gestor.buscar_tiempo_vacio(o, d, ref)
        return (t, km or 0)

    def obtener_parada_limites(nodo: str) -> Tuple[int, int]:
        regla = _obtener_regla_parada(paradas_dict, nodo)
        if not regla:
            return 0, 9999
        return regla.get("min", 0), regla.get("max", 1440)

    bloques_ok: List[List[Dict[str, Any]]] = []
    for idx_bloque, bloque in enumerate(bloques):
        if len(bloque) < 2:
            bloques_ok.append(bloque)
            continue
        nuevo: List[Dict[str, Any]] = []
        for i in range(len(bloque)):
            ev = dict(bloque[i])
            if i == 0:
                nuevo.append(ev)
                continue
            ev_prev = nuevo[-1]
            fin_prev = ev_prev.get("fin")
            inicio_act = ev.get("inicio")
            if fin_prev is None or inicio_act is None:
                nuevo.append(ev)
                continue
            diff = inicio_act - fin_prev
            es_comercial = _es_viaje_comercial(ev)
            es_comercial_prev = _es_viaje_comercial(ev_prev)

            if diff < 0:
                # SOLAPAMIENTO: evento actual empieza antes de que termine el anterior
                # NUNCA modificar comercial. Solo ajustar vacío o parada.
                if es_comercial:
                    # Ajustar evento anterior (debe ser vacío o parada)
                    tipo_prev = ev_prev.get("evento", "")
                    if tipo_prev == "vacio":
                        t_vac = buscar_vacio(
                            ev_prev.get("origen", ""),
                            ev_prev.get("destino", ""),
                            int(fin_prev - 60) if fin_prev else 0,
                        )[0]
                        if t_vac is not None:
                            ev_prev["fin"] = inicio_act
                            ev_prev["inicio"] = inicio_act - t_vac
                            ev_prev["kilometros"] = ev_prev.get("kilometros", 0)
                    elif tipo_prev == "parada":
                        nodo_p = ev_prev.get("destino") or ev_prev.get("origen") or ""
                        pmin, pmax = obtener_parada_limites(nodo_p)
                        ini_p = ev_prev.get("inicio")
                        if ini_p is not None:
                            nueva_duracion = inicio_act - ini_p
                            if pmin <= nueva_duracion <= pmax:
                                ev_prev["fin"] = inicio_act
                else:
                    # Ev actual es vacío/parada/recarga: podemos ajustar (nunca comercial)
                    if ev.get("evento") == "vacio":
                        t_vac = buscar_vacio(
                            ev.get("origen", ""),
                            ev.get("destino", ""),
                            int(inicio_act - 60) if inicio_act else 0,
                        )[0]
                        if t_vac is not None:
                            ev["inicio"] = fin_prev
                            ev["fin"] = fin_prev + t_vac
                    elif ev.get("evento") == "parada":
                        nodo_p = ev.get("destino") or ev.get("origen") or ""
                        pmin, pmax = obtener_parada_limites(nodo_p)
                        if _es_deposito_nodo(nodo_p, depositos_upper):
                            pmax = 9999
                        dur_orig = (ev.get("fin") or inicio_act) - inicio_act
                        dur = min(pmax, max(pmin, dur_orig))
                        ev["inicio"] = fin_prev
                        ev["fin"] = fin_prev + dur
                    elif ev.get("evento") == "recarga":
                        dur = (ev.get("fin") or inicio_act) - inicio_act
                        ev["inicio"] = fin_prev
                        ev["fin"] = fin_prev + max(1, dur)
                nuevo.append(ev)
            elif diff > 0:
                # HUECO: insertar evento(s) respetando parada_min y parada_max
                nodo_actual = ev_prev.get("destino") or ev_prev.get("A") or ""
                origen_sig = ev.get("origen") or ev.get("De") or ""
                parada_min, parada_max = obtener_parada_limites(nodo_actual)
                es_dep = _es_deposito_nodo(nodo_actual, depositos_upper)
                if es_dep:
                    parada_max = 9999

                if parada_min <= diff <= parada_max:
                    # Parada válida en el nodo
                    evento_parada = {
                        "evento": "parada",
                        "origen": nodo_actual,
                        "destino": nodo_actual,
                        "inicio": fin_prev,
                        "fin": inicio_act,
                        "tipo_bus": ev_prev.get("tipo_bus") or ev.get("tipo_bus"),
                        "desc": f"Parada/espera {diff}min",
                    }
                    nuevo.append(evento_parada)
                elif diff > parada_max and not es_dep:
                    # Enviar al depósito: vacío nodo->dep, Parada en dep, vacío dep->origen_sig
                    fin_parada_max = fin_prev + parada_max
                    deposito_elegido = None
                    t_ida, km_ida = None, 0
                    for dep in depositos_config:
                        nombre = getattr(dep, "nombre", None) or (dep if isinstance(dep, dict) else {}).get("nombre")
                        if not nombre:
                            continue
                        t, km = buscar_vacio(nodo_actual, nombre, fin_parada_max)
                        if t is not None:
                            deposito_elegido = nombre
                            t_ida, km_ida = t, km or 0
                            break
                    if deposito_elegido is None:
                        deposito_elegido = gestor.deposito_base
                        t_ida, km_ida = buscar_vacio(nodo_actual, deposito_elegido, fin_parada_max)
                    if t_ida is None:
                        if verbose:
                            print(f"    [CONTIGUIDAD] No hay vacío {nodo_actual}->depósito, gap={diff}min > parada_max={parada_max}. Insertando parada (excede máximo).")
                        nuevo.append({
                            "evento": "parada",
                            "origen": nodo_actual,
                            "destino": nodo_actual,
                            "inicio": fin_prev,
                            "fin": inicio_act,
                            "tipo_bus": ev_prev.get("tipo_bus") or ev.get("tipo_bus"),
                            "desc": f"Parada {diff}min (sin vacío a depósito)",
                        })
                    else:
                        llegada_dep = fin_prev + parada_max + t_ida
                        t_vuelta, km_vuelta = buscar_vacio(deposito_elegido, origen_sig, int(llegada_dep))
                        if t_vuelta is None:
                            if verbose:
                                print(f"    [CONTIGUIDAD] No hay vacío depósito->{origen_sig}. Insertando parada.")
                            nuevo.append({
                                "evento": "parada",
                                "origen": nodo_actual,
                                "destino": nodo_actual,
                                "inicio": fin_prev,
                                "fin": inicio_act,
                                "tipo_bus": ev_prev.get("tipo_bus") or ev.get("tipo_bus"),
                                "desc": f"Parada {diff}min",
                            })
                        else:
                            parada_min_dest, _ = obtener_parada_limites(origen_sig)
                            llegada_destino = inicio_act - parada_min_dest
                            salida_dep = llegada_destino - t_vuelta
                            if salida_dep < llegada_dep + tiempo_min_deposito:
                                if verbose:
                                    print(f"    [CONTIGUIDAD] Tiempo insuficiente en depósito. Insertando parada.")
                                nuevo.append({
                                    "evento": "parada",
                                    "origen": nodo_actual,
                                    "destino": nodo_actual,
                                    "inicio": fin_prev,
                                    "fin": inicio_act,
                                    "tipo_bus": ev_prev.get("tipo_bus") or ev.get("tipo_bus"),
                                    "desc": f"Parada {diff}min",
                                })
                            else:
                                # Insertar secuencia depósito: vacío->dep, parada dep, vacío->origen
                                tipo_bus = ev_prev.get("tipo_bus") or ev.get("tipo_bus")
                                nuevo.append({
                                    "evento": "vacio",
                                    "origen": nodo_actual,
                                    "destino": deposito_elegido,
                                    "inicio": fin_prev + parada_max,
                                    "fin": llegada_dep,
                                    "kilometros": km_ida,
                                    "tipo_bus": tipo_bus,
                                    "desc": f"Vacio a {deposito_elegido} (espera > parada_max {parada_max})",
                                })
                                nuevo.append({
                                    "evento": "parada",
                                    "origen": deposito_elegido,
                                    "destino": deposito_elegido,
                                    "inicio": llegada_dep,
                                    "fin": salida_dep,
                                    "tipo_bus": tipo_bus,
                                    "desc": f"Parada en depósito hasta próximo vacío",
                                })
                                nuevo.append({
                                    "evento": "vacio",
                                    "origen": deposito_elegido,
                                    "destino": origen_sig,
                                    "inicio": salida_dep,
                                    "fin": llegada_destino,
                                    "kilometros": km_vuelta or 0,
                                    "tipo_bus": tipo_bus,
                                    "desc": f"Vacio desde {deposito_elegido} a {origen_sig}",
                                })
                                if parada_min_dest > 0:
                                    nuevo.append({
                                        "evento": "parada",
                                        "origen": origen_sig,
                                        "destino": origen_sig,
                                        "inicio": llegada_destino,
                                        "fin": inicio_act,
                                        "tipo_bus": tipo_bus,
                                        "desc": f"Parada {parada_min_dest}min (antes del viaje)",
                                    })
                else:
                    # parada_min <= diff <= parada_max O diff < parada_min
                    if diff < parada_min and ev_prev.get("evento") == "vacio":
                        t_vac = buscar_vacio(
                            ev_prev.get("origen", ""),
                            ev_prev.get("destino", ""),
                            max(0, int(fin_prev - 120)),
                        )[0]
                        if t_vac is not None:
                            nuevo_fin = inicio_act - parada_min
                            fin_ant2 = nuevo[-2].get("fin") if len(nuevo) >= 2 else 0
                            if nuevo_fin - t_vac >= fin_ant2:
                                ev_prev["fin"] = nuevo_fin
                                ev_prev["inicio"] = nuevo_fin - t_vac
                                fin_prev = nuevo_fin
                                diff = parada_min
                    # Parada: fin debe ser inicio_act para contigüidad
                    if diff < parada_min:
                        if verbose:
                            print(f"    [CONTIGUIDAD] Gap {diff}min < parada_min {parada_min} en {nodo_actual}")
                    nuevo.append({
                        "evento": "parada",
                        "origen": nodo_actual,
                        "destino": nodo_actual,
                        "inicio": fin_prev,
                        "fin": inicio_act,
                        "tipo_bus": ev_prev.get("tipo_bus") or ev.get("tipo_bus"),
                        "desc": f"Parada {diff}min",
                    })
                nuevo.append(ev)
            else:
                nuevo.append(ev)
        bloques_ok.append(nuevo)
        ok, err = _validar_contiguidad_bloque(nuevo)
        if not ok:
            print(f"    [CONTIGUIDAD] ADVERTENCIA Bloque {idx_bloque+1}: {err}")
    return bloques_ok


def _validar_contiguidad_bloque(bloque: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
    """
    Valida que inicio_siguiente - fin_anterior = 0 para todos los pares consecutivos.
    Returns (ok, mensaje_error).
    """
    for i in range(len(bloque) - 1):
        ev1 = bloque[i]
        ev2 = bloque[i + 1]
        fin1 = ev1.get("fin")
        ini2 = ev2.get("inicio")
        if fin1 is not None and ini2 is not None:
            diff = ini2 - fin1
            if diff != 0:
                return False, f"Evento {i+2}: inicio={ini2} - fin_anterior={fin1} = {diff} (debe ser 0)"
    return True, None


def _validar_bloque_sin_teletransportacion(
    bloque: List[Dict[str, Any]],
    depositos_upper: Optional[Set[str]] = None,
    deposito_a_terminal: Optional[Dict[str, str]] = None,
) -> bool:
    """
    Verifica que un bloque no tenga teletransportaciones: cada evento debe conectarse
    al siguiente (destino_i == origen_{i+1}). Depósitos/terminales desde configuración.
    """
    if len(bloque) < 2:
        return True
    for i in range(len(bloque) - 1):
        ev1 = bloque[i]
        ev2 = bloque[i + 1]
        dest = (ev1.get("destino") or ev1.get("A") or "").strip()
        orig = (ev2.get("origen") or ev2.get("De") or "").strip()
        if dest and orig and not _mismo_lugar_nodo(dest, orig, depositos_upper, deposito_a_terminal):
            return False
    return True


def _terminal_upper_por_deposito(nombres_depositos: Optional[List[str]]) -> Dict[str, str]:
    """Construye mapa depósito (upper) -> terminal (upper) desde configuración. Sin nombres hardcodeados."""
    if not nombres_depositos:
        return {}
    out: Dict[str, str] = {}
    for n in nombres_depositos:
        d = (n or "").strip().upper()
        if not d:
            continue
        term = d.replace("DEPOSITO", "").replace("DEPÓSITO", "").strip()
        if term:
            out[d] = term
    return out


def _mismo_lugar_nodo(
    a: str,
    b: str,
    depositos_upper: Optional[Set[str]] = None,
    deposito_a_terminal: Optional[Dict[str, str]] = None,
) -> bool:
    """Compara si dos nodos son el mismo lugar (evitar teletransportaciones). Usa depósitos/terminales desde config."""
    if not a or not b:
        return False
    a, b = str(a).strip().upper(), str(b).strip().upper()
    if a == b:
        return True
    if depositos_upper and a in depositos_upper and b in depositos_upper:
        return True
    # Depósito <-> terminal (nombre sin prefijo "Deposito") según configuración
    if deposito_a_terminal:
        for dep_upper, term_upper in deposito_a_terminal.items():
            if term_upper and ((a == dep_upper and b == term_upper) or (a == term_upper and b == dep_upper)):
                return True
    # Coincidencia por contener prefijo genérico (solo si ambos tienen DEPOSITO)
    if "DEPOSITO" in a and "DEPOSITO" in b:
        return True
    return False


def _es_viaje_comercial(item: Dict[str, Any]) -> bool:
    """True si el item es un viaje comercial del input (no recarga, vacío ni parada)."""
    if not isinstance(item, dict):
        return False
    ev = item.get("evento")
    if ev in ("recarga", "vacio", "parada"):
        return False
    return "inicio" in item and "fin" in item and "origen" in item and "destino" in item


def _insertar_standby_vacios_en_bloques(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
    config: Dict[str, Any],
    verbose: bool = False,
) -> List[List[Dict[str, Any]]]:
    """
    REGLA OBLIGATORIA: Itera sobre TODOS los bloques e inserta vacío a depósito
    cuando entre dos viajes consecutivos hay un hueco > parada_max.
    Garantiza que NUNCA quede un bus con parada ilegal.
    """
    paradas_dict = getattr(gestor, "paradas_dict", {})
    paradas_dict = paradas_dict or {k.upper(): v for k, v in config.get("paradas", {}).items()}
    depositos_config = getattr(gestor, "depositos_config", [])
    if not depositos_config:
        depositos_config = config.get("depositos", [])
    if not depositos_config and config.get("deposito"):
        depositos_config = [{"nombre": config.get("deposito")}]
    tiempo_min_deposito = getattr(gestor, "tiempo_min_deposito", 5)

    def buscar_vacio(o: str, d: str, ref: int) -> Tuple[Optional[int], int]:
        t, km = gestor.buscar_tiempo_vacio(o, d, ref)
        return (t, km or 0)

    def obtener_parada_max(nodo: str) -> int:
        regla = _obtener_regla_parada(paradas_dict, nodo)
        return regla.get("max", 1440) if regla else 1440

    bloques_nuevos: List[List[Dict[str, Any]]] = []
    total_insertados = 0

    for bloque in bloques:
        nuevo_bloque: List[Dict[str, Any]] = []
        i = 0
        while i < len(bloque):
            item = bloque[i]
            nuevo_bloque.append(item)
            if not _es_viaje_comercial(item):
                i += 1
                continue
            viaje_actual = item
            if i + 1 >= len(bloque):
                i += 1
                continue
            siguiente_item = bloque[i + 1]
            if not _es_viaje_comercial(siguiente_item):
                i += 1
                continue
            viaje_siguiente = siguiente_item
            if viaje_actual["destino"] != viaje_siguiente["origen"]:
                i += 1
                continue
            nodo = viaje_actual["destino"]
            gap = viaje_siguiente["inicio"] - viaje_actual["fin"]
            parada_max = obtener_parada_max(nodo)
            if gap <= parada_max:
                i += 1
                continue
            fin_parada_max = viaje_actual["fin"] + parada_max
            deposito_elegido = None
            tiempo_ida = None
            km_ida = 0
            for dep in depositos_config:
                nombre = getattr(dep, "nombre", None) or (dep if isinstance(dep, dict) else {}).get("nombre")
                if not nombre:
                    continue
                t, km = buscar_vacio(nodo, nombre, fin_parada_max)
                if t is not None:
                    deposito_elegido = nombre
                    tiempo_ida = t
                    km_ida = km
                    break
            if deposito_elegido is None:
                deposito_elegido = gestor.deposito_base
                tiempo_ida, km_ida = buscar_vacio(nodo, deposito_elegido, fin_parada_max)
            if tiempo_ida is None:
                if verbose:
                    print(f"    [STANDBY] No se encontró vacío {nodo}->depósito, gap={gap}min > max={parada_max}min")
                i += 1
                continue
            llegada_dep = fin_parada_max + tiempo_ida
            tiempo_vuelta, km_vuelta = buscar_vacio(deposito_elegido, viaje_siguiente["origen"], llegada_dep)
            if tiempo_vuelta is None:
                if verbose:
                    print(f"    [STANDBY] No se encontró vacío depósito->{viaje_siguiente['origen']}")
                i += 1
                continue
            # Parada mínima en destino: el bus debe llegar parada_min antes del viaje
            nodo_destino = viaje_siguiente["origen"]
            regla_dest = _obtener_regla_parada(paradas_dict, nodo_destino)
            parada_min_dest = regla_dest.get("min", 0) if regla_dest else 0
            llegada_destino = viaje_siguiente["inicio"] - parada_min_dest
            salida_min_posible = llegada_dep + tiempo_min_deposito
            salida_dep = llegada_destino - tiempo_vuelta
            if salida_dep < salida_min_posible:
                # No hay tiempo suficiente: el vacío de vuelta empezaría antes de que el bus pueda salir
                if verbose:
                    print(
                        f"    [STANDBY] No insertar: tiempo insuficiente en depósito "
                        f"(llegada {llegada_dep} + min {tiempo_min_deposito} > salida {salida_dep})"
                    )
                i += 1
                continue
            tipo_bus = viaje_actual.get("tipo_bus") or viaje_siguiente.get("tipo_bus")
            # CONTIGUIDAD: insertar parada en nodo (fin_actual -> fin_parada_max) antes del vacío
            evento_parada_nodo = {
                "evento": "parada",
                "origen": nodo,
                "destino": nodo,
                "inicio": viaje_actual["fin"],
                "fin": fin_parada_max,
                "tipo_bus": tipo_bus,
                "desc": f"Parada {parada_max}min (antes standby)",
            }
            evento_vacio_ida = {
                "evento": "vacio",
                "origen": nodo,
                "destino": deposito_elegido,
                "inicio": fin_parada_max,
                "fin": fin_parada_max + tiempo_ida,
                "kilometros": km_ida,
                "tipo_bus": tipo_bus,
                "desc": f"Vacio a {deposito_elegido} (standby - hueco {gap}min > parada_max {parada_max}min)",
            }
            evento_vacio_vuelta = {
                "evento": "vacio",
                "origen": deposito_elegido,
                "destino": nodo_destino,
                "inicio": salida_dep,
                "fin": llegada_destino,
                "kilometros": km_vuelta or 0,
                "tipo_bus": tipo_bus,
                "desc": f"Vacio desde {deposito_elegido} (reactivación standby)",
            }
            nuevo_bloque.append(evento_parada_nodo)
            nuevo_bloque.append(evento_vacio_ida)
            nuevo_bloque.append(evento_vacio_vuelta)
            if parada_min_dest > 0:
                nuevo_bloque.append({
                    "evento": "parada",
                    "origen": nodo_destino,
                    "destino": nodo_destino,
                    "inicio": llegada_destino,
                    "fin": viaje_siguiente["inicio"],
                    "tipo_bus": tipo_bus,
                    "desc": f"Parada {parada_min_dest}min (antes del viaje)",
                })
            total_insertados += 1
            if verbose:
                print(
                    f"    [STANDBY] Insertado en bloque: {nodo} gap {gap}min > {parada_max}min -> "
                    f"vacío a {deposito_elegido} (standby)"
                )
            i += 1
        bloques_nuevos.append(nuevo_bloque)

    if total_insertados > 0:
        print(f"  [STANDBY] Insertados {total_insertados} pares vacío-standby en bloques (huecos > parada_max)")

    return bloques_nuevos


def _asignar_tipos_a_bloques(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
) -> None:
    """
    Determina el tipo de bus asignado a cada bloque respetando la disponibilidad
    total por tipo y las restricciones de cada línea.
    """
    if not bloques or not getattr(gestor, "tipos_bus_config", None):
        return

    tipos_disponibles = list(gestor.tipos_bus_config.keys())
    disponibilidad_cfg = dict(getattr(gestor, "flota_total_por_tipo", {}))
    aplica_tope_por_tipo = any(int(v or 0) > 0 for v in disponibilidad_cfg.values())
    if aplica_tope_por_tipo:
        disponibilidad = {tipo: int(disponibilidad_cfg.get(tipo, 0) or 0) for tipo in tipos_disponibles}
    else:
        # Sin flota explícita por tipo: comportamiento compatible (sin tope por tipo).
        disponibilidad = {tipo: len(bloques) for tipo in tipos_disponibles}

    candidatos_por_bloque: List[Tuple[int, Set[str]]] = []
    for idx, bloque in enumerate(bloques):
        if not bloque:
            continue
        candidatos: Set[str] = set(tipos_disponibles)
        for viaje in bloque:
            permitidos = set(gestor.tipos_permitidos_para_linea(viaje.get("linea")))
            permitidos = {t for t in permitidos if t in candidatos} if permitidos else candidatos
            if permitidos:
                candidatos &= permitidos
        if not candidatos:
            candidatos = set(tipos_disponibles)
        candidatos_por_bloque.append((idx, candidatos))

    for idx, candidatos in sorted(candidatos_por_bloque, key=lambda item: len(item[1])):
        try:
            tipo_asignado = _seleccionar_tipo_disponible(candidatos, disponibilidad)
        except ValueError as e:
            bloque = bloques[idx]
            linea_ref = next((v.get("linea") for v in bloque if v.get("linea")), "SIN_LINEA")
            ini = min((int(v.get("inicio", 0) or 0) for v in bloque), default=0)
            fin = max((int(v.get("fin", 0) or 0) for v in bloque), default=ini)
            raise ValueError(
                "[FASE 1 - REGLA DURA] Flota por tipo insuficiente para asignar bloque. "
                f"linea={linea_ref} ventana={formatear_hora(ini)}-{formatear_hora(fin)} "
                f"candidatos={sorted(candidatos)} disponibilidad={disponibilidad}"
            ) from e
        for viaje in bloques[idx]:
            viaje["tipo_bus"] = tipo_asignado


def _seleccionar_tipo_disponible(candidatos: Set[str], disponibilidad: Dict[str, int]) -> str:
    """
    Elige el mejor tipo disponible para un bloque priorizando la mayor disponibilidad restante.
    """
    if not candidatos:
        candidatos = set(disponibilidad.keys())
    if not candidatos:
        return ""
    candidatos_con_stock = [t for t in candidatos if disponibilidad.get(t, 0) > 0]
    if not candidatos_con_stock:
        raise ValueError(
            "No hay stock de flota para ninguno de los tipos candidatos. "
            f"candidatos={sorted(candidatos)} disponibilidad={disponibilidad}"
        )
    universo = candidatos_con_stock
    tipo = max(universo, key=lambda t: disponibilidad.get(t, 0))
    disponibilidad[tipo] = disponibilidad.get(tipo, 0) - 1
    return tipo


def _validar_flota_por_tipo_en_bloques(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
) -> None:
    """
    REGLA DURA: no superar flota máxima por tipo.
    """
    flota_cfg = dict(getattr(gestor, "flota_total_por_tipo", {}) or {})
    if not flota_cfg or not any(int(v or 0) > 0 for v in flota_cfg.values()):
        return

    usados: Dict[str, int] = collections.Counter()
    for bloque in bloques or []:
        tipo = next((str(v.get("tipo_bus", "")).strip().upper() for v in bloque if v.get("tipo_bus")), "")
        if tipo:
            usados[tipo] += 1

    excesos: List[Tuple[str, int, int]] = []
    for tipo, usados_tipo in sorted(usados.items()):
        max_tipo = int(flota_cfg.get(tipo, 0) or 0)
        if usados_tipo > max_tipo:
            excesos.append((tipo, usados_tipo, max_tipo))

    if excesos:
        detalle = ", ".join(f"{t}: usados={u} max={m}" for t, u, m in excesos)
        raise ValueError(
            "[FASE 1 - REGLA DURA] Se excede la flota por tipo. "
            + detalle
        )


def _insertar_vacios_conexion(
    bus: Dict[str, Any],
    ultimo: Dict[str, Any],
    viaje: Dict[str, Any],
    hora_disp: int,
    buscar_vacio_func,
    ruta_via_deposito_func,
    paradas_dict: Dict[str, Dict[str, Any]],
    nombres_depositos_upper: Set[str],
    tiempo_min_deposito: int,
    tipo_bus: Optional[str],
) -> bool:
    """
    Inserta los vacíos necesarios entre ultimo y viaje para evitar teletransportaciones.
    REGLA: destino del evento anterior debe coincidir con origen del siguiente.
    Returns True si se insertaron vacíos o no eran necesarios, False si no se pudo conectar.
    """
    if ultimo["destino"] == viaje["origen"]:
        return True  # Mismo lugar, no hay teletransportación

    # Intentar vacío directo
    t_vacio, km_vacio = buscar_vacio_func(ultimo["destino"], viaje["origen"], hora_disp)
    if t_vacio is not None and hora_disp + t_vacio <= viaje["inicio"]:
        regla_destino = paradas_dict.get(str(viaje["origen"]).upper())
        if regla_destino:
            parada_min = regla_destino.get("min", 0)
            parada_max = regla_destino.get("max", 1440)
            tiempo_restante = viaje["inicio"] - (hora_disp + t_vacio)
            if tiempo_restante < parada_min or tiempo_restante > parada_max:
                return False
        inicio_vacio = hora_disp
        fin_vacio = hora_disp + t_vacio
        evento_vacio = {
            "evento": "vacio",
            "origen": ultimo["destino"],
            "destino": viaje["origen"],
            "inicio": inicio_vacio,
            "fin": fin_vacio,
            "kilometros": km_vacio or 0,
            "tipo_bus": tipo_bus,
            "desc": f"Vacio {ultimo['destino']} -> {viaje['origen']} (evitar teletransportación)",
        }
        bus["viajes"].append(evento_vacio)
        bus["hora_disponible"] = fin_vacio
        return True

    # Intentar ruta vía depósito
    ruta = ruta_via_deposito_func(ultimo, viaje, hora_disp)
    if not ruta:
        return False
    dep = ruta.get("deposito")
    if not dep:
        return False
    t_ida, km_ida = buscar_vacio_func(ultimo["destino"], dep, hora_disp)
    if t_ida is None:
        return False
    llegada_dep = hora_disp + t_ida
    salida_min_dep = llegada_dep + tiempo_min_deposito
    aprox_salida = max(viaje["inicio"] - 30, salida_min_dep)  # aproximación
    t_vuelta, km_vuelta = buscar_vacio_func(dep, viaje["origen"], int(aprox_salida))
    if t_vuelta is None:
        return False
    salida_dep = viaje["inicio"] - t_vuelta
    if salida_dep < salida_min_dep:
        return False
    # Insertar vacío ida
    evento_ida = {
        "evento": "vacio",
        "origen": ultimo["destino"],
        "destino": dep,
        "inicio": hora_disp,
        "fin": llegada_dep,
        "kilometros": km_ida or 0,
        "tipo_bus": tipo_bus,
        "desc": f"Vacio a {dep} (conexión vía depósito)",
    }
    bus["viajes"].append(evento_ida)
    # Insertar vacío vuelta
    evento_vuelta = {
        "evento": "vacio",
        "origen": dep,
        "destino": viaje["origen"],
        "inicio": salida_dep,
        "fin": viaje["inicio"],
        "kilometros": km_vuelta or 0,
        "tipo_bus": tipo_bus,
        "desc": f"Vacio desde {dep} a {viaje['origen']}",
    }
    bus["viajes"].append(evento_vuelta)
    bus["hora_disponible"] = viaje["inicio"]
    return True


def _evaluar_conexion_con_recarga(
    bus: Dict[str, Any],
    viaje: Dict[str, Any],
    hora_disp: int,
    paradas_dict: Dict[str, Dict[str, Any]],
    nombres_depositos_upper: Set[str],
    buscar_vacio_func,
    ruta_via_deposito_func,
    gestor: GestorDeLogistica,
    depositos_config: List[Any],
) -> Tuple[bool, Optional[float], bool]:
    """
    Evalúa si un bus puede tomar un viaje considerando restricciones de recarga eléctrica.
    
    Returns:
        (conexion_valida, costo, necesita_recarga)
    """
    ultimo = bus["ultimo_viaje"]
    conexion_valida = False
    costo: Optional[float] = None
    necesita_recarga = False
    
    # REGLA CRÍTICA: Verificar interlineado ANTES de evaluar la conexión
    # Si el bus ya tiene viajes asignados, verificar que la línea del nuevo viaje
    # pueda interlinear con la línea del último viaje del bus
    if ultimo:
        linea_ultimo_viaje = ultimo.get("linea")
        linea_nuevo_viaje = viaje.get("linea")
        
        # Si las líneas son diferentes, verificar si pueden interlinear
        if linea_ultimo_viaje and linea_nuevo_viaje and linea_ultimo_viaje != linea_nuevo_viaje:
            pueden = gestor.pueden_interlinear(linea_ultimo_viaje, linea_nuevo_viaje)
            if not pueden:
                # No pueden interlinear - rechazar la conexión
                return False, None, False
    
    # Verificar conexión básica
    if ultimo["destino"] == viaje["origen"]:
        espera = viaje["inicio"] - ultimo["fin"]
        regla = paradas_dict.get(str(ultimo["destino"]).upper())
        es_deposito = str(ultimo["destino"]).upper() in nombres_depositos_upper
        
        # REGLA DURA: Las paradas SIEMPRE deben existir y ajustarse al rango min/max
        if regla:
            parada_min = regla.get("min", 0)
            parada_max = regla.get("max", 1440)

            if espera < parada_min:
                conexion_valida = False
            elif espera > parada_max:
                # Si la espera en terminal excede el máximo, intentar conexión vía depósito
                # (vacío a depósito + espera en depósito + vacío de retorno), que sí es válida.
                ruta = ruta_via_deposito_func(ultimo, viaje, hora_disp)
                if ruta:
                    conexion_valida = True
                    costo = ruta.get("km_totales", 0)
                else:
                    conexion_valida = False
            else:
                conexion_valida = True
                costo = 0.0
        elif es_deposito:
            # Depósitos no tienen reglas de parada (pueden esperar cualquier tiempo)
            conexion_valida = True
            costo = 0.0
        else:
            # Sin regla de parada, permitir la conexión
            conexion_valida = True
            costo = 0.0
    else:
        t_vacio, km_vacio = buscar_vacio_func(ultimo["destino"], viaje["origen"], hora_disp)
        if t_vacio is not None and hora_disp + t_vacio <= viaje["inicio"]:
            # Vacío directo sin retrasar salida: la espera debe cumplir el rango
            regla_destino = paradas_dict.get(str(viaje["origen"]).upper())
            if regla_destino:
                parada_min = regla_destino.get("min", 0)
                parada_max = regla_destino.get("max", 1440)
                tiempo_restante = viaje["inicio"] - (hora_disp + t_vacio)
                if tiempo_restante < parada_min or tiempo_restante > parada_max:
                    # Si el tiempo restante en terminal no cumple, intentar vía depósito.
                    ruta = ruta_via_deposito_func(ultimo, viaje, hora_disp)
                    if ruta:
                        conexion_valida = True
                        costo = ruta.get("km_totales", 0)
                    else:
                        conexion_valida = False
                else:
                    conexion_valida = True
                    costo = float(km_vacio or 0)
            else:
                conexion_valida = True
                costo = float(km_vacio or 0)
        else:
            ruta = ruta_via_deposito_func(ultimo, viaje, hora_disp)
            if ruta:
                conexion_valida = True
                costo = ruta.get("km_totales", 0)
    
    if not conexion_valida:
        return False, None, False
    
    # Verificar si necesita recarga (solo para buses eléctricos)
    tipo_bus = viaje.get("tipo_bus") or bus.get("tipo_bus")
    if tipo_bus:
        parametros_electricos = _obtener_parametros_electricos_bloque(tipo_bus, gestor)
        if parametros_electricos:
            bateria_actual = bus.get("bateria_actual", parametros_electricos.carga_inicial_pct)
            consumo_viaje = _calcular_consumo_viaje_electrico(viaje, parametros_electricos)
            bateria_despues = bateria_actual - consumo_viaje
            minimo_circular = parametros_electricos.minimo_para_circular_pct
            
            if bateria_actual < minimo_circular or bateria_despues < minimo_circular:
                necesita_recarga = True
                # Verificar si hay tiempo para recargar
                tiempo_disponible = viaje["inicio"] - hora_disp
                puede_recargar = _puede_recargar_en_tiempo(
                    ultimo["destino"],
                    viaje["origen"],
                    viaje["inicio"],
                    bateria_actual,
                    parametros_electricos,
                    gestor,
                    depositos_config,
                    tiempo_disponible,
                )
                if not puede_recargar:
                    # No puede recargar en el tiempo disponible
                    return False, None, True
    
    return conexion_valida, costo, necesita_recarga


def _obtener_parametros_electricos_bloque(
    tipo_bus: Optional[str],
    gestor: GestorDeLogistica,
) -> Optional[Any]:
    """Obtiene parámetros eléctricos para un tipo de bus."""
    if not tipo_bus or not hasattr(gestor, "obtener_tipo_bus"):
        return None
    config_tipo = gestor.obtener_tipo_bus(tipo_bus)
    if config_tipo and getattr(config_tipo, "es_electrico", False):
        return getattr(config_tipo, "parametros_electricos", None)
    return None


def _calcular_consumo_viaje_electrico(
    viaje: Dict[str, Any],
    parametros: Any,
) -> float:
    """Calcula el consumo de batería para un viaje."""
    kilometros = viaje.get("kilometros", 0) or 0
    if kilometros <= 0:
        return 0.0
    
    # Usar el método del parámetro si existe
    if hasattr(parametros, "obtener_consumo_linea"):
        linea = viaje.get("linea")
        consumo_linea = parametros.obtener_consumo_linea(linea) if linea else None
        if consumo_linea:
            return kilometros * consumo_linea
    
    # Fallback a consumo por km
    consumo_por_km = getattr(parametros, "consumo_pct_por_km", 0.5)
    return kilometros * consumo_por_km


def _necesita_recarga_antes_viaje(
    bus: Dict[str, Any],
    viaje: Dict[str, Any],
    hora_disp: int,
    gestor: GestorDeLogistica,
    depositos_config: List[Any],
) -> bool:
    """Verifica si un bus necesita recargar antes de tomar un viaje."""
    tipo_bus = viaje.get("tipo_bus") or bus.get("tipo_bus")
    if not tipo_bus:
        return False
    
    parametros_electricos = _obtener_parametros_electricos_bloque(tipo_bus, gestor)
    if not parametros_electricos:
        return False
    
    bateria_actual = bus.get("bateria_actual", parametros_electricos.carga_inicial_pct)
    consumo_viaje = _calcular_consumo_viaje_electrico(viaje, parametros_electricos)
    bateria_despues = bateria_actual - consumo_viaje
    minimo_circular = parametros_electricos.minimo_para_circular_pct
    
    return bateria_actual < minimo_circular or bateria_despues < minimo_circular


def _planificar_recarga_antes_viaje(
    bus: Dict[str, Any],
    viaje: Dict[str, Any],
    hora_disp: int,
    gestor: GestorDeLogistica,
    depositos_config: List[Any],
) -> Optional[Dict[str, Any]]:
    """Planifica una recarga antes de un viaje."""
    from diagramador_optimizado.core.builders.recarga import _buscar_oportunidad_recarga
    
    tipo_bus = viaje.get("tipo_bus") or bus.get("tipo_bus")
    if not tipo_bus:
        return None
    
    parametros_electricos = _obtener_parametros_electricos_bloque(tipo_bus, gestor)
    if not parametros_electricos:
        return None
    
    bateria_actual = bus.get("bateria_actual", parametros_electricos.carga_inicial_pct)
    ultimo = bus.get("ultimo_viaje")
    # Si no hay último viaje (bus nuevo), buscar el mejor depósito disponible
    if ultimo:
        origen_actual = ultimo["destino"]
    else:
        # Buscar el mejor depósito (más cercano) para iniciar
        nombres_depositos = gestor._nombres_depositos() if hasattr(gestor, "_nombres_depositos") else [gestor.deposito_base]
        mejor_deposito = gestor.deposito_base
        mejor_tiempo = None
        for dep in nombres_depositos:
            t_vacio, _ = gestor.buscar_tiempo_vacio(dep, viaje["origen"], viaje["inicio"])
            if t_vacio is not None:
                if mejor_tiempo is None or t_vacio < mejor_tiempo:
                    mejor_tiempo = t_vacio
                    mejor_deposito = dep
        origen_actual = mejor_deposito
    tiempo_disponible = viaje["inicio"] - hora_disp
    
    evento_recarga = _buscar_oportunidad_recarga(
        origen_actual,
        viaje["origen"],
        viaje["inicio"],
        bateria_actual,
        parametros_electricos,
        gestor,
        gestor.buscar_tiempo_vacio,
        tiempo_disponible,
        False,  # verbose
    )
    
    return evento_recarga


def _puede_recargar_en_tiempo(
    origen: str,
    destino: str,
    inicio_viaje: int,
    bateria_actual: float,
    parametros: Any,
    gestor: GestorDeLogistica,
    depositos_config: List[Any],
    tiempo_disponible: int,
) -> bool:
    """Verifica si se puede recargar en el tiempo disponible."""
    from diagramador_optimizado.core.builders.recarga import _buscar_oportunidad_recarga
    
    evento_recarga = _buscar_oportunidad_recarga(
        origen,
        destino,
        inicio_viaje,
        bateria_actual,
        parametros,
        gestor,
        gestor.buscar_tiempo_vacio,
        tiempo_disponible,
        False,
    )
    return evento_recarga is not None


def _buscar_bus_forzar_reutilizacion(
    buses_grupo: List[Dict[str, Any]],
    viaje: Dict[str, Any],
    paradas_dict: Dict[str, Dict[str, Any]],
    nombres_depositos_upper: Set[str],
    buscar_vacio_func,
    ruta_via_deposito_func,
    gestor: GestorDeLogistica,
    depositos_config: List[Any],
    max_buses: int,
) -> Optional[int]:
    """
    Busca un bus para forzar reutilización cuando se alcanza el límite de buses.
    Prioriza buses que puedan tomar el viaje aunque requieran recarga.
    """
    mejor_idx: Optional[int] = None
    mejor_costo: Optional[float] = None
    
    for idx, bus in enumerate(buses_grupo):
        ultimo = bus.get("ultimo_viaje")
        if not ultimo:
            continue
        ultimo_evento = bus.get("viajes", [])[-1] if bus.get("viajes") else None
        hora_disp = bus.get("hora_disponible", 0)
        if ultimo_evento and ultimo_evento.get("fin") is not None:
            hora_disp = max(hora_disp, ultimo_evento["fin"])
        if hora_disp > (viaje.get("inicio") or 0):
            continue

        # Evaluar conexión considerando recarga
        conexion_valida, costo, necesita_recarga = _evaluar_conexion_con_recarga(
            bus,
            viaje,
            hora_disp,
            paradas_dict,
            nombres_depositos_upper,
            buscar_vacio_func,
            ruta_via_deposito_func,
            gestor,
            depositos_config,
        )
        
        if conexion_valida:
            # Preferir el bus con menor costo
            if mejor_idx is None or (costo is not None and (mejor_costo is None or costo < mejor_costo)):
                mejor_idx = idx
                mejor_costo = costo
    
    return mejor_idx


def _ajustar_bloques_electricos_con_recarga(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
    verbose: bool = False,
) -> List[List[Dict[str, Any]]]:
    """
    Ajusta bloques eléctricos para cumplir con restricciones de recarga.
    
    Itera sobre los bloques y verifica si los buses eléctricos pueden cumplir
    con las recargas necesarias. Si un bloque no puede cumplir, lo divide
    creando nuevos buses hasta que todos los bloques sean factibles.
    
    Returns:
        Lista de bloques ajustados (puede tener más bloques que la entrada)
    """
    from diagramador_optimizado.core.builders.recarga import (
        _obtener_parametros_electricos as obtener_parametros,
        _verificar_factibilidad_bloque_electrico as verificar_factibilidad,
        _dividir_bloque_electrico as dividir_bloque,
    )
    
    bloques_ajustados: List[List[Dict[str, Any]]] = []
    max_iteraciones = 10  # Evitar bucles infinitos
    iteracion = 0
    
    while iteracion < max_iteraciones:
        iteracion += 1
        bloques_pendientes = bloques_ajustados if bloques_ajustados else bloques
        bloques_ajustados = []
        hubo_division = False
        
        for idx_bloque, bloque in enumerate(bloques_pendientes):
            if not bloque:
                bloques_ajustados.append(bloque)
                continue
            
            tipo_bus = bloque[0].get("tipo_bus")
            parametros_electricos = obtener_parametros(tipo_bus, gestor)
            
            if not parametros_electricos:
                # No es bus eléctrico, agregar sin modificar
                bloques_ajustados.append(bloque)
                continue
            
            # Verificar si el bloque eléctrico puede cumplir con recargas
            es_factible, punto_division = verificar_factibilidad(
                bloque,
                parametros_electricos,
                gestor,
                verbose,
            )
            
            if es_factible:
                # Bloque factible, agregarlo
                bloques_ajustados.append(bloque)
            else:
                # Bloque no factible, dividirlo
                if verbose:
                    print(
                        f"  Bloque {idx_bloque + 1} (tipo {tipo_bus}) no puede cumplir recargas. "
                        f"Dividiendo en punto {punto_division}..."
                    )
                
                bloques_divididos = dividir_bloque(
                    bloque,
                    punto_division,
                    tipo_bus,
                )
                bloques_ajustados.extend(bloques_divididos)
                hubo_division = True
        
        # Si no hubo divisiones en esta iteración, todos los bloques son factibles
        if not hubo_division:
            break
        
        # Si hubo divisiones, verificar nuevamente en la siguiente iteración
        bloques = bloques_ajustados.copy()
    
    if iteracion >= max_iteraciones:
        print(
            f"  ADVERTENCIA: Se alcanzó el máximo de iteraciones ({max_iteraciones}). "
            "Algunos bloques pueden no cumplir completamente con recargas."
        )
    
    print(f"  Bloques ajustados: {len(bloques)} -> {len(bloques_ajustados)} (iteraciones: {iteracion})")
    return bloques_ajustados



    if random_seed is not None:

        try:

            solver.parameters.random_seed = int(abs(random_seed)) % 2_147_483_647

            print(f"[SEED] Ejecutando CP-SAT con random_seed={solver.parameters.random_seed}")

        except Exception:

            print(f"[ADVERTENCIA] No se pudo asignar random_seed={random_seed} al solver CP-SAT")



    print(

        f"Iniciando optimización de buses (límite: {max_buses}, tiempo máximo: "

        f"{solver.parameters.max_time_in_seconds:.0f}s)..."

    )

    print(f"Total arcos compatibles: {len(compat)} de {n * (n - 1)} posibles.")



    callback = CallbackMejoras(variables_arcos, compat, viajes_comerciales)

    estado = solver.Solve(modelo, callback)

    status_str = solver.StatusName(estado)



    if estado not in (cp_model.OPTIMAL, cp_model.FEASIBLE):

        print(f"[CRIT] Fase 1 sin solución ({status_str}).")

        if len(compat) == 0:
            bloques_solitarios = [[v] for v in viajes_comerciales]
            _asignar_tipos_a_bloques(bloques_solitarios, gestor)
            eventos_bus = construir_eventos_bus(bloques_solitarios, gestor, verbose=verbose)
            return bloques_solitarios, eventos_bus, status_str
        print("Se construirán bloques manuales mediante heurística.")
        bloques = _construir_bloques_por_heuristica(viajes_comerciales, compat)
        _asignar_tipos_a_bloques(bloques, gestor)
        eventos_bus = construir_eventos_bus(bloques, gestor, verbose=verbose)
        return bloques, eventos_bus, "FEASIBLE_MANUAL"

    next_of = {i: None for i in range(n)}

    prev_of = {j: None for j in range(n)}

    matches = 0



    for (i, j) in compat:

        if solver.Value(variables_arcos[(i, j)]) == 1:

            next_of[i] = j

            prev_of[j] = i

            matches += 1



    num_buses = n - matches

    porcentaje_reutilizacion = (matches / n * 100) if n > 0 else 0

    print("\n" + "=" * 80)

    print("--- RESULTADO OPTIMIZACIÓN FASE 1 ---")

    print("=" * 80)

    print(f"Total viajes: {n}")

    print(f"Emparejamientos encontrados: {matches}")

    print(f"Buses generados: {num_buses} (objetivo máximo: {max_buses})")

    print(f"Porcentaje de reutilización: {porcentaje_reutilizacion:.1f}%")

    if estado == cp_model.OPTIMAL:

        print("[OK] Solución ÓPTIMA encontrada.")

    else:

        print("[WARN] Solución FACTIBLE encontrada (no necesariamente óptima).")

    print("=" * 80 + "\n")



    if num_buses > max_buses and status_str == "OPTIMAL":
        permitir_exceso_flota = True
        config["_diagnostico_flota_fase1"] = {
            "permitido_continuar": permitir_exceso_flota,
            "status": "FEASIBLE_MAX_BUSES_EXCEDIDO",
            "max_buses_configurado": int(max_buses),
            "buses_requeridos": int(num_buses),
            "buses_exceso": int(num_buses - max_buses),
            "rechazos_factibilidad": [],
        }
        if not permitir_exceso_flota:
            raise ValueError(
                f"[FASE 1 - REGLA DURA] Se generaron {num_buses} buses pero el máximo configurado es {max_buses}. "
                "No se permite continuar en estado FEASIBLE_MAX_BUSES_EXCEDIDO."
            )
        status_str = "FEASIBLE_MAX_BUSES_EXCEDIDO"
        print(
            f"[FASE 1] Modo permisivo activo: se continúa con exceso de flota "
            f"({num_buses} requeridos vs {max_buses} configurados)."
        )



    visitado = [False] * n

    bloques: List[List[Dict[str, Any]]] = []

    for i in range(n):

        if prev_of[i] is None:

            camino = []

            actual = i

            while actual is not None and actual < n and not visitado[actual]:

                visitado[actual] = True

                camino.append(viajes_comerciales[actual])

                actual = next_of[actual]

            if camino:

                bloques.append(camino)

    for i in range(n):

        if not visitado[i]:

            bloques.append([viajes_comerciales[i]])



    _asignar_tipos_a_bloques(bloques, gestor)
    print(f"Total de bloques de bus generados: {len(bloques)}")
    validar_fase1_sin_solapamiento_bloques(bloques)

    # Construir eventos_bus completos (InS, Vacio, Comercial, Parada, Recarga, FnS)
    print("\nConstruyendo secuencia de eventos por bus (validación y tipado)...")
    eventos_bus = construir_eventos_bus(bloques, gestor, verbose=verbose)
    return bloques, eventos_bus, status_str


def _construir_bloques_por_heuristica(

    viajes: List[Dict[str, Any]],

    compat: List[Tuple[int, int]],

) -> List[List[Dict[str, Any]]]:

    """

    Replica el fallback del script original cuando CP-SAT no produce solución.

    """

    n = len(viajes)

    mejor_solucion: Optional[List[List[Dict[str, Any]]]] = None

    menor_num_buses = n



    for estrategia in range(2):

        next_of = {i: None for i in range(n)}

        prev_of = {j: None for j in range(n)}

        conexiones_priorizadas = []

        for (i, j) in compat:

            espera = viajes[j]["inicio"] - viajes[i]["fin"]

            if estrategia == 0:

                conexiones_priorizadas.append((espera, i, j))

            else:

                conexiones_priorizadas.append((-espera, i, j))

        conexiones_priorizadas.sort()



        conexiones_usadas = 0

        for _, i, j in conexiones_priorizadas:

            if next_of[i] is None and prev_of[j] is None:

                next_of[i] = j

                prev_of[j] = i

                conexiones_usadas += 1

                if conexiones_usadas >= n - 1:

                    break



        visitado = [False] * n

        bloques_temp: List[List[Dict[str, Any]]] = []

        for i in range(n):

            if prev_of[i] is None:

                camino = []

                actual = i

                while actual is not None and actual < n and not visitado[actual]:

                    visitado[actual] = True

                    camino.append(viajes[actual])

                    actual = next_of[actual]

                if camino:

                    bloques_temp.append(camino)

        for i in range(n):

            if not visitado[i]:

                bloques_temp.append([viajes[i]])



        if len(bloques_temp) < menor_num_buses:

            menor_num_buses = len(bloques_temp)

            mejor_solucion = bloques_temp



    return mejor_solucion if mejor_solucion else [[v] for v in viajes]




def _asignar_tipos_a_bloques(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
) -> None:
    """
    Determina el tipo de bus asignado a cada bloque respetando la disponibilidad
    total por tipo y las restricciones de cada línea.
    """
    if not bloques or not getattr(gestor, "tipos_bus_config", None):
        return

    tipos_disponibles = list(gestor.tipos_bus_config.keys())
    disponibilidad = dict(getattr(gestor, "flota_total_por_tipo", {}))
    if not disponibilidad:
        disponibilidad = {tipo: len(bloques) for tipo in tipos_disponibles}

    candidatos_por_bloque: List[Tuple[int, Set[str]]] = []
    for idx, bloque in enumerate(bloques):
        if not bloque:
            continue
        candidatos: Set[str] = set(tipos_disponibles)
        for viaje in bloque:
            permitidos = set(gestor.tipos_permitidos_para_linea(viaje.get("linea")))
            permitidos = {t for t in permitidos if t in candidatos} if permitidos else candidatos
            if permitidos:
                candidatos &= permitidos
        if not candidatos:
            candidatos = set(tipos_disponibles)
        candidatos_por_bloque.append((idx, candidatos))

    for idx, candidatos in sorted(candidatos_por_bloque, key=lambda item: len(item[1])):
        tipo_asignado = _seleccionar_tipo_disponible(candidatos, disponibilidad)
        for viaje in bloques[idx]:
            viaje["tipo_bus"] = tipo_asignado


def _seleccionar_tipo_disponible(candidatos: Set[str], disponibilidad: Dict[str, int]) -> str:
    """
    Elige el mejor tipo disponible para un bloque priorizando la mayor disponibilidad restante.
    """
    if not candidatos:
        candidatos = set(disponibilidad.keys())
    if not candidatos:
        return ""
    candidatos_con_stock = [t for t in candidatos if disponibilidad.get(t, 0) > 0]
    universo = candidatos_con_stock if candidatos_con_stock else list(candidatos)
    tipo = max(universo, key=lambda t: disponibilidad.get(t, 0))
    disponibilidad[tipo] = disponibilidad.get(tipo, 0) - 1
    return tipo


