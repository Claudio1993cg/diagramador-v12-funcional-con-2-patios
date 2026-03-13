"""
Módulo para planificación de recargas de buses eléctricos en la Fase 1.
"""

from __future__ import annotations

import math
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.domain.tipos_vehiculo import ParametrosElectricos


def _construir_cache_vacio(gestor: GestorDeLogistica):
    """
    Pequeño caché para consultas de vacíos usadas durante planificación de recargas.
    """
    @lru_cache(maxsize=10000)
    def _cached(origen: str, destino: str, referencia: int):
        try:
            referencia_int = int(referencia)
        except Exception:
            referencia_int = 0
        return gestor.buscar_tiempo_vacio(origen, destino, referencia_int)

    return _cached


def planificar_recargas_bloques(
    bloques: List[List[Dict[str, Any]]],
    gestor: GestorDeLogistica,
    verbose: bool = False,
) -> List[List[Dict[str, Any]]]:
    """
    Planifica eventos de recarga para cada bloque de bus eléctrico.
    
    Si no puede planificar recargas necesarias, divide el bloque para garantizar
    que siempre haya una solución factible.
    
    Args:
        bloques: Lista de bloques de buses (cada bloque es una lista de viajes)
        gestor: Instancia de GestorDeLogistica
        verbose: Si es True, muestra información detallada
        
    Returns:
        Lista de bloques enriquecidos con eventos de recarga (puede tener más bloques
        que la entrada si se dividieron algunos)
    """
    bloques_enriquecidos: List[List[Dict[str, Any]]] = []
    deposito_base = gestor.deposito_base
    buscar_vacio = _construir_cache_vacio(gestor)
    
    for idx_bloque, bloque in enumerate(bloques):
        if not bloque:
            bloques_enriquecidos.append(bloque)
            continue
        
        # Obtener tipo de bus y parámetros eléctricos
        tipo_bus = bloque[0].get("tipo_bus")
        parametros_electricos = _obtener_parametros_electricos(tipo_bus, gestor)
        
        if not parametros_electricos:
            # No es bus eléctrico, agregar bloque sin modificar
            bloques_enriquecidos.append(bloque)
            continue
        
        # Planificar recargas para este bloque
        resultado = _planificar_recargas_bloque(
            bloque,
            parametros_electricos,
            gestor,
            deposito_base,
            buscar_vacio,
            verbose,
        )
        
        # resultado puede ser un bloque o una lista de bloques (si se dividió)
        if isinstance(resultado, list) and len(resultado) > 0 and isinstance(resultado[0], list):
            # Se dividió en múltiples bloques
            bloques_enriquecidos.extend(resultado)
            if verbose:
                print(f"  Bloque {idx_bloque + 1}: dividido en {len(resultado)} bloques para cumplir recargas")
        else:
            # Bloque único con recargas
            bloque_con_recargas = resultado
            bloques_enriquecidos.append(bloque_con_recargas)
            if verbose:
                num_recargas = sum(1 for item in bloque_con_recargas if item.get("evento") == "recarga")
                if num_recargas > 0:
                    print(f"  Bloque {idx_bloque + 1}: {num_recargas} recarga(s) planificada(s)")
    
    return bloques_enriquecidos


def _obtener_parametros_electricos(
    tipo_bus: Optional[str],
    gestor: GestorDeLogistica,
) -> Optional[ParametrosElectricos]:
    """Obtiene los parámetros eléctricos para un tipo de bus."""
    if not tipo_bus or not hasattr(gestor, "obtener_tipo_bus"):
        return None
    config_tipo = gestor.obtener_tipo_bus(tipo_bus)
    if config_tipo and config_tipo.es_electrico:
        return config_tipo.parametros_electricos
    return None


def _planificar_recargas_bloque(
    bloque: List[Dict[str, Any]],
    parametros: ParametrosElectricos,
    gestor: GestorDeLogistica,
    deposito_base: str,
    buscar_vacio,
    verbose: bool,
) -> List[Dict[str, Any]]:
    """
    Planifica recargas para un bloque específico.
    
    Si no puede planificar una recarga obligatoria, divide el bloque en el punto
    donde falla para garantizar una solución factible.
    
    Returns:
        Lista de eventos (viajes + recargas) o lista de bloques si se dividió
    """
    bloque_enriquecido: List[Dict[str, Any]] = []
    bateria_actual = parametros.carga_inicial_pct
    minimo_circular = parametros.minimo_para_circular_pct
    
    for idx, viaje in enumerate(bloque):
        # Calcular consumo del viaje
        consumo_viaje = _calcular_consumo_viaje(viaje, parametros)
        bateria_despues_viaje = bateria_actual - consumo_viaje
        
        # Verificar si se requiere recarga antes del viaje
        requiere_recarga = (
            bateria_actual < minimo_circular
            or bateria_despues_viaje < minimo_circular
        )
        
        if requiere_recarga:
            # Buscar oportunidad de recarga antes del viaje
            ultimo_evento = bloque_enriquecido[-1] if bloque_enriquecido else None
            tiempo_disponible = viaje["inicio"] - (ultimo_evento["fin"] if ultimo_evento else 0)
            origen_actual = ultimo_evento["destino"] if ultimo_evento else deposito_base
            
            evento_recarga = _buscar_oportunidad_recarga(
                origen_actual,
                viaje["origen"],
                viaje["inicio"],
                bateria_actual,
                parametros,
                gestor,
                buscar_vacio,
                tiempo_disponible,
                verbose,
                inicio_ventana=(ultimo_evento["fin"] if ultimo_evento else max(0, viaje["inicio"] - tiempo_disponible)),
            )
            
            if evento_recarga:
                bloque_enriquecido.append(evento_recarga)
                bateria_actual = evento_recarga.get("bateria_final", bateria_actual)
            else:
                # No se puede planificar recarga obligatoria: dividir el bloque
                if verbose:
                    print(
                        f"    ADVERTENCIA: No se puede planificar recarga obligatoria antes del viaje {viaje.get('id')}. "
                        f"Dividiendo bloque en este punto."
                    )
                
                # Dividir el bloque: el bloque actual termina antes de este viaje
                # y el siguiente bloque comienza con este viaje
                bloque_actual = bloque_enriquecido.copy()
                bloque_restante = bloque[idx:]
                
                # Si el bloque actual tiene viajes, retornarlo
                if bloque_actual:
                    # Asegurar que el bloque actual tenga al menos un viaje comercial
                    viajes_comerciales_actual = [v for v in bloque_actual if v.get("evento") != "recarga"]
                    if viajes_comerciales_actual:
                        # Retornar lista de bloques: [bloque_actual, bloque_restante]
                        return [bloque_actual, bloque_restante]
                
                # Si no hay bloque actual, continuar con el bloque restante
                # pero marcar que necesita recarga al inicio
                bloque_restante[0]["_necesita_recarga_inicial"] = True
                return bloque_restante
        
        # Agregar el viaje comercial
        bloque_enriquecido.append(viaje)
        
        # Actualizar batería después del viaje
        bateria_actual = bateria_despues_viaje
        
        # Verificar si se requiere recarga después del viaje (oportunidad)
        if idx < len(bloque) - 1:
            siguiente_viaje = bloque[idx + 1]
            tiempo_entre = siguiente_viaje["inicio"] - viaje["fin"]
            
            # Si hay tiempo suficiente y la batería está baja, considerar recarga
            if (tiempo_entre >= parametros.tiempo_minimo_recarga + 60 and
                bateria_actual < parametros.porcentaje_max_entrada_pct):
                evento_recarga = _buscar_oportunidad_recarga(
                    viaje["destino"],
                    siguiente_viaje["origen"],
                    siguiente_viaje["inicio"],
                    bateria_actual,
                    parametros,
                    gestor,
                    buscar_vacio,
                    tiempo_entre,
                    verbose,
                    inicio_ventana=viaje["fin"],
                )
                
                if evento_recarga:
                    bloque_enriquecido.append(evento_recarga)
                    bateria_actual = evento_recarga.get("bateria_final", bateria_actual)
    
    return bloque_enriquecido


def _calcular_consumo_viaje(
    viaje: Dict[str, Any],
    parametros: ParametrosElectricos,
) -> float:
    """Calcula el consumo de batería para un viaje."""
    kilometros = viaje.get("kilometros", 0) or 0
    if kilometros <= 0:
        return 0.0
    
    linea = viaje.get("linea")
    consumo_linea = parametros.obtener_consumo_linea(linea) if linea else None
    clave_arco = None
    if viaje.get("origen") and viaje.get("destino"):
        clave_arco = f"{viaje['origen']}_{viaje['destino']}"
    consumo_arco = parametros.obtener_consumo_arco(clave_arco) if clave_arco else None
    factor = consumo_linea or consumo_arco or parametros.consumo_pct_por_km
    return kilometros * factor


def _buscar_oportunidad_recarga(
    origen: str,
    destino_viaje: str,
    inicio_viaje: int,
    bateria_actual: float,
    parametros: ParametrosElectricos,
    gestor: GestorDeLogistica,
    buscar_vacio,
    tiempo_disponible: int,
    verbose: bool,
    inicio_ventana: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Busca una oportunidad de recarga entre origen y destino_viaje.
    
    Returns:
        Diccionario con evento de recarga o None si no se encuentra oportunidad
    """
    mejor_deposito = None
    mejor_tiempo_total = float("inf")
    mejor_info = None
    
    # Buscar en todos los depósitos configurados
    for deposito_obj in gestor.depositos_config:
        deposito_nombre = deposito_obj.nombre if hasattr(deposito_obj, "nombre") else str(deposito_obj)
        if not isinstance(deposito_nombre, str):
            deposito_nombre = str(deposito_nombre)
        
        if not gestor.permite_recarga_en_deposito(deposito_nombre):
            continue
        
        # Calcular tiempo de ida al depósito
        tiempo_ida, km_ida = buscar_vacio(origen, deposito_nombre, 0)
        if tiempo_ida is None:
            continue
        
        # Calcular tiempo de vuelta del depósito al destino
        tiempo_vuelta, km_vuelta = buscar_vacio(
            deposito_nombre,
            destino_viaje,
            inicio_viaje - 30,  # Aproximación
        )
        if tiempo_vuelta is None:
            continue
        
        tiempo_total = tiempo_ida + tiempo_vuelta
        tiempo_para_recarga = tiempo_disponible - tiempo_total
        
        # Ser más flexible: aceptar recargas parciales si es necesario
        # Si hay al menos 10 minutos para recargar, considerarlo
        if tiempo_para_recarga < 10:
            continue
        
        # Verificar ventana de recarga usando tiempo real de la ventana disponible.
        # Si no se entrega inicio_ventana, inferirla desde tiempo_disponible.
        if inicio_ventana is None:
            try:
                inicio_ventana_calc = int(inicio_viaje) - int(tiempo_disponible)
            except Exception:
                inicio_ventana_calc = 0
            inicio_ventana = max(0, inicio_ventana_calc)

        llegada_deposito_aprox = int(inicio_ventana) + int(tiempo_ida)
        fin_recarga_max = int(inicio_viaje) - int(tiempo_vuelta)
        if fin_recarga_max <= llegada_deposito_aprox:
            continue
        
        # Intentar calcular recarga disponible
        recarga_info = _calcular_recarga_disponible(
            parametros,
            bateria_actual,
            llegada_deposito_aprox,
            fin_recarga_max,
            100.0,  # Objetivo: recargar completamente
        )
        
        if recarga_info:
            inicio_recarga, fin_recarga, bateria_final = recarga_info
            if tiempo_total < mejor_tiempo_total:
                mejor_tiempo_total = tiempo_total
                mejor_deposito = deposito_nombre
                mejor_info = {
                    "tiempo_ida": tiempo_ida,
                    "tiempo_vuelta": tiempo_vuelta,
                    "km_ida": km_ida or 0,
                    "km_vuelta": km_vuelta or 0,
                    "inicio_recarga": inicio_recarga,
                    "fin_recarga": fin_recarga,
                    "bateria_final": bateria_final,
                    "tiempo_para_recarga": tiempo_para_recarga,
                }
        else:
            # Si no cabe en la ventana, intentar recarga parcial (al menos al mínimo)
            if tiempo_para_recarga >= parametros.tiempo_minimo_recarga:
                # Calcular recarga parcial
                tiempo_recarga_parcial = min(tiempo_para_recarga, parametros.tiempo_minimo_recarga * 2)
                delta_bateria = tiempo_recarga_parcial * parametros.tasa_recarga_pct_por_min
                bateria_final_parcial = min(100.0, bateria_actual + delta_bateria)
                
                # Verificar que al menos llegue al mínimo para circular
                minimo_circular = parametros.minimo_para_circular_pct
                if bateria_final_parcial >= minimo_circular:
                    if tiempo_total < mejor_tiempo_total:
                        mejor_tiempo_total = tiempo_total
                        mejor_deposito = deposito_nombre
                        mejor_info = {
                            "tiempo_ida": tiempo_ida,
                            "tiempo_vuelta": tiempo_vuelta,
                            "km_ida": km_ida or 0,
                            "km_vuelta": km_vuelta or 0,
                            "inicio_recarga": llegada_deposito_aprox,
                            "fin_recarga": llegada_deposito_aprox + tiempo_recarga_parcial,
                            "bateria_final": bateria_final_parcial,
                            "tiempo_para_recarga": tiempo_para_recarga,
                            "recarga_parcial": True,
                        }
    
    if mejor_deposito and mejor_info:
        # Crear evento de recarga
        llegada_deposito = mejor_info["inicio_recarga"] - mejor_info["tiempo_ida"]
        salida_deposito = mejor_info["fin_recarga"]
        llegada_destino = salida_deposito + mejor_info["tiempo_vuelta"]
        
        evento_recarga = {
            "evento": "recarga",
            "origen": origen,
            "destino": mejor_deposito,
            "inicio": llegada_deposito,
            "fin": llegada_deposito + mejor_info["tiempo_ida"],
            "kilometros": mejor_info["km_ida"],
            "tipo_bus": None,  # Se asignará después
            "desc": f"Vacio a {mejor_deposito} (recarga)",
        }
        
        # Agregar evento de recarga propiamente dicho
        evento_recarga_principal = {
            "evento": "recarga",
            "origen": mejor_deposito,
            "destino": mejor_deposito,
            "inicio": mejor_info["inicio_recarga"],
            "fin": mejor_info["fin_recarga"],
            "kilometros": 0,
            "tipo_bus": None,
            "bateria_inicial": bateria_actual,
            "bateria_final": mejor_info["bateria_final"],
            "desc": f"Recarga en {mejor_deposito}",
        }
        
        # Agregar evento de vacío de vuelta
        evento_vuelta = {
            "evento": "Vacio",
            "origen": mejor_deposito,
            "destino": destino_viaje,
            "inicio": salida_deposito,
            "fin": llegada_destino,
            "kilometros": mejor_info["km_vuelta"],
            "tipo_bus": None,
            "desc": f"Vacio desde {mejor_deposito} (post-recarga)",
        }
        
        # Retornar el evento de recarga principal con información completa
        # Los vacíos de ida y vuelta se pueden agregar en la Fase 1 si es necesario
        # Por ahora retornamos el evento principal con toda la información
        evento_recarga_principal["vacio_ida"] = {
            "origen": origen,
            "destino": mejor_deposito,
            "inicio": llegada_deposito,
            "fin": llegada_deposito + mejor_info["tiempo_ida"],
            "kilometros": mejor_info["km_ida"],
        }
        evento_recarga_principal["vacio_vuelta"] = {
            "origen": mejor_deposito,
            "destino": destino_viaje,
            "inicio": salida_deposito,
            "fin": llegada_destino,
            "kilometros": mejor_info["km_vuelta"],
        }
        return evento_recarga_principal
    
    return None


def _calcular_recarga_disponible(
    parametros: ParametrosElectricos,
    bateria_actual: float,
    inicio_minimo: int,
    fin_maximo: int,
    bateria_objetivo: float = 100.0,
) -> Optional[Tuple[int, int, float]]:
    """
    Calcula la ventana de recarga disponible respetando:
    - El tiempo mínimo de recarga configurado (SIEMPRE)
    - La ventana horaria de recarga permitida (SIEMPRE)
    """
    if fin_maximo <= inicio_minimo:
        return None
    
    # Verificar ventana horaria de recarga
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


def _verificar_factibilidad_bloque_electrico(
    bloque: List[Dict[str, Any]],
    parametros: ParametrosElectricos,
    gestor: GestorDeLogistica,
    verbose: bool,
) -> Tuple[bool, Optional[int]]:
    """
    Verifica si un bloque eléctrico puede cumplir con todas las recargas necesarias.
    
    Returns:
        (es_factible, punto_division)
        - es_factible: True si el bloque puede cumplir con todas las recargas
        - punto_division: Índice del viaje donde dividir si no es factible (None si es factible)
    """
    deposito_base = gestor.deposito_base
    bateria_actual = parametros.carga_inicial_pct
    minimo_circular = parametros.minimo_para_circular_pct
    
    for idx, viaje in enumerate(bloque):
        # Calcular consumo del viaje
        consumo_viaje = _calcular_consumo_viaje(viaje, parametros)
        bateria_despues_viaje = bateria_actual - consumo_viaje
        
        # Verificar si se requiere recarga antes del viaje
        requiere_recarga = (
            bateria_actual < minimo_circular
            or bateria_despues_viaje < minimo_circular
        )
        
        if requiere_recarga:
            # Calcular tiempo disponible antes del viaje
            if idx == 0:
                # Primer viaje: tiempo desde depósito
                t_vacio_ini, _ = gestor.buscar_tiempo_vacio(
                    deposito_base, viaje["origen"], viaje["inicio"]
                )
                t_vacio_ini = t_vacio_ini or 0
                tiempo_toma = gestor.tiempo_toma
                hora_salida_deposito = viaje["inicio"] - t_vacio_ini
                inicio_toma = hora_salida_deposito - tiempo_toma
                tiempo_disponible = viaje["inicio"] - inicio_toma
                origen_actual = deposito_base
            else:
                # Viaje intermedio: tiempo desde el viaje anterior
                viaje_anterior = bloque[idx - 1]
                tiempo_disponible = viaje["inicio"] - viaje_anterior["fin"]
                origen_actual = viaje_anterior["destino"]
            
            # Buscar oportunidad de recarga
            evento_recarga = _buscar_oportunidad_recarga(
                origen_actual,
                viaje["origen"],
                viaje["inicio"],
                bateria_actual,
                parametros,
                gestor,
                gestor.buscar_tiempo_vacio,
                tiempo_disponible,
                verbose,
                inicio_ventana=(inicio_toma if idx == 0 else viaje_anterior["fin"]),
            )
            
            if not evento_recarga:
                # No se puede planificar recarga obligatoria: dividir antes de este viaje
                return False, idx
        
        # Actualizar batería después del viaje
        bateria_actual = bateria_despues_viaje
    
    # Si llegamos aquí, el bloque es factible
    return True, None


def _dividir_bloque_electrico(
    bloque: List[Dict[str, Any]],
    punto_division: int,
    tipo_bus: Optional[str],
) -> List[List[Dict[str, Any]]]:
    """
    Divide un bloque eléctrico en el punto especificado.
    
    Args:
        bloque: Bloque original a dividir
        punto_division: Índice del viaje donde dividir (el bloque se divide ANTES de este viaje)
        tipo_bus: Tipo de bus del bloque
        
    Returns:
        Lista de bloques resultantes (normalmente 2 bloques)
    """
    if punto_division <= 0 or punto_division >= len(bloque):
        # No se puede dividir en este punto, retornar bloque original
        return [bloque]
    
    # Dividir el bloque
    bloque_1 = bloque[:punto_division]
    bloque_2 = bloque[punto_division:]
    
    # Asignar tipo de bus a ambos bloques
    for viaje in bloque_1:
        viaje["tipo_bus"] = tipo_bus
    for viaje in bloque_2:
        viaje["tipo_bus"] = tipo_bus
    
    return [bloque_1, bloque_2]

