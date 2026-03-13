from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from functools import lru_cache

from diagramador_optimizado.utils.time_utils import _to_minutes, formatear_hora
from diagramador_optimizado.core.domain.tipos_vehiculo import (
    ConfiguracionDeposito,
    ConfiguracionLinea,
    ConfiguracionTipoBus,
    normalizar_depositos_por_tipo,
    normalizar_lineas,
    normalizar_tipos_bus,
)


class GestorDeLogistica:
    """
    Encapsula TODA la lógica de negocio referente a paradas, vacíos,
    desplazamientos y cálculos de turnos. Este módulo es la única fuente
    de verdad para cualquier regla operacional usada en las fases del
    optimizador y en la exportación.
    """

    def __init__(self, config: Optional[Dict[str, Any]]) -> None:
        self.config: Dict[str, Any] = config or {}
        self.config.setdefault("paradas", {})
        self.config.setdefault("vacios", {})
        self.config.setdefault("desplazamientos", {})
        self.config.setdefault("deposito", "Deposito")
        self.config.setdefault("tiempo_toma", 15)
        self.config.setdefault("limite_jornada", 720)
        self.config.setdefault("limite_jornada_por_grupo_linea", {})
        self.config.setdefault("parada_larga_umbral", 60)
        self.config.setdefault("tiempo_min_deposito", 5)
        self.config.setdefault("tiempo_descanso_minimo", 0)
        # Índices normalizados para acelerar consultas repetidas
        self._indice_vacios, self._indice_desplazamientos = self._construir_indices_movimientos()

        self.tiempo_toma: int = int(self.config.get("tiempo_toma", 15))
        self.limite_jornada: int = int(self.config.get("limite_jornada", 720))
        raw_limites_grupo = self.config.get("limite_jornada_por_grupo_linea", {}) or {}
        self.limite_jornada_por_grupo_linea: Dict[str, int] = {}
        if isinstance(raw_limites_grupo, dict):
            for grupo, valor in raw_limites_grupo.items():
                try:
                    self.limite_jornada_por_grupo_linea[str(grupo).strip()] = int(float(valor))
                except Exception:
                    continue
        self.parada_larga_umbral: int = int(self.config.get("parada_larga_umbral", 60))
        self.tiempo_min_deposito: int = int(self.config.get("tiempo_min_deposito", 5))
        self._tiempo_descanso_minimo: int = int(self.config.get("tiempo_descanso_minimo", 0))
        self._t_de_dep_aprox: int = 30

        self.tipos_bus_config: Dict[str, ConfiguracionTipoBus] = normalizar_tipos_bus(self.config.get("tipos_bus"))
        self.tipos_bus_disponibles: List[str] = list(self.tipos_bus_config.keys())
        self.lineas_config: Dict[str, ConfiguracionLinea] = normalizar_lineas(
            self.config.get("lineas"),
            self.tipos_bus_disponibles,
        )

        self.permite_recarga_por_defecto: bool = bool(self.config.get("permite_recarga_por_defecto", True))
        self.posiciones_recarga_por_defecto: int = int(self.config.get("posiciones_recarga_por_defecto", 0))

        self.depositos_config: List[ConfiguracionDeposito] = self._normalizar_depositos(
            self.config,
            self.permite_recarga_por_defecto,
            self.posiciones_recarga_por_defecto,
            self.tipos_bus_disponibles,
        )
        self._depositos_nombres_cache: List[str] = [
            dep.nombre for dep in self.depositos_config if getattr(dep, "nombre", None)
        ]
        self._depositos_por_nombre: Dict[str, ConfiguracionDeposito] = {
            dep.nombre: dep for dep in self.depositos_config if dep.nombre
        }
        self.deposito_base: str = (
            self.depositos_config[0].nombre
            if self.depositos_config
            else self.config.get("deposito", "Deposito")
        )
        
        # Logging para depuración: verificar que se cargaron todos los depósitos
        nombres_cargados = [dep.nombre for dep in self.depositos_config]
        print(f"\n[GestorDeLogistica] Depósitos cargados desde configuración: {len(nombres_cargados)}")
        for i, nombre in enumerate(nombres_cargados, 1):
            dep_config = self._depositos_por_nombre.get(nombre)
            max_buses = dep_config.max_buses if dep_config else "N/A"
            print(f"  {i}. {nombre} (max_buses: {max_buses})")
        if len(nombres_cargados) > 1:
            print(f"  [INFO] MÚLTIPLES DEPÓSITOS configurados - El sistema buscará el mejor para cada viaje")
        print(f"  Depósito base (fallback): {self.deposito_base}\n")
        self.flota_total_por_tipo: Dict[str, int] = self._resumir_flota_por_tipo()
        self.paradas_dict: Dict[str, Dict[str, Any]] = {k.upper(): v for k, v in self.config.get("paradas", {}).items()}
        
        # Configuración de grupos de líneas para interlineados
        self.grupos_lineas: Dict[str, Set[str]] = self._normalizar_grupos_lineas()
        self.interlineado_global: bool = self.config.get("interlineado_global", False)
        # REGLA DURA: si respetar_grupos_lineas=true, bus y conductor SOLO operan en su grupo (ignora interlineado_global)
        self.respetar_grupos_lineas: bool = self.config.get("respetar_grupos_lineas", bool(self.grupos_lineas))

        # Puntos de relevo: se determinan DINÁMICAMENTE por desplazamientos habilitados.
        # Un nodo es punto de relevo si tiene desplazamiento habilitado hacia algún depósito configurado
        # (para aplicar FnS/InS). Lista opcional en config solo filtra cuando está definida y no vacía.
        pr = self.config.get("puntos_relevo", [])
        self.puntos_relevo: Set[str] = {str(n).strip().upper() for n in pr} if isinstance(pr, (list, tuple)) else set()

        # Tipos de conductor: rangos de ingreso y fin de jornada (en minutos desde 00:00)
        self.tipos_conductor: List[Dict[str, Any]] = self._normalizar_tipos_conductor()

    # ------------------------------------------------------------------
    # Métodos auxiliares de configuración
    # ------------------------------------------------------------------
    def _construir_indices_movimientos(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Precalcula índices normalizados para vacíos y desplazamientos:
        - por_clave: acceso directo ORIGEN_DESTINO en mayúsculas
        - fallback: lista de pares (origen, destino, entrada) para matching flexible
        """
        def _build(movimientos: Any) -> Dict[str, Any]:
            por_clave: Dict[str, Any] = {}
            fallback: List[Tuple[str, str, Any]] = []
            if isinstance(movimientos, dict):
                for clave, entrada in movimientos.items():
                    if not isinstance(entrada, dict):
                        continue
                    partes = clave.split("_", 1)
                    if len(partes) != 2:
                        continue
                    origen = partes[0].strip().upper()
                    destino = partes[1].strip().upper()
                    if not origen or not destino:
                        continue
                    llave = f"{origen}_{destino}"
                    por_clave[llave] = entrada
                    fallback.append((origen, destino, entrada))
            return {"por_clave": por_clave, "fallback": fallback}

        return _build(self.config.get("vacios", {})), _build(self.config.get("desplazamientos", {}))

    def _normalizar_depositos(
        self,
        config: Dict[str, Any],
        permite_recarga_por_defecto: bool,
        posiciones_recarga_por_defecto: int,
        tipos_disponibles: Sequence[str],
    ) -> List[ConfiguracionDeposito]:
        """
        Centraliza la normalización de depósitos y sus flotas por tipo.
        """
        return normalizar_depositos_por_tipo(
            config,
            tipos_disponibles,
            permite_recarga_por_defecto,
            posiciones_recarga_por_defecto,
            200,
        )

    def _resumir_flota_por_tipo(self) -> Dict[str, int]:
        """
        Suma la flota disponible por tipo a partir de todos los depósitos.
        """
        totales: Dict[str, int] = {tipo: 0 for tipo in self.tipos_bus_disponibles}
        for deposito in self.depositos_config:
            for tipo, cantidad in deposito.flota_por_tipo.items():
                totales[tipo] = totales.get(tipo, 0) + max(0, int(cantidad))
        return totales

    def _normalizar_grupos_lineas(self) -> Dict[str, Set[str]]:
        """
        Normaliza la configuración de grupos de líneas para interlineados.
        
        Returns:
            Diccionario donde la clave es el nombre del grupo y el valor es un set de líneas
        """
        grupos_config = self.config.get("grupos_lineas", {})
        grupos_normalizados: Dict[str, Set[str]] = {}
        
        if isinstance(grupos_config, dict):
            for grupo_nombre, lineas in grupos_config.items():
                if isinstance(lineas, (list, set)):
                    # Normalizar nombres de líneas a mayúsculas
                    lineas_normalizadas = {str(linea).strip().upper() for linea in lineas if linea}
                    if lineas_normalizadas:
                        grupos_normalizados[str(grupo_nombre).strip()] = lineas_normalizadas
        
        return grupos_normalizados

    def _normalizar_tipos_conductor(self) -> List[Dict[str, Any]]:
        """
        Normaliza tipos de conductor: convierte rangos HH:MM a minutos desde 00:00.
        Cada tipo tiene: id, nombre, rango_ingreso_min, rango_ingreso_max, rango_fin_min, rango_fin_max.
        """
        raw = self.config.get("tipos_conductor") or []
        if not isinstance(raw, list):
            return []
        result: List[Dict[str, Any]] = []
        for tc in raw:
            if not isinstance(tc, dict):
                continue
            tid = str(tc.get("id", "") or tc.get("nombre", "")).strip()
            if not tid:
                continue
            ri = tc.get("rango_ingreso") or {}
            rf = tc.get("rango_fin_jornada") or {}
            rango_ing_min = _to_minutes(ri.get("min", "00:00"))
            rango_ing_max = _to_minutes(ri.get("max", "23:59"))
            rango_fin_min = _to_minutes(rf.get("min", "00:00"))
            rango_fin_max = _to_minutes(rf.get("max", "23:59"))
            result.append({
                "id": tid,
                "nombre": str(tc.get("nombre", tid)).strip(),
                "rango_ingreso_min": rango_ing_min,
                "rango_ingreso_max": rango_ing_max,
                "rango_fin_min": rango_fin_min,
                "rango_fin_max": rango_fin_max,
            })
        return result

    def clasificar_turno_por_tipo(
        self, inicio_pago_minutos: int, fin_pago_minutos: int
    ) -> Optional[str]:
        """
        Clasifica un turno (inicio y fin de pago en minutos desde 00:00) en un tipo de conductor.
        Un turno pertenece a un tipo si:
        - inicio_pago está dentro de [rango_ingreso.min, rango_ingreso.max]
        - fin_pago está dentro de [rango_fin_jornada.min, rango_fin_jornada.max]
        Los horarios son del mismo día (0-1439). Si fin < inicio (cruce de medianoche),
        se considera fin al día siguiente y se compara fin_pago con rango_fin en 0-1439.
        
        Returns:
            id del tipo de conductor si hay coincidencia, None si no hay tipos o no coincide ninguno.
        """
        if not self.tipos_conductor:
            return None
        for tc in self.tipos_conductor:
            ing_min = tc["rango_ingreso_min"]
            ing_max = tc["rango_ingreso_max"]
            fin_min = tc["rango_fin_min"]
            fin_max = tc["rango_fin_max"]
            if ing_min <= inicio_pago_minutos <= ing_max and fin_min <= fin_pago_minutos <= fin_max:
                return tc["id"]
        return None

    def obtener_tipo_conductor_mas_cercano(
        self, inicio_pago_minutos: int, fin_pago_minutos: int
    ) -> Optional[str]:
        """
        Si el turno no clasifica en ningún tipo, devuelve el tipo cuyo rango esté más cercano
        (por distancia al centro del rango de ingreso). Útil para no dejar turnos sin tipo cuando hay tipos definidos.
        """
        if not self.tipos_conductor:
            return None
        mejor_tipo: Optional[str] = None
        mejor_dist: Optional[float] = None
        for tc in self.tipos_conductor:
            centro_ing = (tc["rango_ingreso_min"] + tc["rango_ingreso_max"]) / 2
            dist = abs(inicio_pago_minutos - centro_ing)
            if dist > 12 * 60:  # más de 12 horas de diferencia, poco razonable
                continue
            if mejor_dist is None or dist < mejor_dist:
                mejor_dist = dist
                mejor_tipo = tc["id"]
        return mejor_tipo

    def _nombres_depositos(self) -> List[str]:
        """Devuelve la lista de nombres de todos los depósitos configurados."""
        if self._depositos_nombres_cache:
            return self._depositos_nombres_cache
        if self.depositos_config:
            self._depositos_nombres_cache = [
                dep.nombre for dep in self.depositos_config if dep.nombre
            ]
            if self._depositos_nombres_cache:
                return self._depositos_nombres_cache
        deposito = self.config.get("deposito")
        return [deposito] if deposito else []

    def permite_recarga_en_deposito(self, nombre_deposito: Optional[str]) -> bool:
        """
        Indica si un depósito permite operaciones de recarga según la configuración.
        """
        if not nombre_deposito:
            return False
        datos = self._depositos_por_nombre.get(nombre_deposito)
        if datos:
            return bool(datos.permite_recarga)
        return self.permite_recarga_por_defecto

    def posiciones_recarga_en_deposito(self, nombre_deposito: Optional[str]) -> Optional[int]:
        """
        Devuelve el número de posiciones disponibles en el punto de recarga del depósito (si aplica).
        """
        if not nombre_deposito:
            return None
        datos = self._depositos_por_nombre.get(nombre_deposito)
        posiciones = None
        if datos:
            posiciones = datos.posiciones_recarga or self.posiciones_recarga_por_defecto
        else:
            posiciones = self.posiciones_recarga_por_defecto
        return posiciones if posiciones else None

    def tipos_permitidos_para_linea(self, nombre_linea: Optional[str]) -> List[str]:
        """
        Devuelve los tipos autorizados para la línea especificada;
        en caso de no existir configuración, se devuelven todos los tipos disponibles.
        """
        if not nombre_linea:
            return list(self.tipos_bus_disponibles)
        clave = str(nombre_linea).strip().upper()
        configuracion = self.lineas_config.get(clave)
        if configuracion:
            return configuracion.tipos_permitidos
        return list(self.tipos_bus_disponibles)

    def obtener_configuracion_linea(self, nombre_linea: Optional[str]) -> Optional[ConfiguracionLinea]:
        """
        Retorna la configuración completa de una línea si existe.
        """
        if not nombre_linea:
            return None
        clave = str(nombre_linea).strip().upper()
        return self.lineas_config.get(clave)

    def obtener_tipo_bus(self, nombre_tipo: Optional[str]) -> Optional[ConfiguracionTipoBus]:
        """
        Obtiene la configuración detallada para un tipo de bus determinado.
        """
        if not nombre_tipo:
            return None
        return self.tipos_bus_config.get(str(nombre_tipo).strip().upper())

    def pueden_interlinear(self, linea1: Optional[str], linea2: Optional[str]) -> bool:
        """
        Verifica si dos líneas pueden interlinear entre sí.
        
        REGLAS:
        - Si interlineado_global es True: todas las líneas pueden interlinear
        - Si interlineado_global es False: solo pueden interlinear si están en el mismo grupo
        - Si una línea no tiene grupo o no existe: no puede interlinear (excepto si es global)
        
        Args:
            linea1: Nombre de la primera línea
            linea2: Nombre de la segunda línea
            
        Returns:
            True si pueden interlinear, False en caso contrario
        """
        # Si es el mismo viaje (misma línea), siempre puede continuar
        if not linea1 or not linea2:
            return False
        
        linea1_norm = str(linea1).strip().upper()
        linea2_norm = str(linea2).strip().upper()
        
        # Misma línea siempre puede continuar
        if linea1_norm == linea2_norm:
            return True
        
        # REGLA DURA: respetar_grupos_lineas obliga a que bus/conductor solo operen en su grupo
        if getattr(self, "respetar_grupos_lineas", False):
            for grupo_nombre, lineas_grupo in self.grupos_lineas.items():
                if linea1_norm in lineas_grupo and linea2_norm in lineas_grupo:
                    return True
            return False
        
        # Si interlineado global está habilitado, todas las líneas pueden interlinear
        if self.interlineado_global:
            return True
        
        # Si no hay grupos configurados, no se permite interlineado
        if not self.grupos_lineas:
            return False
        
        # Buscar si ambas líneas están en el mismo grupo
        for grupo_nombre, lineas_grupo in self.grupos_lineas.items():
            if linea1_norm in lineas_grupo and linea2_norm in lineas_grupo:
                return True
        
        # No están en el mismo grupo
        return False

    def obtener_grupo_linea(self, nombre_linea: Optional[str]) -> Optional[str]:
        """
        Obtiene el nombre del grupo al que pertenece una línea.
        
        Args:
            nombre_linea: Nombre de la línea
            
        Returns:
            Nombre del grupo si existe, None en caso contrario
        """
        if not nombre_linea:
            return None
        
        linea_norm = str(nombre_linea).strip().upper()
        
        for grupo_nombre, lineas_grupo in self.grupos_lineas.items():
            if linea_norm in lineas_grupo:
                return grupo_nombre
        
        return None

    def limite_jornada_para_linea(self, nombre_linea: Optional[str]) -> int:
        """
        Límite de jornada aplicable a la línea según grupo.
        Si no hay override de grupo, usa el límite global.
        """
        grupo = self.obtener_grupo_linea(nombre_linea)
        if grupo:
            override = self.limite_jornada_por_grupo_linea.get(grupo)
            if override is not None:
                return int(override)
        return int(self.limite_jornada)

    def tiempo_descanso_minimo(self) -> int:
        """
        Retorna el descanso mínimo requerido entre tareas del mismo conductor.
        """
        return self._tiempo_descanso_minimo

    # ------------------------------------------------------------------
    # Funciones movidas desde el script original
    # ------------------------------------------------------------------
    def buscar_tiempo_vacio(
        self,
        origen: str,
        destino: str,
        minutos_actuales: int,
        _permitir_inverso: bool = True,
    ) -> Tuple[Optional[int], int]:
        """
        Consulta la tabla de vacíos para obtener la duración y kilómetros asociados.
        Usa caché por (origen, destino, minuto) para evitar recomputar.
        """
        origen_norm = str(origen).strip().upper()
        destino_norm = str(destino).strip().upper()
        try:
            minutos_int = int(minutos_actuales)
        except Exception:
            minutos_int = 0
        return self._buscar_tiempo_vacio_cached(origen_norm, destino_norm, minutos_int, bool(_permitir_inverso))

    @lru_cache(maxsize=20000)
    def _buscar_tiempo_vacio_cached(
        self,
        origen_norm: str,
        destino_norm: str,
        minutos_int: int,
        permitir_inverso: bool,
    ) -> Tuple[Optional[int], int]:
        vacios = self.config.get("vacios", {})
        if not origen_norm or not destino_norm or not vacios:
            return None, 0

        clave = f"{origen_norm}_{destino_norm}"
        entrada = vacios.get(clave)

        if not isinstance(entrada, dict):
            for clave_config, valor_config in vacios.items():
                if not isinstance(valor_config, dict):
                    continue
                partes = clave_config.split("_", 1)
                if len(partes) == 2:
                    origen_config = partes[0].strip().upper()
                    destino_config = partes[1].strip().upper()
                    o_n = origen_norm.upper()
                    d_n = destino_norm.upper()
                    if origen_config == o_n and destino_config == d_n:
                        clave = clave_config
                        entrada = valor_config
                        break
                    # Incluir variantes (ej. LA PIRAMI = LA PIRAMIDE)
                    o_ok = (origen_config == o_n or origen_config in o_n or o_n in origen_config or
                            (len(o_n) >= 6 and len(origen_config) >= 6 and (o_n.startswith(origen_config) or origen_config.startswith(o_n))))
                    d_ok = (destino_config == d_n or destino_config in d_n or d_n in destino_config or
                            (len(d_n) >= 6 and len(destino_config) >= 6 and (d_n.startswith(destino_config) or destino_config.startswith(d_n))))
                    if o_ok and d_ok:
                        clave = clave_config
                        entrada = valor_config
                        break

        if not isinstance(entrada, dict):
            return None, 0

        if not entrada.get("habilitado", True):
            return None, 0

        franjas = entrada.get("franjas", entrada if isinstance(entrada, list) else [])
        if not franjas:
            return None, 0

        for franja in franjas:
            if not isinstance(franja, dict):
                continue
            inicio_franja = _to_minutes(franja.get("inicio", "00:00"))
            fin_franja = _to_minutes(franja.get("fin", "00:00"))
            if fin_franja <= inicio_franja:
                fin_franja = 24 * 60 * 2
            if inicio_franja <= minutos_int <= fin_franja:
                tiempo = franja.get("tiempo")
                kilometros = franja.get("km", 0)
                if tiempo is not None:
                    try:
                        tiempo_num = float(tiempo)
                    except (ValueError, TypeError):
                        tiempo_num = None

                    if (
                        tiempo_num is not None
                        and tiempo_num <= 1.0
                        and origen_norm.upper() != destino_norm.upper()
                        and permitir_inverso
                    ):
                        tiempo_inv, km_inv = self._buscar_tiempo_vacio_cached(
                            destino_norm,
                            origen_norm,
                            minutos_int,
                            False,
                        )
                        if tiempo_inv is not None and tiempo_inv > 1:
                            return tiempo_inv, km_inv
                        if tiempo_num and tiempo_num > 0:
                            return int(tiempo_num), kilometros
                        return None, 0
                    if tiempo_num is not None and tiempo_num > 0:
                        return int(tiempo_num), kilometros
                    return None, 0

        if franjas and isinstance(franjas[0], dict):
            tiempo = franjas[0].get("tiempo")
            kilometros = franjas[0].get("km", 0)
            if tiempo is not None:
                try:
                    tiempo_num = float(tiempo)
                    if tiempo_num > 0:
                        return int(tiempo_num), kilometros
                except (ValueError, TypeError):
                    pass

        return None, 0

    def _nodos_coinciden_para_desplaz(self, a: str, b: str) -> bool:
        """Compara nodos para matching de desplazamientos (ej. LA PIRAMI = LA PIRAMIDE)."""
        if not a or not b:
            return False
        na, nb = a.strip().upper(), b.strip().upper()
        if na == nb:
            return True
        if na in nb or nb in na:
            return True
        if len(na) >= 6 and len(nb) >= 6 and (na.startswith(nb) or nb.startswith(na)):
            return True
        return False

    def _alias_deposito_para_conectividad(self) -> Set[str]:
        """Nodos que se consideran el mismo lugar que el depósito (desplazamiento <= 1 min ida y vuelta)."""
        cache = getattr(self, "_alias_deposito_cache", None)
        if cache is not None:
            return cache
        dep = (self.deposito_base or "").strip().upper()
        if not dep:
            self._alias_deposito_cache = set()
            return set()
        alias: Set[str] = set()
        idx = getattr(self, "_indice_desplazamientos", {})
        por_clave = idx.get("por_clave", {}) if isinstance(idx, dict) else {}
        for clave, regla in por_clave.items():
            if not isinstance(regla, dict) or not regla.get("habilitado", False):
                continue
            partes = clave.split("_", 1)
            if len(partes) != 2:
                continue
            o, d = partes[0].strip().upper(), partes[1].strip().upper()
            if not o or not d:
                continue
            t = regla.get("tiempo")
            if t is None and isinstance(regla.get("franjas"), list) and regla["franjas"]:
                t = regla["franjas"][0].get("tiempo")
            try:
                t_min = int(float(t)) if t is not None else 999
            except (TypeError, ValueError):
                t_min = 999
            if t_min > 1:
                continue
            if o == dep or d == dep:
                alias.add(o if o != dep else d)
        self._alias_deposito_cache = alias
        return alias

    def nodo_canonico_para_conectividad(self, nodo: str) -> str:
        """
        Devuelve el nombre canónico del nodo para encadenamiento/conectividad.
        Depósito y sus alias (ej. JUANITA cuando el depósito es Deposito Juanita)
        se normalizan al nombre del depósito base, para evitar falsos teletransportes.
        """
        if not nodo or not (nodo := str(nodo).strip()):
            return ""
        n_upper = nodo.upper()
        dep = (self.deposito_base or "").strip()
        dep_upper = dep.upper()
        if n_upper == dep_upper:
            return dep
        if "DEPOSITO" in n_upper and "DEPOSITO" in dep_upper and (n_upper in dep_upper or dep_upper in n_upper):
            return dep
        if n_upper in self._alias_deposito_para_conectividad():
            return dep
        return nodo

    def buscar_info_desplazamiento(self, origen: str, destino: str, minutos_actuales: int = 0) -> Tuple[bool, Optional[int]]:
        """
        Revisa la configuración de desplazamientos de conductores.
        Usa caché por (origen, destino, minuto) para acelerar búsquedas repetidas.
        Intenta nombres canónicos de config si el lookup directo falla (ej. LA PIRAMI -> LA PIRAMIDE).
        """
        origen_norm = str(origen).strip().upper()
        destino_norm = str(destino).strip().upper()
        try:
            minutos_int = int(minutos_actuales)
        except Exception:
            minutos_int = 0
        hab, t = self._buscar_info_desplazamiento_cached(origen_norm, destino_norm, minutos_int)
        if hab and t is not None:
            return True, t
        # Fallback: variantes desde config (LA PIRAMI vs LA PIRAMIDE; LOS TILOS LA PIRAMI -> LOS TILOS)
        nodos_cfg = (self.config or {}).get("nodos", [])
        nombres_dep = self._nombres_depositos() or [self.deposito_base]
        for n in nodos_cfg:
            n_upper = (str(n or "").strip().upper())
            if not n_upper:
                continue
            orig_canon = n_upper if self._nodos_coinciden_para_desplaz(n_upper, origen_norm) else None
            dest_canon = n_upper if self._nodos_coinciden_para_desplaz(n_upper, destino_norm) else None
            for dep in nombres_dep:
                dep_upper = (str(dep or "").strip().upper())
                dest_ok = dep_upper == destino_norm or destino_norm in dep_upper or dep_upper in destino_norm
                if not dest_ok:
                    continue
                o_try = orig_canon if orig_canon else origen_norm
                d_try = dep_upper
                if o_try == origen_norm and d_try == destino_norm:
                    continue
                hab2, t2 = self._buscar_info_desplazamiento_cached(o_try, d_try, minutos_int)
                if hab2 and t2 is not None:
                    return True, t2
        # Fallback destino: origen del viaje puede ser "LOS TILOS LA PIRAMI"; en config está "LOS TILOS"
        for n in nodos_cfg:
            n_upper = (str(n or "").strip().upper())
            if not n_upper or not self._nodos_coinciden_para_desplaz(n_upper, destino_norm):
                continue
            if n_upper == destino_norm:
                continue
            hab2, t2 = self._buscar_info_desplazamiento_cached(origen_norm, n_upper, minutos_int)
            if hab2 and t2 is not None:
                return True, t2
        return False, None

    @lru_cache(maxsize=20000)
    def _buscar_info_desplazamiento_cached(
        self,
        origen_norm: str,
        destino_norm: str,
        minutos_int: int,
    ) -> Tuple[bool, Optional[int]]:
        desplazamientos_idx = getattr(self, "_indice_desplazamientos", {"por_clave": {}, "fallback": []})
        if not origen_norm or not destino_norm:
            return False, None

        clave = f"{origen_norm}_{destino_norm}"
        regla = desplazamientos_idx.get("por_clave", {}).get(clave)

        # REGLA DURA: Solo coincidencia EXACTA. Nunca matching flexible (evitar desplazamientos no autorizados).
        if not isinstance(regla, dict):
            for origen_cfg, destino_cfg, valor_config in desplazamientos_idx.get("fallback", []):
                if origen_cfg == origen_norm and destino_cfg == destino_norm:
                    regla = valor_config
                    break

        if not isinstance(regla, dict):
            return False, None
        if not regla.get("habilitado", False):
            return False, None

        franjas = regla.get("franjas", [])
        if franjas and isinstance(franjas, list):
            for franja in franjas:
                if not isinstance(franja, dict):
                    continue
                inicio_franja = _to_minutes(franja.get("inicio", "00:00"))
                fin_franja = _to_minutes(franja.get("fin", "00:00"))
                if fin_franja <= inicio_franja:
                    fin_franja = 24 * 60 * 2
                if inicio_franja <= minutos_int <= fin_franja:
                    tiempo = franja.get("tiempo")
                    if tiempo is not None:
                        try:
                            tiempo_int = int(float(tiempo))
                            return True, tiempo_int
                        except (ValueError, TypeError):
                            continue

            if franjas and isinstance(franjas[0], dict):
                tiempo = franjas[0].get("tiempo")
                if tiempo is not None:
                    try:
                        tiempo_int = int(float(tiempo))
                        return True, tiempo_int
                    except (ValueError, TypeError):
                        pass

        tiempo = regla.get("tiempo")
        if tiempo is not None:
            if tiempo == 0 or tiempo == 0.0:
                tiempo_vacio, _ = self._buscar_tiempo_vacio_cached(origen_norm, destino_norm, minutos_int, True)
                if tiempo_vacio is not None and tiempo_vacio > 0:
                    return True, tiempo_vacio
                return True, 5
            try:
                tiempo_int = int(float(tiempo))
                return True, tiempo_int
            except (ValueError, TypeError):
                pass

        print(
            f"ADVERTENCIA (Config): Desplazamiento {clave} está habilitado pero no tiene 'tiempo' ni 'franjas'. "
            "Intentando usar tiempo de vacío como fallback..."
        )
        tiempo_vacio, _ = self._buscar_tiempo_vacio_cached(origen_norm, destino_norm, minutos_int, True)
        if tiempo_vacio is not None and tiempo_vacio > 0:
            return True, tiempo_vacio
        print(
            f"ADVERTENCIA (Config): No se encontró vacío para {clave}. Se considerará no habilitado."
        )
        return False, None

    def get_inicio_turno_conductor(self, meta_primera: Dict[str, Any], devolver_detalle: bool = False, deposito_bus: Optional[str] = None):
        """
        Calcula el inicio de pago de un turno basándose en la primera tarea asignada.
        SOPORTA MÚLTIPLES DEPÓSITOS: Si se proporciona deposito_bus, usa ese depósito.
        Si no, busca entre todos los depósitos configurados para encontrar la mejor opción.

        Args:
            meta_primera: Metadata de la primera tarea del turno.
            devolver_detalle: Si es True devuelve un tercer elemento con información del movimiento.
            deposito_bus: Depósito del bus. Si se proporciona, el conductor debe iniciar desde ahí.

        Returns:
            (bool, inicio_pago) o (bool, inicio_pago, detalle_movimiento).
        """
        # REGLA CRÍTICA: Si se especifica deposito_bus, el conductor DEBE iniciar desde ahí
        deposito_a_usar = deposito_bus
        
        # Obtener todos los depósitos disponibles
        nombres_depositos = self._nombres_depositos()
        if not nombres_depositos:
            nombres_depositos = [self.deposito_base]
        
        # Si se especificó deposito_bus, validar que existe
        if deposito_a_usar:
            if deposito_a_usar not in nombres_depositos:
                # Depósito del bus no válido, usar el mejor disponible
                deposito_a_usar = None
        
        destino_busqueda = meta_primera["viaje"]["origen"]
        tiempo_referencia = meta_primera["viaje"]["inicio"]
        
        mejor_opcion: Optional[Dict[str, Any]] = None
        
        if meta_primera.get("es_primero", False):
            # Primer viaje del bloque: buscar vacío (bus sale del depósito hacia el nodo)
            if deposito_a_usar:
                vacio, kilometros = self.buscar_tiempo_vacio(deposito_a_usar, destino_busqueda, tiempo_referencia)
                if vacio is not None:
                    hora_inicio_mov = tiempo_referencia - vacio
                    mejor_opcion = {
                        "deposito": deposito_a_usar,
                        "modo": "vacio",
                        "kilometros": kilometros or 0,
                        "duracion_movimiento": vacio,
                        "hora_inicio_movimiento": hora_inicio_mov,
                    }
            else:
                for deposito in nombres_depositos:
                    vacio, kilometros = self.buscar_tiempo_vacio(deposito, destino_busqueda, tiempo_referencia)
                    if vacio is not None:
                        hora_inicio_mov = tiempo_referencia - vacio
                        opcion = {
                            "deposito": deposito,
                            "modo": "vacio",
                            "kilometros": kilometros or 0,
                            "duracion_movimiento": vacio,
                            "hora_inicio_movimiento": hora_inicio_mov,
                        }
                        if mejor_opcion is None or hora_inicio_mov > mejor_opcion["hora_inicio_movimiento"]:
                            mejor_opcion = opcion
        else:
            # Relevo: conductor llega al nodo. Preferir desplazamiento; si no hay, usar vacío (puede tomar otro bus)
            if deposito_a_usar:
                habilitado, tiempo_despl = self.buscar_info_desplazamiento(deposito_a_usar, destino_busqueda, tiempo_referencia)
                if habilitado and tiempo_despl is not None:
                    hora_inicio_mov = tiempo_referencia - tiempo_despl
                    mejor_opcion = {
                        "deposito": deposito_a_usar,
                        "modo": "desplazamiento",
                        "kilometros": 0,
                        "duracion_movimiento": tiempo_despl,
                        "hora_inicio_movimiento": hora_inicio_mov,
                    }
                if mejor_opcion is None:
                    vacio, kilometros = self.buscar_tiempo_vacio(deposito_a_usar, destino_busqueda, tiempo_referencia)
                    if vacio is not None:
                        hora_inicio_mov = tiempo_referencia - vacio
                        mejor_opcion = {
                            "deposito": deposito_a_usar,
                            "modo": "vacio",
                            "kilometros": kilometros or 0,
                            "duracion_movimiento": vacio,
                            "hora_inicio_movimiento": hora_inicio_mov,
                        }
            else:
                for deposito in nombres_depositos:
                    habilitado, tiempo_despl = self.buscar_info_desplazamiento(deposito, destino_busqueda, tiempo_referencia)
                    if habilitado and tiempo_despl is not None:
                        hora_inicio_mov = tiempo_referencia - tiempo_despl
                        opcion = {
                            "deposito": deposito,
                            "modo": "desplazamiento",
                            "kilometros": 0,
                            "duracion_movimiento": tiempo_despl,
                            "hora_inicio_movimiento": hora_inicio_mov,
                        }
                        if mejor_opcion is None or hora_inicio_mov > mejor_opcion["hora_inicio_movimiento"]:
                            mejor_opcion = opcion
                if mejor_opcion is None:
                    for deposito in nombres_depositos:
                        vacio, kilometros = self.buscar_tiempo_vacio(deposito, destino_busqueda, tiempo_referencia)
                        if vacio is not None:
                            hora_inicio_mov = tiempo_referencia - vacio
                            opcion = {
                                "deposito": deposito,
                                "modo": "vacio",
                                "kilometros": kilometros or 0,
                                "duracion_movimiento": vacio,
                                "hora_inicio_movimiento": hora_inicio_mov,
                            }
                            if mejor_opcion is None or hora_inicio_mov > mejor_opcion["hora_inicio_movimiento"]:
                                mejor_opcion = opcion

        if mejor_opcion is None:
            # Si no se encontró opción, usar depósito base como fallback (compatibilidad)
            deposito_fallback = nombres_depositos[0] if nombres_depositos else self.deposito_base
            detalle = {
                "modo": None,
                "origen": deposito_fallback,
                "destino": destino_busqueda,
                "kilometros": 0,
                "duracion_movimiento": 0,
            }
            mensaje = (
                f"ERROR (Inicio Turno): No se encontró vacío ni desplazamiento habilitado "
                f"desde ningún depósito hacia {destino_busqueda}."
            )
            print(mensaje)
            return (False, 0, detalle) if devolver_detalle else (False, 0)
        
        detalle = {
            "modo": mejor_opcion["modo"],
            "origen": mejor_opcion["deposito"],
            "destino": destino_busqueda,
            "kilometros": mejor_opcion["kilometros"],
            "duracion_movimiento": mejor_opcion["duracion_movimiento"],
        }
        
        hora_inicio_movimiento = mejor_opcion["hora_inicio_movimiento"]
        inicio_pago = hora_inicio_movimiento - self.tiempo_toma
        
        if devolver_detalle:
            detalle["inicio_movimiento"] = hora_inicio_movimiento
            detalle["inicio_pago"] = inicio_pago
            return True, inicio_pago, detalle
        return True, inicio_pago

    def _get_inicio_turno(self, meta_primera: Dict[str, Any], devolver_detalle: bool = False):
        """Compatibilidad retro. Usar get_inicio_turno_conductor."""
        return self.get_inicio_turno_conductor(meta_primera, devolver_detalle)

    def get_fin_turno_conductor(self, meta_ultima: Dict[str, Any], devolver_detalle: bool = False, deposito_inicio: Optional[str] = None):
        """
        Determina el fin de pago de un turno considerando la última tarea.
        El conductor SIEMPRE termina en el depósito (donde inició).

        - Si el último viaje termina EN el depósito: modo "en_deposito"; no hay movimiento; fin_pago = fin del viaje.
        - Si termina en un nodo que NO es depósito (p. ej. terminal): DESPLAZAMIENTO (nodo -> depósito).
          El conductor deja el bus y va al depósito. Solo se usa vacío si no hay desplazamiento habilitado.

        Args:
            meta_ultima: Metadata de la última tarea del turno.
            devolver_detalle: Si es True devuelve un tercer elemento con información del movimiento.
            deposito_inicio: Depósito donde inició el turno. El conductor debe terminar ahí.
        """
        # REGLA CRÍTICA: Si se especifica deposito_inicio, el conductor DEBE terminar ahí
        deposito_destino = deposito_inicio
        
        # Obtener todos los depósitos disponibles
        nombres_depositos = self._nombres_depositos()
        if not nombres_depositos:
            nombres_depositos = [self.deposito_base]
        
        # Si no se especificó deposito_inicio, buscar entre todos (comportamiento anterior)
        # Si se especificó, validar que existe en la configuración
        if deposito_destino:
            if deposito_destino not in nombres_depositos:
                # Depósito de inicio no válido, usar el mejor disponible
                deposito_destino = None
        
        origen_relevo = meta_ultima["viaje"]["destino"]
        tiempo_referencia = meta_ultima["viaje"]["fin"]
        
        mejor_opcion: Optional[Dict[str, Any]] = None

        depositos_a_evaluar = [deposito_destino] if deposito_destino else nombres_depositos

        # Si el último viaje termina YA en el depósito → no hay movimiento; FnS en el mismo lugar.
        ya_en_deposito = origen_relevo in nombres_depositos and (
            not deposito_destino or origen_relevo == deposito_destino
        )
        if ya_en_deposito:
            dep = deposito_destino or origen_relevo
            mejor_opcion = {
                "deposito": dep,
                "modo": "en_deposito",
                "kilometros": 0,
                "duracion_movimiento": 0,
                "fin_pago": tiempo_referencia,
            }
        else:
            # Último viaje termina en nodo no-depósito (p. ej. terminal) → DESPLAZAMIENTO (nodo -> depósito).
            # El conductor deja el bus y va al depósito. Solo vacío si no hay desplazamiento.
            mejor_opcion = None
            for deposito in depositos_a_evaluar:
                hab, t_d = self.buscar_info_desplazamiento(origen_relevo, deposito, tiempo_referencia)
                if hab and t_d is not None:
                    opcion = {
                        "deposito": deposito,
                        "modo": "desplazamiento",
                        "kilometros": 0,
                        "duracion_movimiento": t_d,
                        "fin_pago": tiempo_referencia + t_d,
                    }
                    if mejor_opcion is None or t_d < mejor_opcion["duracion_movimiento"]:
                        mejor_opcion = opcion
            if mejor_opcion is None:
                for deposito in depositos_a_evaluar:
                    vacio, km = self.buscar_tiempo_vacio(origen_relevo, deposito, tiempo_referencia)
                    if vacio is not None:
                        opcion = {
                            "deposito": deposito,
                            "modo": "vacio",
                            "kilometros": km or 0,
                            "duracion_movimiento": vacio,
                            "fin_pago": tiempo_referencia + vacio,
                        }
                        if mejor_opcion is None or vacio < mejor_opcion["duracion_movimiento"]:
                            mejor_opcion = opcion

        if mejor_opcion is None:
            # Si no se encontró opción, usar depósito base como fallback (compatibilidad)
            deposito_fallback = nombres_depositos[0] if nombres_depositos else self.deposito_base
            detalle = {
                "modo": None,
                "origen": origen_relevo,
                "destino": deposito_fallback,
                "kilometros": 0,
                "duracion_movimiento": 0,
            }
            print(
                f"ERROR (Fin Turno): No se encontró vacío del bus programado ni desplazamiento habilitado "
                f"desde {origen_relevo} hacia ningún depósito."
            )
            return (False, 0, detalle) if devolver_detalle else (False, 0)
        
        detalle = {
            "modo": mejor_opcion["modo"],
            "origen": origen_relevo,
            "destino": mejor_opcion["deposito"],
            "kilometros": mejor_opcion["kilometros"],
            "duracion_movimiento": mejor_opcion["duracion_movimiento"],
        }
        
        fin_pago = mejor_opcion["fin_pago"]

        if devolver_detalle:
            detalle["fin_pago"] = fin_pago
            return True, fin_pago, detalle
        return True, fin_pago

    def _get_fin_turno(self, meta_ultima: Dict[str, Any], devolver_detalle: bool = False):
        """Compatibilidad retro. Usar get_fin_turno_conductor."""
        return self.get_fin_turno_conductor(meta_ultima, devolver_detalle)

    # ------------------------------------------------------------------
    # Evaluación de conexiones para buses
    # ------------------------------------------------------------------
    def evaluar_conexion_bus(
        self,
        tarea_origen: Dict[str, Any],
        tarea_destino: Dict[str, Any],
        indice_origen: Optional[int] = None,
        indice_destino: Optional[int] = None,
        devolver_detalle: bool = False,
    ):
        """
        Reemplaza a _evaluar_gap_bus. Determina si dos viajes comerciales pueden conectarse.
        """
        resultado = self._resolver_conexion_bus(tarea_origen, tarea_destino, indice_origen, indice_destino)
        if devolver_detalle:
            return resultado
        return resultado[0]

    def _resolver_conexion_bus(
        self,
        tarea_origen: Dict[str, Any],
        tarea_destino: Dict[str, Any],
        indice_origen: Optional[int],
        indice_destino: Optional[int],
    ) -> Tuple[bool, Dict[str, Any]]:
        detalle = {
            "indice_origen": indice_origen,
            "indice_destino": indice_destino,
            "id_viaje_origen": tarea_origen.get("id"),
            "id_viaje_destino": tarea_destino.get("id"),
            "tipo": None,
        }

        espera = tarea_destino["inicio"] - tarea_origen["fin"]
        if espera <= 0:
            detalle["motivo"] = "gap_no_positivo"
            return False, detalle
        detalle["espera"] = espera

        mismo_lugar = tarea_origen["destino"] == tarea_destino["origen"]
        detalle["mismo_lugar"] = mismo_lugar

        if mismo_lugar:
            nodo = tarea_origen["destino"]
            regla_parada = self.paradas_dict.get(nodo.upper())
            detalle.update({"nodo": nodo})
            
            # REGLA DURA: Las paradas SIEMPRE deben existir y ajustarse al rango min/max
            if regla_parada:
                parada_min = regla_parada.get("min", 0)
                parada_max = regla_parada.get("max", 1440)
                detalle.update({"parada_min": parada_min, "parada_max": parada_max})
                
                # REGLA DURA: La espera DEBE cumplir con el rango min/max
                # Si no cumple, intentar buscar alternativa vía depósito antes de rechazar
                if espera < parada_min:
                    # REGLA CRÍTICA: Si no cumple el mínimo y estamos en el mismo lugar,
                    # NO se puede crear una parada válida - rechazar la conexión
                    # El sistema debería buscar una alternativa (vía depósito) en la fase de optimización
                    detalle["motivo"] = f"espera_menor_minimo: {espera} min < {parada_min} min"
                    return False, detalle
                elif espera > parada_max:
                    detalle["motivo"] = f"espera_excede_maximo: {espera} min > {parada_max} min"
                    return False, detalle
                else:
                    # Está dentro del rango, usar el tiempo disponible
                    detalle["tipo"] = "parada"
                    detalle["tiempo_parada_ajustado"] = espera
                    detalle["requiere_ajuste"] = False
                    return True, detalle
            
            # Si no hay regla de parada, permitir la parada (sin restricción)
            detalle["tipo"] = "parada"
            return True, detalle

        # Nodos distintos: intentar vacío directo primero
        es_vacio, info_vacio = self._evaluar_vacio_interno(tarea_origen, tarea_destino)
        if es_vacio:
            info_vacio.update({"tipo": "vacio"})
            return True, {**detalle, **info_vacio}

        es_deposito, info_deposito = self._evaluar_reutilizacion_deposito(tarea_origen, tarea_destino)
        if es_deposito:
            info_deposito.update({"tipo": "deposito"})
            return True, {**detalle, **info_deposito}

        detalle["motivo"] = "sin_vacio_ni_deposito"
        return False, detalle

    def _evaluar_vacio_interno(self, viaje_origen: Dict[str, Any], viaje_destino: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Evalúa si existe un vacío interno configurado y habilitado entre el destino del viaje origen
        y el origen del viaje destino. REGLA ESTRICTA: La configuración es mandatoria, no se modifica,
        no se flexibiliza, se respeta estrictamente.
        """
        destino_origen = viaje_origen["destino"]
        origen_destino = viaje_destino["origen"]
        
        # Buscar el vacío interno configurado
        tiempo_vacio, kilometros_vacio = self.buscar_tiempo_vacio(destino_origen, origen_destino, viaje_origen["fin"])
        
        # Si no existe el vacío interno configurado, retornar False
        if tiempo_vacio is None:
            return False, {}
        
        # REGLA ESTRICTA: Verificar que esté habilitado en la configuración
        vacios = self.config.get("vacios", {})
        clave = f"{destino_origen}_{origen_destino}"
        entrada = vacios.get(clave, {})
        
        # Si está explícitamente deshabilitado, no usarlo
        if isinstance(entrada, dict) and entrada.get("habilitado", True) is False:
            return False, {}
        
        # Verificar que el tiempo de vacío configurado quepa en la ventana disponible
        tiempo_disponible = viaje_destino["inicio"] - viaje_origen["fin"]
        if tiempo_vacio > tiempo_disponible:
            return False, {}

        # Verificar que la espera restante después del vacío cumpla el rango sin retrasar salida
        regla_parada = self.paradas_dict.get(str(origen_destino).upper()) if hasattr(self, "paradas_dict") else None
        if regla_parada:
            parada_min = regla_parada.get("min", 0)
            parada_max = regla_parada.get("max", 1440)
            tiempo_restante = viaje_destino["inicio"] - (viaje_origen["fin"] + tiempo_vacio)
            if tiempo_restante < parada_min or tiempo_restante > parada_max:
                return False, {}

        # REGLA ESTRICTA: Usar EXACTAMENTE el tiempo de vacío de la configuración
        # NO ajustar, NO flexibilizar, respetar estrictamente
        return True, {
            "tiempo_vacio": tiempo_vacio,
            "kilometros_vacio": kilometros_vacio or 0,
        }

    def _evaluar_reutilizacion_deposito(self, viaje_origen: Dict[str, Any], viaje_destino: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        mejor_opcion: Optional[Dict[str, Any]] = None
        
        # REGLA CRÍTICA: Detectar vacíos cruzados
        # Un vacío cruzado ocurre cuando:
        # - El destino del viaje origen es el mismo que el origen del viaje destino
        # - Y se está usando depósito (va al depósito y vuelve al mismo lugar)
        mismo_lugar = viaje_origen["destino"] == viaje_destino["origen"]
        es_vacio_cruzado = mismo_lugar
        
        for deposito in self.depositos_config:
            nombre = deposito.nombre
            if not nombre:
                continue
            tiempo_a_dep, km_a_dep = self.buscar_tiempo_vacio(viaje_origen["destino"], nombre, viaje_origen["fin"])
            tiempo_de_dep, km_de_dep = self.buscar_tiempo_vacio(
                nombre,
                viaje_destino["origen"],
                max(viaje_destino["inicio"] - self._t_de_dep_aprox, 0),
            )
            if tiempo_a_dep is None or tiempo_de_dep is None:
                continue

            tiempo_llegada = viaje_origen["fin"] + tiempo_a_dep
            tiempo_salida = tiempo_llegada + self.tiempo_min_deposito
            parada_min_dest = 0
            if hasattr(self, "paradas_dict"):
                regla = self.paradas_dict.get(str(viaje_destino["origen"] or "").upper())
                parada_min_dest = regla.get("min", 0) if regla else 0
            llegada_destino_max = viaje_destino["inicio"] - parada_min_dest
            if tiempo_salida + tiempo_de_dep > llegada_destino_max:
                continue

            tiempo_total = tiempo_a_dep + self.tiempo_min_deposito + tiempo_de_dep
            kilometros_totales = (km_a_dep or 0) + (km_de_dep or 0)
            
            # REGLA CRÍTICA: Penalizar vacíos cruzados
            # Si es un vacío cruzado, agregar una penalización significativa a los kilómetros
            # para que esta opción solo se use si no hay alternativas
            kilometros_penalizados = kilometros_totales
            if es_vacio_cruzado:
                # Penalizar con 100 km adicionales para desincentivar vacíos cruzados
                kilometros_penalizados = kilometros_totales + 100
            
            opcion = {
                "deposito": nombre,
                "t_a_dep": tiempo_a_dep,
                "km_a_dep": km_a_dep or 0,
                "t_de_dep": tiempo_de_dep,
                "km_de_dep": km_de_dep or 0,
                "tiempo_llegada_deposito": tiempo_llegada,
                "tiempo_salida_deposito": tiempo_salida,
                "tiempo_total": tiempo_total,
                "kilometros_totales": kilometros_totales,
                "kilometros_penalizados": kilometros_penalizados,  # Para comparación
                "espera_total": viaje_destino["inicio"] - viaje_origen["fin"],
                "es_vacio_cruzado": es_vacio_cruzado,
            }
            
            # Usar kilómetros penalizados para la comparación
            if (
                mejor_opcion is None
                or kilometros_penalizados < mejor_opcion["kilometros_penalizados"]
                or (kilometros_penalizados == mejor_opcion["kilometros_penalizados"] and tiempo_total < mejor_opcion["tiempo_total"])
            ):
                mejor_opcion = opcion

        if mejor_opcion:
            return True, mejor_opcion
        return False, {}

    # ------------------------------------------------------------------
    # Evaluación de conexiones para conductores
    # ------------------------------------------------------------------
    def evaluar_conexion_conductor(
        self,
        meta_A: Dict[str, Any],
        meta_B: Dict[str, Any],
        debug: bool = False,
        devolver_detalle: bool = False,
    ):
        """
        Reemplaza a _es_conexion_factible_conductor. Devuelve la factibilidad, el tipo
        de conexión (parada/desplazamiento) y el costo temporal en minutos.
        """
        resultado = self._resolver_conexion_conductor(meta_A, meta_B, debug)
        if devolver_detalle:
            return resultado
        return resultado[0], resultado[1], resultado[2]

    @lru_cache(maxsize=5000)
    def puede_hacer_relevo_en_nodo(self, nodo: str) -> Tuple[bool, Optional[int]]:
        """
        Determina si es posible realizar un relevo en 'nodo' de forma DINÁMICA.
        
        REGLA: Un nodo es punto de relevo si está conectado con desplazamientos habilitados
        a algún depósito configurado (para aplicar FnS o InS). Funciona para cualquier depósito
        configurado en la instancia, no solo uno fijo.
        
        - Si el nodo es un depósito configurado → válido (tiempo 0).
        - Si existe al menos un depósito configurado con desplazamiento nodo→depósito Y
          depósito→nodo habilitados → el nodo es punto de relevo (tiempo = max de ambos).
        - Opcional: si config tiene 'puntos_relevo' no vacío, solo se consideran nodos
          que coincidan con esa lista (además de tener desplazamiento).
        
        Returns:
            (puede_relevo, tiempo_maximo): Tupla con booleano y tiempo máximo del desplazamiento
        """
        if not nodo:
            return False, None
        
        nodo_norm = str(nodo).strip().upper()

        # Filtro opcional por lista de config: si existe y no está vacía, el nodo debe estar en ella
        if getattr(self, "puntos_relevo", None) and len(self.puntos_relevo) > 0:
            coincide = any(
                self._nodos_coinciden_para_desplaz(nodo_norm, str(p or "").strip().upper())
                for p in self.puntos_relevo
            )
            if not coincide:
                return False, None

        # Depósitos configurados (dinámico: cualquier depósito en la configuración)
        nombres_depositos = self._nombres_depositos()
        if not nombres_depositos:
            nombres_depositos = [self.deposito_base]

        mejor_tiempo_maximo: Optional[int] = None

        for deposito in nombres_depositos:
            deposito_norm = str(deposito).strip().upper()

            if nodo_norm == deposito_norm:
                return True, 0

            hab_ida, tiempo_ida = self.buscar_info_desplazamiento(deposito, nodo, 0)
            hab_vuelta, tiempo_vuelta = self.buscar_info_desplazamiento(nodo, deposito, 0)

            if hab_ida and hab_vuelta and tiempo_ida is not None and tiempo_vuelta is not None:
                tiempo_maximo = max(tiempo_ida, tiempo_vuelta)
                if mejor_tiempo_maximo is None or tiempo_maximo < mejor_tiempo_maximo:
                    mejor_tiempo_maximo = tiempo_maximo

        if mejor_tiempo_maximo is not None:
            return True, mejor_tiempo_maximo

        return False, None

    def _resolver_conexion_conductor(
        self,
        meta_A: Dict[str, Any],
        meta_B: Dict[str, Any],
        debug: bool,
    ) -> Tuple[bool, Optional[str], int, Dict[str, Any]]:
        """
        Resuelve si un conductor puede ir de Tarea A a Tarea B.
        
        REGLAS DURAS:
        - Las conexiones inter-bloque (cambio de bus) solo se permiten si hay 
          desplazamientos habilitados. Los relevos solo pueden ocurrir en puntos 
          donde hay desplazamientos habilitados en ambas direcciones.
        - Las reglas de parada (min/max) DEBEN cumplirse estrictamente para conexiones 
          intra-bloque (mismo bus, mismo conductor).
        - IMPORTANTE: Durante relevos (cambio de conductor), el bus puede tener paradas 
          SIN CONDUCTOR. En estos casos, las reglas de parada NO aplican porque el bus 
          está esperando sin conductor.
        """
        viaje_A = meta_A["viaje"]
        viaje_B = meta_B["viaje"]
        detalle = {
            "origen": viaje_A["destino"],
            "destino": viaje_B["origen"],
            "id_viaje_origen": viaje_A.get("id"),
            "id_viaje_destino": viaje_B.get("id"),
        }

        if viaje_A["fin"] >= viaje_B["inicio"]:
            if debug:
                print(
                    f"  [X] Conexion imposible (tiempo): Viaje {viaje_A['id']}->{viaje_B['id']} | "
                    f"{formatear_hora(viaje_A['fin'])} >= {formatear_hora(viaje_B['inicio'])}"
                )
            detalle["motivo"] = "solapamiento_temporal"
            return False, None, 0, detalle

        espera = viaje_B["inicio"] - viaje_A["fin"]
        detalle["espera"] = espera

        # CASO 1: Conexión Intra-Bloque (mismo bus, secuencial)
        id_b = viaje_B.get("id") or viaje_B.get("_tmp_id")
        if meta_A["id_bus"] == meta_B["id_bus"] and meta_A.get("id_siguiente") == id_b:
            if debug:
                print(
                    f"  [OK] CONEXION INTRA-BLOQUE: Viaje {viaje_A['id']}->{viaje_B['id']} | "
                    f"Bus {meta_A['id_bus']} | Espera {espera} min"
                )
            detalle.update({"modo": "intra_bloque", "nodo": viaje_A["destino"]})
            return True, "parada", espera, detalle

        # CASO 2: Mismo nodo (parada) - solo válido si es intra-bloque o hay desplazamiento
        if viaje_A["destino"] == viaje_B["origen"]:
            # Si es inter-bloque (cambio de bus), es un RELEVO
            if meta_A["id_bus"] != meta_B["id_bus"]:
                # REGLA DURA: Inter-bloque en mismo nodo requiere desplazamiento habilitado
                puede_relevo, tiempo_relevo = self.puede_hacer_relevo_en_nodo(viaje_A["destino"])
                if not puede_relevo:
                    if debug:
                        print(
                            f"  [X] RELEVO NO HABILITADO: Viaje {viaje_A['id']}->{viaje_B['id']} | "
                            f"Nodo {viaje_A['destino']} no tiene desplazamientos habilitados para relevo"
                        )
                    detalle["motivo"] = "relevo_no_habilitado"
                    return False, None, 0, detalle
                
                # IMPORTANTE: Durante un relevo, el bus puede tener una parada SIN CONDUCTOR
                # Las reglas de parada NO aplican porque el bus espera sin conductor
                detalle.update({"modo": "parada_con_relevo", "nodo": viaje_A["destino"], "tiempo_relevo": tiempo_relevo})
                return True, "parada", espera, detalle
            
            # Intra-bloque en mismo nodo: validar reglas de parada (REGLAS DURAS)
            # Solo aplica cuando el mismo conductor está en el bus (no hay relevo)
            regla = self.paradas_dict.get(viaje_A["destino"].upper())
            detalle.update({"modo": "parada", "nodo": viaje_A["destino"]})
            
            # REGLA DURA: Las paradas SIEMPRE deben existir y ajustarse al rango min/max
            if regla:
                parada_min = regla.get("min", 0)
                parada_max = regla.get("max", 1440)
                
                # IMPORTANTE: El evento de parada SIEMPRE debe existir
                # Si está fuera del rango, se ajustará durante la construcción de eventos
                # La conexión se permite siempre, pero el tiempo de parada se ajustará al rango
                
                # Verificar que hay tiempo suficiente para la parada mínima
                if espera < parada_min:
                    # No hay tiempo suficiente para cumplir el mínimo - rechazar conexión
                    if debug:
                        print(
                            f"  [X] PARADA RECHAZADA (Tiempo insuficiente): Viaje {viaje_A['id']}->{viaje_B['id']} | "
                            f"Nodo: {viaje_A['destino']} | Espera: {espera} min < MIN: {parada_min} min"
                        )
                    detalle["motivo"] = "parada_tiempo_insuficiente"
                    return False, None, 0, detalle
                
                # Calcular tiempo de parada ajustado (dentro del rango)
                if espera > parada_max:
                    # Ajustar al máximo - el evento de parada tendrá duración máxima
                    tiempo_parada = parada_max
                    detalle["requiere_ajuste"] = True
                    detalle["ajuste_tipo"] = "maximo"
                else:
                    # Está dentro del rango
                    tiempo_parada = espera
                    detalle["requiere_ajuste"] = False
                
                detalle["tiempo_parada"] = tiempo_parada
                detalle["parada_min"] = parada_min
                detalle["parada_max"] = parada_max
                
                return True, "parada", tiempo_parada, detalle
            
            # Parada válida (sin regla - usar tiempo disponible)
            return True, "parada", espera, detalle

        # CASO 3: Diferentes nodos - requiere desplazamiento habilitado
        # REGLA DURA: Si es inter-bloque (cambio de bus), solo se permite con desplazamiento
        es_inter_bloque = meta_A["id_bus"] != meta_B["id_bus"]
        
        # Intentar desplazamiento directo
        # Pasar el tiempo de fin del viaje A para seleccionar la franja correcta
        hab, tiempo_despl = self.buscar_info_desplazamiento(viaje_A["destino"], viaje_B["origen"], viaje_A["fin"])
        if hab and tiempo_despl is not None and viaje_A["fin"] + tiempo_despl <= viaje_B["inicio"]:
            if debug:
                print(
                    f"  [OK] DESPLAZAMIENTO CONDUCTOR: Viaje {viaje_A['id']}->{viaje_B['id']} | "
                    f"{viaje_A['destino']}->{viaje_B['origen']} | Tiempo: {tiempo_despl} min"
                )
            detalle.update({"modo": "desplazamiento_directo", "tiempo": tiempo_despl})
            return True, "desplazamiento", tiempo_despl, detalle

        # Intentar desplazamiento vía depósito
        for deposito in self._nombres_depositos():
            # Pasar tiempos de referencia para seleccionar las franjas correctas
            tiempo_ida_estimado = viaje_A["fin"]
            tiempo_vuelta_estimado = viaje_A["fin"] + 30  # Aproximación: 30 min después del fin del viaje A
            ida_hab, tiempo_ida = self.buscar_info_desplazamiento(viaje_A["destino"], deposito, tiempo_ida_estimado)
            vuelta_hab, tiempo_vuelta = self.buscar_info_desplazamiento(deposito, viaje_B["origen"], tiempo_vuelta_estimado)
            if not (ida_hab and vuelta_hab):
                continue
            if tiempo_ida is None or tiempo_vuelta is None:
                continue
            tiempo_total = tiempo_ida + tiempo_vuelta
            if viaje_A["fin"] + tiempo_total <= viaje_B["inicio"]:
                if debug:
                    print(
                        f"  [OK] DESPLAZAMIENTO VIA DEPOSITO ({deposito}): Viaje {viaje_A['id']}->{viaje_B['id']} | "
                        f"Tiempo total: {tiempo_total} min"
                    )
                detalle.update(
                    {
                        "modo": "deposito",
                        "deposito": deposito,
                        "tiempo_ida": tiempo_ida,
                        "tiempo_vuelta": tiempo_vuelta,
                        "tiempo_total": tiempo_total,
                    }
                )
                return True, "desplazamiento", tiempo_total, detalle

        # REGLA DURA: Si es inter-bloque y no hay desplazamiento, rechazar
        if es_inter_bloque:
            if debug:
                print(
                    f"  [X] INTER-BLOQUE SIN DESPLAZAMIENTO: Viaje {viaje_A['id']}->{viaje_B['id']} | "
                    f"Bus {meta_A['id_bus']}->{meta_B['id_bus']} | No hay desplazamiento habilitado"
                )
            detalle["motivo"] = "inter_bloque_sin_desplazamiento"
            return False, None, 0, detalle

        # Si es intra-bloque pero no hay conexión válida, también rechazar
        if debug:
            print(
                f"  [X] SIN DESPLAZAMIENTO FACTIBLE: Viaje {viaje_A['id']}->{viaje_B['id']} | "
                f"{viaje_A['destino']}->{viaje_B['origen']}"
            )
        detalle["motivo"] = "sin_camino"
        return False, None, 0, detalle

