"""
Servicio para manejo centralizado de configuración del sistema.
Encapsula toda la lógica relacionada con configuracion.json.
"""

import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from werkzeug.datastructures import ImmutableMultiDict

from ..utils.logging_utils import logger
from diagramador_optimizado.io.config_validator import autocompletar_configuracion, validar_configuracion, ConfigValidationError

TIPOS_BUS_DISPONIBLES: List[str] = ["A", "B", "BE", "BPAL", "C"]


def _to_float(valor: Any, default: Optional[float] = None) -> Optional[float]:
    """Convierte un valor a float de forma segura."""
    try:
        if valor is None:
            return default
        if isinstance(valor, (int, float)):
            return float(valor)
        texto = str(valor).strip()
        if not texto:
            return default
        return float(texto.replace(",", "."))
    except (TypeError, ValueError):
        return default


def _to_int(valor: Any, default: int = 0) -> int:
    """Convierte un valor a int de forma segura."""
    try:
        if valor is None:
            return default
        if isinstance(valor, (int, float)):
            return int(float(valor))
        texto = str(valor).strip()
        if not texto:
            return default
        return int(float(texto))
    except (TypeError, ValueError):
        return default


class ConfigService:
    """
    Servicio centralizado para manejo de configuración.
    Proporciona métodos para cargar, guardar y actualizar configuración.
    """
    
    def __init__(self, config_path: str = "configuracion.json"):
        """
        Inicializa el servicio de configuración.
        
        Args:
            config_path: Ruta al archivo de configuración JSON (se normaliza a absoluta)
        """
        self.config_path = os.path.abspath(config_path)
        self._config_data: Dict[str, Any] = {}
        self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """
        Carga la configuración desde el archivo JSON.
        
        Returns:
            Diccionario con la configuración cargada
        """
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, "r", encoding="utf-8") as f:
                    self._config_data = json.load(f)
                logger.info(f"Configuración cargada desde {self.config_path}")
            else:
                logger.warning(f"Archivo de configuración no encontrado: {self.config_path}. Usando valores por defecto.")
                self._config_data = self._get_default_config()
            
            # Asegurar que existen las secciones principales
            self._config_data.setdefault("tipos_bus", {})
            self._config_data.setdefault("lineas", {})
            self._config_data.setdefault("depositos", [])
            self._config_data.setdefault("flota_por_tipo", {})
            self._config_data.setdefault("tipos_conductor", [])
            self._config_data.setdefault("puntos_relevo", [])
            
            # Sincronizar: incluir en nodos cualquier origen/destino de vacíos/desplazamientos/paradas
            self._sync_nodos_from_connections()
            
            return self._config_data.copy()
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            self._config_data = self._get_default_config()
            return self._config_data.copy()
    
    def get_config(self) -> Dict[str, Any]:
        """
        Obtiene una copia de la configuración actual.
        
        Returns:
            Copia del diccionario de configuración
        """
        return self._config_data.copy()
    
    def save_config(self, verify: bool = True) -> bool:
        """
        Guarda la configuración al archivo JSON con verificación opcional.
        
        Args:
            verify: Si es True, verifica que el archivo se guardó correctamente
            
        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        try:
            # Crear carpeta si no existe (ej. diagramador_optimizado/)
            dir_path = os.path.dirname(self.config_path)
            if dir_path and not os.path.isdir(dir_path):
                os.makedirs(dir_path, exist_ok=True)
            # Guardar archivo
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self._config_data, f, indent=2, ensure_ascii=False)
                f.flush()
                if hasattr(f, 'fileno'):
                    try:
                        os.fsync(f.fileno())
                    except:
                        pass
            
            # Pausa para asegurar escritura completa
            time.sleep(0.3)
            
            # Verificación opcional
            if verify:
                return self._verify_saved_config()
            
            return True
        except Exception as e:
            logger.error(f"Error guardando configuración: {e}")
            return False
    
    def _verify_saved_config(self, max_attempts: int = 5) -> bool:
        """
        Verifica que la configuración se guardó correctamente.
        
        Args:
            max_attempts: Número máximo de intentos de verificación
            
        Returns:
            True si se verificó correctamente, False en caso contrario
        """
        for intento in range(max_attempts):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config_verificada = json.load(f)
                
                # Verificar campos críticos
                if (config_verificada.get('deposito') == self._config_data.get('deposito') and
                    config_verificada.get('limite_jornada') == self._config_data.get('limite_jornada') and
                    set(config_verificada.get('nodos', [])) == set(self._config_data.get('nodos', []))):
                    logger.info(f"Configuración verificada correctamente (intento {intento + 1})")
                    return True
                else:
                    logger.warning(f"Intento {intento + 1}: Configuración no coincide, reintentando...")
                    time.sleep(0.1)
            except Exception as e:
                logger.warning(f"Intento {intento + 1} de verificación falló: {e}")
                time.sleep(0.1)
        
        logger.error("No se pudo verificar que la configuración se guardó correctamente")
        return False
    
    def update_from_form(self, form_data: ImmutableMultiDict) -> Dict[str, Any]:
        """
        Actualiza la configuración desde datos de formulario web.
        
        Args:
            form_data: Datos del formulario Flask
            
        Returns:
            Diccionario con resultado de la operación
        """
        try:
            # Cargar configuración actual
            self.load_config()
            
            # Actualizar parámetros básicos
            self._config_data["limite_jornada"] = int(form_data.get("limite_jornada", 720))
            self._config_data["tiempo_toma"] = int(form_data.get("tiempo_toma", 15))
            self._config_data["max_inicio_jornada_conductor"] = (
                form_data.get("max_inicio_jornada_conductor", self._config_data.get("max_inicio_jornada_conductor", "23:59")) or "23:59"
            ).strip()
            # El límite de flota se maneja por depósito (depositos[].max_buses), no a nivel raíz.
            self._config_data.pop("max_buses", None)
            self._config_data["fase_3_union_conductores"] = {
                "union_solo_por_deposito": form_data.get("f3_union_solo_por_deposito") == "on",
                "parada_larga_umbral_union": int(form_data.get("f3_parada_larga_umbral_union", (self._config_data.get("fase_3_union_conductores") or {}).get("parada_larga_umbral_union", 60))),
                "parada_larga_excepcion_depot_min": int(form_data.get("f3_parada_larga_excepcion_depot_min", (self._config_data.get("fase_3_union_conductores") or {}).get("parada_larga_excepcion_depot_min", 120))),
                "max_rondas_union": int(form_data.get("f3_max_rondas_union", (self._config_data.get("fase_3_union_conductores") or {}).get("max_rondas_union", 1500))),
                "timeout_ortools_segundos": int(form_data.get("f3_timeout_ortools_segundos", (self._config_data.get("fase_3_union_conductores") or {}).get("timeout_ortools_segundos", 180))),
            }
            
            # Actualizar nodos (aceptar coma, punto y coma, salto de línea, tabulador)
            nodos_str = form_data.get("nodos", "")
            self._config_data["nodos"] = self._parse_nodos_from_text(nodos_str)
            
            # Procesar depósitos
            self._process_depositos(form_data)
            
            # Procesar tipos de bus
            self._process_tipos_bus(form_data)
            
            # Procesar líneas
            self._process_lineas(form_data)
            
            # Procesar paradas
            self._process_paradas(form_data)
            
            # Procesar vacíos
            self._process_vacios(form_data)
            
            # Procesar desplazamientos
            self._process_desplazamientos(form_data)
            
            # Procesar grupos de líneas para interlineados
            self._process_grupos_lineas(form_data)
            
            # Procesar puntos de relevo
            self._process_puntos_relevo(form_data)
            
            # Procesar tipos de conductor
            self._process_tipos_conductor(form_data)

            # Autocompletar estructura para escenarios nuevos y validar antes de guardar.
            self._config_data = autocompletar_configuracion(self._config_data)
            try:
                validar_configuracion(self._config_data)
            except ConfigValidationError as e:
                return {"success": False, "message": f"Configuración inválida: {str(e)}"}
            
            # Guardar configuración
            if self.save_config():
                logger.info("Configuración guardada exitosamente desde formulario")
                return {"success": True, "message": "Configuración guardada exitosamente"}
            else:
                return {"success": False, "message": "Error al verificar que la configuración se guardó"}
                
        except Exception as e:
            logger.error(f"Error actualizando configuración desde formulario: {e}")
            return {"success": False, "message": f"Error guardando configuración: {str(e)}"}
    
    def _process_depositos(self, form_data: ImmutableMultiDict) -> None:
        """Procesa depósitos desde el formulario.
        
        Soporta dos formatos:
        1. Formato simple: deposito_nombre_0, deposito_max_buses_0, etc.
        2. Formato anidado: depositos[0][nombre], depositos[0][max_buses], etc.
        """
        depositos_configurados: List[Dict[str, Any]] = []
        
        # Intentar primero el formato simple (deposito_nombre_0)
        try:
            num_depositos = int(form_data.get("num_depositos", 0))
        except (ValueError, TypeError):
            num_depositos = 0
            logger.warning("Error al leer num_depositos del formulario, intentando detectar depósitos manualmente")
        
        # Si num_depositos es 0, intentar detectar depósitos contando los campos deposito_nombre_*
        if num_depositos == 0:
            indices_detectados = set()
            for key in form_data.keys():
                if key.startswith("deposito_nombre_"):
                    try:
                        idx = int(key.replace("deposito_nombre_", ""))
                        indices_detectados.add(idx)
                    except ValueError:
                        pass
            if indices_detectados:
                num_depositos = max(indices_detectados) + 1
                logger.info(f"Depósitos detectados manualmente: {num_depositos} (índices: {sorted(indices_detectados)})")
        
        if num_depositos > 0:
            # Formato simple: deposito_nombre_0, deposito_max_buses_0
            for i in range(num_depositos):
                nombre = form_data.get(f"deposito_nombre_{i}", "").strip()
                max_buses_str = form_data.get(f"deposito_max_buses_{i}", "200")
                
                if nombre:  # Solo agregar si tiene nombre
                    try:
                        max_buses = int(max_buses_str) if max_buses_str else 200
                    except:
                        max_buses = 200
                    
                    depositos_configurados.append({
                        "nombre": nombre,
                        "max_buses": max_buses,
                        "permite_recarga": False,  # Por defecto
                        "posiciones_recarga": 0,
                        "flota_por_tipo": {tipo: 0 for tipo in TIPOS_BUS_DISPONIBLES},
                    })
        else:
            # Intentar formato anidado (depositos[0][nombre])
            patron_deposito = re.compile(r"depositos\[(\d+)\]\[(.+?)\]")
            depositos_raw: Dict[int, Dict[str, Any]] = {}
            
            for key, value in form_data.items():
                coincidencia = patron_deposito.match(key)
                if coincidencia:
                    idx = int(coincidencia.group(1))
                    campo = coincidencia.group(2)
                    depositos_raw.setdefault(idx, {})[campo] = value
            
            for idx in sorted(depositos_raw.keys()):
                datos_dep = depositos_raw[idx]
                nombre_dep = (datos_dep.get("nombre") or "").strip()
                if not nombre_dep:
                    continue
                
                flota_dep = {
                    tipo: _to_int(datos_dep.get(f"flota_{tipo}"), 0)
                    for tipo in TIPOS_BUS_DISPONIBLES
                }
                
                deposito_dict = {
                    "nombre": nombre_dep,
                    "max_buses": _to_int(datos_dep.get("max_buses"), 200),
                    "permite_recarga": datos_dep.get("permite_recarga") is not None,
                    "posiciones_recarga": _to_int(datos_dep.get("posiciones_recarga"), 0),
                    "flota_por_tipo": flota_dep,
                }
                depositos_configurados.append(deposito_dict)
        
        if not depositos_configurados:
            depositos_configurados.append({
                "nombre": form_data.get("deposito") or self._config_data.get("deposito") or "Deposito Pie Andino",
                "max_buses": 200,
                "permite_recarga": False,
                "posiciones_recarga": 0,
                "flota_por_tipo": {tipo: 0 for tipo in TIPOS_BUS_DISPONIBLES},
            })
        
        logger.info(f"Depósitos procesados: {len(depositos_configurados)}")
        for i, dep in enumerate(depositos_configurados):
            logger.info(f"  Depósito {i+1}: {dep.get('nombre')} (max_buses: {dep.get('max_buses')})")
        
        self._config_data["depositos"] = depositos_configurados
        self._config_data["deposito"] = form_data.get("deposito") or depositos_configurados[0]["nombre"]
        
        # Calcular flota global
        flota_global: Dict[str, int] = {}
        for deposito_cfg in depositos_configurados:
            flota_dep = deposito_cfg.get("flota_por_tipo") or {}
            for tipo, cantidad in flota_dep.items():
                flota_global[tipo] = flota_global.get(tipo, 0) + _to_int(cantidad, 0)
        for tipo in TIPOS_BUS_DISPONIBLES:
            flota_global.setdefault(tipo, 0)
        self._config_data["flota_por_tipo"] = flota_global
        
        # IMPORTANTE: Regenerar conexiones después de actualizar depósitos
        # Esto incluye conexiones entre depósitos para permitir que los buses compartan recursos
        self.regenerate_connections()
    
    def _process_tipos_bus(self, form_data: ImmutableMultiDict) -> None:
        """Procesa tipos de bus desde el formulario."""
        tipos_config = {}
        for tipo in TIPOS_BUS_DISPONIBLES:
            habilitado = form_data.get(f"tipo_{tipo}_habilitado") == "on"
            descripcion = form_data.get(f"tipo_{tipo}_descripcion", "").strip()
            autonomia = _to_float(form_data.get(f"tipo_{tipo}_autonomia"))
            capacidad = _to_int(form_data.get(f"tipo_{tipo}_capacidad"), 0)
            
            if not habilitado and tipo != "BE":
                continue
            
            entrada_tipo = {}
            if descripcion:
                entrada_tipo["descripcion"] = descripcion
            if autonomia is not None:
                entrada_tipo["autonomia_km"] = autonomia
            if capacidad:
                entrada_tipo["capacidad_pasajeros"] = capacidad
            
            if tipo == "BE":
                parametros = {
                    "carga_inicial": _to_float(form_data.get("tipo_BE_carga_inicial"), 95) or 95,
                    "consumo_por_km": _to_float(form_data.get("tipo_BE_consumo_por_km"), 0.5) or 0.5,
                    "limite_operacion": _to_float(form_data.get("tipo_BE_limite_operacion"), 30) or 30,
                    "min_entrada_recarga": _to_float(form_data.get("tipo_BE_min_entrada"), 60) or 60,
                    "max_entrada_recarga": _to_float(form_data.get("tipo_BE_max_entrada"), 80) or 80,
                    "tasa_recarga_por_minuto": _to_float(form_data.get("tipo_BE_tasa_recarga"), 1.25) or 1.25,
                    "ventana_recarga": {
                        "inicio": form_data.get("tipo_BE_ventana_inicio", "09:00"),
                        "fin": form_data.get("tipo_BE_ventana_fin", "18:00"),
                    },
                }
                entrada_tipo["parametros_electricos"] = parametros
            
            tipos_config[tipo] = entrada_tipo
        
        if not tipos_config:
            tipos_config = {tipo: {} for tipo in TIPOS_BUS_DISPONIBLES}
        
        self._config_data["tipos_bus"] = tipos_config
    
    def _process_lineas(self, form_data: ImmutableMultiDict) -> None:
        """Procesa líneas desde el formulario."""
        lineas_catalogo = form_data.get("lineas_catalogo", "")
        lineas_nombres = [linea.strip() for linea in lineas_catalogo.split(",") if linea.strip()]
        lineas_config = {}
        tipos_config = self._config_data.get("tipos_bus", {})
        
        for idx, linea in enumerate(lineas_nombres):
            tipos_permitidos = [
                tipo
                for tipo in TIPOS_BUS_DISPONIBLES
                if form_data.get(f"linea_{idx}_tipo_{tipo}") == "on"
            ]
            if not tipos_permitidos:
                tipos_permitidos = list(tipos_config.keys())
            lineas_config[linea] = {"tipos_permitidos": tipos_permitidos}
        
        self._config_data["lineas"] = lineas_config
    
    def _process_paradas(self, form_data: ImmutableMultiDict) -> None:
        """Procesa paradas desde el formulario."""
        paradas = {}
        for nodo in self._config_data.get("nodos", []):
            paradas[nodo] = {
                "min": int(form_data.get(f"parada_{nodo}_min", 5)),
                "max": int(form_data.get(f"parada_{nodo}_max", 120)),
            }
        self._config_data["paradas"] = paradas
    
    def _process_vacios(self, form_data: ImmutableMultiDict) -> None:
        """Procesa vacíos desde el formulario."""
        vacios = {}
        conexiones = self.get_all_connections()
        
        for conexion in conexiones:
            habilitado = form_data.get(f"vacios_{conexion}_habilitado") == "on"
            vacios[conexion] = {"habilitado": habilitado, "franjas": []}
            
            franja_index = 0
            while True:
                inicio_key = f"vacios_{conexion}_franja_{franja_index}_inicio"
                if inicio_key not in form_data:
                    break
                
                inicio = form_data.get(inicio_key)
                fin = form_data.get(f"vacios_{conexion}_franja_{franja_index}_fin")
                tiempo = form_data.get(f"vacios_{conexion}_franja_{franja_index}_tiempo")
                km = form_data.get(f"vacios_{conexion}_franja_{franja_index}_km")
                
                if inicio and fin and tiempo is not None and km is not None:
                    vacios[conexion]["franjas"].append({
                        "inicio": inicio,
                        "fin": fin,
                        "tiempo": float(tiempo),
                        "km": float(km),
                    })
                franja_index += 1
        
        self._config_data["vacios"] = vacios
    
    def _process_desplazamientos(self, form_data: ImmutableMultiDict) -> None:
        """Procesa desplazamientos desde el formulario."""
        desplazamientos = {}
        conexiones = self.get_all_connections()
        
        for conexion in conexiones:
            habilitado = form_data.get(f"desplaz_{conexion}_habilitado") == "on"
            tiempo = form_data.get(f"desplaz_{conexion}_tiempo", 0)
            desplazamientos[conexion] = {
                "habilitado": habilitado,
                "tiempo": float(tiempo),
            }
        
        self._config_data["desplazamientos"] = desplazamientos
    
    def _process_grupos_lineas(self, form_data: ImmutableMultiDict) -> None:
        """Procesa grupos de líneas para interlineados desde el formulario."""
        # Obtener configuración de interlineado global
        interlineado_global = form_data.get("interlineado_global") == "on"
        self._config_data["interlineado_global"] = interlineado_global
        
        # Si interlineado global está habilitado, no necesitamos grupos
        if interlineado_global:
            self._config_data["grupos_lineas"] = {}
            self._config_data["limite_jornada_por_grupo_linea"] = {}
            self._config_data["respetar_grupos_lineas"] = False
            return
        
        # Procesar grupos de líneas
        grupos_lineas: Dict[str, List[str]] = {}
        
        # Buscar todos los grupos configurados
        # Formato esperado: grupos_lineas[grupo_index][linea_index] = nombre_linea
        patron_grupo = re.compile(r"grupos_lineas\[(\d+)\]\[(\d+)\]")
        grupos_raw: Dict[int, Dict[int, str]] = {}
        
        for key, value in form_data.items():
            coincidencia = patron_grupo.match(key)
            if coincidencia:
                grupo_idx = int(coincidencia.group(1))
                linea_idx = int(coincidencia.group(2))
                grupos_raw.setdefault(grupo_idx, {})[linea_idx] = value.strip()
        
        # También buscar nombres de grupos
        patron_nombre_grupo = re.compile(r"grupo_nombre\[(\d+)\]")
        nombres_grupos: Dict[int, str] = {}
        limites_grupos_raw: Dict[int, int] = {}
        for key, value in form_data.items():
            coincidencia = patron_nombre_grupo.match(key)
            if coincidencia:
                grupo_idx = int(coincidencia.group(1))
                nombre_grupo = value.strip()
                if nombre_grupo:
                    nombres_grupos[grupo_idx] = nombre_grupo
        patron_limite_grupo = re.compile(r"grupo_limite_jornada\[(\d+)\]")
        for key, value in form_data.items():
            coincidencia = patron_limite_grupo.match(key)
            if coincidencia:
                grupo_idx = int(coincidencia.group(1))
                try:
                    limites_grupos_raw[grupo_idx] = int(float(value))
                except Exception:
                    continue
        
        # Construir diccionario de grupos
        limites_por_grupo: Dict[str, int] = {}
        limite_global = int(self._config_data.get("limite_jornada", 720) or 720)
        for grupo_idx in sorted(grupos_raw.keys()):
            lineas_grupo = [
                linea for linea in sorted(grupos_raw[grupo_idx].values())
                if linea
            ]
            if lineas_grupo:
                nombre_grupo = nombres_grupos.get(grupo_idx, f"Grupo_{grupo_idx}")
                grupos_lineas[nombre_grupo] = lineas_grupo
                limites_por_grupo[nombre_grupo] = limites_grupos_raw.get(grupo_idx, limite_global)
        
        self._config_data["grupos_lineas"] = grupos_lineas
        self._config_data["limite_jornada_por_grupo_linea"] = limites_por_grupo
        # Cuando hay grupos definidos, forzar respeto estricto de grupo en fases operativas.
        self._config_data["respetar_grupos_lineas"] = bool(grupos_lineas)

    def _process_puntos_relevo(self, form_data: ImmutableMultiDict) -> None:
        """Procesa puntos de relevo desde el formulario (checkboxes puntos_relevo_NODO)."""
        puntos: List[str] = []
        for key in form_data.keys():
            if str(key).startswith("puntos_relevo_") and key != "puntos_relevo_":
                nodo = str(key).replace("puntos_relevo_", "", 1).strip()
                if nodo:
                    puntos.append(nodo)
        self._config_data["puntos_relevo"] = sorted(list(set(puntos)))

    def _process_tipos_conductor(self, form_data: ImmutableMultiDict) -> None:
        """Procesa tipos de conductor desde el formulario (rangos de ingreso y fin de jornada)."""
        patron = re.compile(r"tipo_conductor_(\d+)_nombre")
        indices = set()
        for key in form_data.keys():
            m = patron.match(key)
            if m:
                indices.add(int(m.group(1)))
        tipos: List[Dict[str, Any]] = []
        for idx in sorted(indices):
            nombre = (form_data.get(f"tipo_conductor_{idx}_nombre") or "").strip()
            if not nombre:
                continue
            ing_min = (form_data.get(f"tipo_conductor_{idx}_ingreso_min") or "00:00").strip()
            ing_max = (form_data.get(f"tipo_conductor_{idx}_ingreso_max") or "23:59").strip()
            fin_min = (form_data.get(f"tipo_conductor_{idx}_fin_min") or "00:00").strip()
            fin_max = (form_data.get(f"tipo_conductor_{idx}_fin_max") or "23:59").strip()
            tipo_id = (form_data.get(f"tipo_conductor_{idx}_id") or nombre).strip() or nombre
            tipos.append({
                "id": tipo_id,
                "nombre": nombre,
                "rango_ingreso": {"min": ing_min, "max": ing_max},
                "rango_fin_jornada": {"min": fin_min, "max": fin_max},
            })
        self._config_data["tipos_conductor"] = tipos if tipos else self._config_data.get("tipos_conductor") or []
    
    def get_active_deposits(self) -> List[str]:
        """
        Obtiene la lista de depósitos activos.
        
        Returns:
            Lista de nombres de depósitos
        """
        depositos_config = self._config_data.get("depositos") or []
        nombres = [
            dep.get("nombre")
            for dep in depositos_config
            if isinstance(dep, dict) and dep.get("nombre")
        ]
        
        deposito_principal = self._config_data.get("deposito")
        if deposito_principal:
            nombres.append(deposito_principal)
        
        nombres_limpios = sorted({
            nombre.strip() for nombre in nombres
            if nombre and str(nombre).strip()
        })
        
        if not nombres_limpios:
            nombres_limpios = ["Deposito Pie Andino"]
        
        return nombres_limpios
    
    def get_all_connections(self) -> List[str]:
        """
        Obtiene todas las conexiones posibles dinámicamente.
        
        Returns:
            Lista ordenada de conexiones (formato: "origen_destino")
        """
        nodos = self._config_data.get("nodos", [])
        depositos_activos = self.get_active_deposits()
        
        conexiones: Set[str] = set()
        
        # Conexiones desde/hacia cada depósito
        for deposito in depositos_activos:
            for nodo in nodos:
                if nodo and nodo != deposito:
                    conexiones.add(f"{deposito}_{nodo}")
                    conexiones.add(f"{nodo}_{deposito}")
        
        # Conexiones entre nodos
        for i, nodo1 in enumerate(nodos):
            for j, nodo2 in enumerate(nodos):
                if i != j and nodo1 and nodo2 and nodo1 != nodo2:
                    conexiones.add(f"{nodo1}_{nodo2}")
        
        # Conexiones entre depósitos
        for i, dep_origen in enumerate(depositos_activos):
            for j, dep_destino in enumerate(depositos_activos):
                if i != j and dep_origen != dep_destino:
                    conexiones.add(f"{dep_origen}_{dep_destino}")
        
        return sorted(conexiones)
    
    def regenerate_connections(self) -> None:
        """
        Regenera todas las conexiones dinámicamente basándose en nodos y depósitos actuales.
        Preserva configuración existente y agrega nuevas conexiones si faltan.
        IMPORTANTE: Incluye conexiones entre depósitos para permitir que los buses compartan recursos.
        """
        nodos = self._config_data.get("nodos", [])
        depositos_activos = self.get_active_deposits()
        
        logger.info(f"Regenerando conexiones dinámicamente:")
        logger.info(f"  - Depósitos: {depositos_activos}")
        logger.info(f"  - Nodos: {nodos}")
        
        # Inicializar estructuras si no existen
        self._config_data.setdefault("vacios", {})
        self._config_data.setdefault("desplazamientos", {})
        self._config_data.setdefault("paradas", {})
        
        todas_las_conexiones = self.get_all_connections()
        
        # Preservar configuración existente, crear nuevas si faltan
        vacios_nuevos = {}
        desplazamientos_nuevos = {}
        
        for conexion in todas_las_conexiones:
            # Determinar si es una conexión entre depósitos
            partes = conexion.split("_", 1)
            es_entre_depositos = False
            if len(partes) == 2:
                origen, destino = partes
                es_entre_depositos = (origen in depositos_activos and destino in depositos_activos)
            
            # Vacíos: preservar si existe, crear nueva si no
            if conexion in self._config_data["vacios"]:
                vacios_nuevos[conexion] = self._config_data["vacios"][conexion]
            else:
                # Valores por defecto: conexiones entre depósitos tienen tiempos más largos
                tiempo_default = 60 if es_entre_depositos else 30
                km_default = 10.0 if es_entre_depositos else 0.0
                vacios_nuevos[conexion] = {
                    "habilitado": True,  # Por defecto habilitado para permitir flexibilidad
                    "franjas": [{"inicio": "00:00", "fin": "40:00", "tiempo": tiempo_default, "km": km_default}],
                }
            
            # Desplazamientos: preservar si existe, crear nueva si no
            if conexion in self._config_data["desplazamientos"]:
                desplazamientos_nuevos[conexion] = self._config_data["desplazamientos"][conexion]
            else:
                # Para conexiones entre depósitos, deshabilitar desplazamientos por defecto
                # (los conductores normalmente no se desplazan entre depósitos)
                tiempo_default = 60 if es_entre_depositos else 30
                habilitado_default = False if es_entre_depositos else True
                desplazamientos_nuevos[conexion] = {
                    "habilitado": habilitado_default, 
                    "tiempo": tiempo_default
                }
        
        self._config_data["vacios"] = vacios_nuevos
        self._config_data["desplazamientos"] = desplazamientos_nuevos
        
        # Actualizar paradas para todos los nodos (no para depósitos)
        paradas_nuevas = {}
        for nodo in nodos:
            # Solo agregar paradas para nodos que NO son depósitos
            if nodo not in depositos_activos:
                if nodo in self._config_data["paradas"]:
                    paradas_nuevas[nodo] = self._config_data["paradas"][nodo]
                else:
                    paradas_nuevas[nodo] = {"min": 5, "max": 480}
        
        self._config_data["paradas"] = paradas_nuevas
        
        # Contar conexiones entre depósitos
        conexiones_entre_depositos = 0
        for c in todas_las_conexiones:
            partes = c.split('_', 1)
            if len(partes) == 2:
                origen, destino = partes
                if origen in depositos_activos and destino in depositos_activos:
                    conexiones_entre_depositos += 1
        
        logger.info(f"  - Conexiones de vacíos: {len(self._config_data['vacios'])} (incluye {conexiones_entre_depositos} entre depósitos)")
        logger.info(f"  - Conexiones de desplazamientos: {len(self._config_data['desplazamientos'])}")
        logger.info(f"  - Paradas configuradas: {len(self._config_data['paradas'])}")
    
    def _sync_nodos_from_connections(self) -> None:
        """
        Asegura que la lista de nodos incluya todos los que aparecen en
        vacíos, desplazamientos y paradas (evita nodos faltantes).
        """
        nodos_actuales = set(self._config_data.get("nodos", []))
        for conexion in list(self._config_data.get("vacios", {}).keys()) + list(
            self._config_data.get("desplazamientos", {}).keys()
        ):
            if conexion and "_" in conexion:
                origen, destino = conexion.split("_", 1)
                if origen:
                    nodos_actuales.add(origen.strip())
                if destino:
                    nodos_actuales.add(destino.strip())
        for nodo in self._config_data.get("paradas", {}).keys():
            if nodo:
                nodos_actuales.add(nodo.strip())
        for dep in self._config_data.get("depositos", []) or []:
            if isinstance(dep, dict) and dep.get("nombre"):
                nodos_actuales.add(str(dep["nombre"]).strip())
        if self._config_data.get("deposito"):
            nodos_actuales.add(str(self._config_data["deposito"]).strip())
        nuevos = nodos_actuales - set(self._config_data.get("nodos", []))
        if nuevos:
            self._config_data["nodos"] = sorted(nodos_actuales)
            logger.info(f"Nodos sincronizados desde conexiones: +{len(nuevos)} nuevos: {sorted(nuevos)}")
    
    def _parse_nodos_from_text(self, texto: str) -> List[str]:
        """
        Parsea una cadena de nodos aceptando múltiples separadores:
        coma, punto y coma, salto de línea, tabulador.
        
        Args:
            texto: Cadena con nodos separados por comas, ;, \\n, \\t
            
        Returns:
            Lista ordenada y sin duplicados de nodos
        """
        if not texto or not str(texto).strip():
            return []
        partes = re.split(r"[,;\n\r\t]+", str(texto))
        return sorted(list(set(n.strip() for n in partes if n and n.strip())))
    
    def update_nodes(self, nodos: List[str]) -> None:
        """
        Actualiza la lista de nodos y regenera conexiones.
        
        Args:
            nodos: Lista de nombres de nodos
        """
        self._config_data["nodos"] = sorted(list(set(nodos)))
        self.regenerate_connections()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Retorna configuración por defecto."""
        return {
            "deposito": "Deposito Pie Andino",
            "limite_jornada": 720,
            "tiempo_toma": 15,
            "nodos": [],
            "vacios": {},
            "desplazamientos": {},
            "paradas": {},
            "tipos_bus": {},
            "lineas": {},
            "depositos": [],
            "flota_por_tipo": {},
            "interlineado_global": False,
            "grupos_lineas": {},
            "limite_jornada_por_grupo_linea": {},
            "puntos_relevo": [],
            "tipos_conductor": [],
            "max_inicio_jornada_conductor": "23:59",
            "fase_3_union_conductores": {
                "union_solo_por_deposito": False,
                "parada_larga_umbral_union": 60,
                "parada_larga_excepcion_depot_min": 120,
                "max_rondas_union": 1500,
                "timeout_ortools_segundos": 180,
            },
        }






