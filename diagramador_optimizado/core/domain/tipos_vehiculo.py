from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from diagramador_optimizado.utils.time_utils import _to_minutes


def _normalizar_porcentaje(valor: Any, default: float) -> float:
    """
    Convierte valores expresados como fracción (0-1) o porcentaje (0-100)
    a un porcentaje en el rango [0, 100].
    """
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return default
    if numero <= 1:
        numero *= 100
    return max(0.0, min(100.0, numero))


def _normalizar_float(valor: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if valor is None:
            return default
        return float(valor)
    except (TypeError, ValueError):
        return default


def _normalizar_int(valor: Any, default: int = 0) -> int:
    try:
        if valor is None:
            return default
        return int(float(valor))
    except (TypeError, ValueError):
        return default


def _normalizar_lista_tipos(
    tipos: Optional[Iterable[Any]],
    tipos_validos: Sequence[str],
) -> List[str]:
    if not tipos:
        return []
    tipos_normalizados: List[str] = []
    tipos_validos_upper = {t.upper() for t in tipos_validos}
    for tipo in tipos:
        nombre = str(tipo).strip().upper()
        if nombre and nombre in tipos_validos_upper and nombre not in tipos_normalizados:
            tipos_normalizados.append(nombre)
    return tipos_normalizados


@dataclass(frozen=True)
class VentanaHorario:
    """
    Representa una ventana horaria en minutos desde las 00:00.
    """

    inicio: int
    fin: int

    @staticmethod
    def desde_config(
        data: Optional[Dict[str, Any]],
        inicio_default: int,
        fin_default: int,
    ) -> VentanaHorario:
        inicio = _to_minutes((data or {}).get("inicio", inicio_default))
        fin = _to_minutes((data or {}).get("fin", fin_default))
        if fin <= inicio:
            fin = inicio + (fin_default - inicio_default)
        return VentanaHorario(inicio=inicio, fin=fin)


@dataclass(frozen=True)
class ParametrosElectricos:
    """
    Define todos los parámetros de operación eléctrica para un tipo de bus.
    Los valores que terminan en *_pct están expresados en porcentaje (0-100).
    """

    carga_inicial_pct: float
    consumo_pct_por_km: float
    minimo_para_circular_pct: float  # Renombrado de limite_operacion_pct
    tasa_recarga_pct_por_min: float
    tiempo_minimo_recarga: int  # Tiempo mínimo de recarga en minutos
    ventana_recarga: VentanaHorario
    porcentaje_max_entrada_pct: float  # % máximo con el que un bus puede entrar a recarga (si está por debajo puede entrar)
    consumo_pct_por_arco: Dict[str, float] = field(default_factory=dict)
    consumo_pct_por_linea: Dict[str, float] = field(default_factory=dict)

    def obtener_consumo_arco(self, clave: str) -> Optional[float]:
        return self.consumo_pct_por_arco.get(clave.upper())

    def obtener_consumo_linea(self, linea: str) -> Optional[float]:
        return self.consumo_pct_por_linea.get(linea.upper())


@dataclass(frozen=True)
class ConfiguracionTipoBus:
    """
    Configuración consolidada para un tipo de bus (diesel o eléctrico).
    """

    nombre: str
    descripcion: str = ""
    es_electrico: bool = False
    autonomia_km: Optional[float] = None
    capacidad_pasajeros: Optional[int] = None
    consumo_base_por_km: Optional[float] = None
    parametros_electricos: Optional[ParametrosElectricos] = None


@dataclass(frozen=True)
class ConfiguracionLinea:
    """
    Reglas operacionales por línea para determinar tipos habilitados
    y métricas de salida (frecuencia/duración óptima).
    """

    nombre: str
    tipos_permitidos: List[str]
    frecuencia_objetivo_min: Optional[float] = None
    duracion_optima_min: Optional[float] = None
    desviacion_frecuencia_permitida_min: Optional[float] = None
    desviacion_duracion_permitida_min: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfiguracionDeposito:
    """
    Estructura normalizada para la información de cada depósito.
    """

    nombre: str
    max_buses: int
    permite_recarga: bool
    posiciones_recarga: int
    flota_por_tipo: Dict[str, int] = field(default_factory=dict)


def _definiciones_tipos_por_defecto() -> Dict[str, Dict[str, Any]]:
    """
    Devuelve la definición base para cada tipo requerido (A, B, BE, BPAL, C).
    Estos valores se pueden sobreescribir parcial o totalmente desde JSON.
    """
    return {
        "A": {
            "descripcion": "Bus rígido estándar 12m",
            "es_electrico": False,
            "autonomia_km": 350,
            "capacidad_pasajeros": 90,
        },
        "B": {
            "descripcion": "Bus articulado de alta capacidad",
            "es_electrico": False,
            "autonomia_km": 320,
            "capacidad_pasajeros": 140,
        },
        "BE": {
            "descripcion": "Bus eléctrico con baterías de 450 kWh",
            "es_electrico": True,
            "autonomia_km": 260,
            "capacidad_pasajeros": 95,
            "parametros_electricos": {
                "carga_inicial": 95,
                "consumo_por_km": 0.5,
                "% minimo para circular": 30,
                "tasa_recarga_por_minuto": 1.25,
                "tiempo_minimo_recarga": 30,
                "max_entrada_recarga": 80,
                "ventana_recarga": {"inicio": "09:00", "fin": "18:00"},
            },
        },
        "BPAL": {
            "descripcion": "Bus padrón piso alto",
            "es_electrico": False,
            "autonomia_km": 330,
            "capacidad_pasajeros": 80,
        },
        "C": {
            "descripcion": "Bus corto de refuerzo",
            "es_electrico": False,
            "autonomia_km": 280,
            "capacidad_pasajeros": 60,
        },
    }


def _construir_parametros_electricos(
    datos: Optional[Dict[str, Any]],
) -> ParametrosElectricos:
    datos = datos or {}
    ventana = VentanaHorario.desde_config(
        datos.get("ventana_recarga"),
        inicio_default=_to_minutes("09:00"),
        fin_default=_to_minutes("18:00"),
    )
    consumo_linea = {
        str(linea).strip().upper(): float(valor)
        for linea, valor in (datos.get("consumo_por_linea") or {}).items()
        if isinstance(linea, str)
    }
    consumo_arco = {
        str(arco).strip().upper(): float(valor)
        for arco, valor in (datos.get("consumo_por_arco") or {}).items()
        if isinstance(arco, str)
    }
    return ParametrosElectricos(
        carga_inicial_pct=_normalizar_porcentaje(datos.get("carga_inicial"), 95.0),
        consumo_pct_por_km=_normalizar_float(datos.get("consumo_por_km"), 0.5) or 0.5,
        minimo_para_circular_pct=_normalizar_porcentaje(
            datos.get("% minimo para circular") or datos.get("limite_operacion"), 30.0
        ),  # Compatibilidad: acepta ambos nombres
        tasa_recarga_pct_por_min=_normalizar_float(datos.get("tasa_recarga_por_minuto"), 1.0) or 1.0,
        tiempo_minimo_recarga=_normalizar_int(datos.get("tiempo_minimo_recarga"), 30),
        ventana_recarga=ventana,
        porcentaje_max_entrada_pct=_normalizar_porcentaje(datos.get("max_entrada_recarga"), 80.0),
        consumo_pct_por_linea=consumo_linea,
        consumo_pct_por_arco=consumo_arco,
    )


def normalizar_tipos_bus(
    definiciones_usuario: Optional[Dict[str, Any]],
) -> Dict[str, ConfiguracionTipoBus]:
    """
    Fusiona definiciones por defecto con las provistas por el usuario.
    """
    definiciones = _definiciones_tipos_por_defecto()
    if definiciones_usuario:
        for nombre, data in definiciones_usuario.items():
            if not isinstance(data, dict):
                continue
            clave = str(nombre).strip().upper()
            if not clave:
                continue
            base = definiciones.get(clave, {})
            merged = {**base, **data}
            definiciones[clave] = merged

    resultado: Dict[str, ConfiguracionTipoBus] = {}
    for nombre, data in definiciones.items():
        es_electrico = bool(data.get("es_electrico", False))
        parametros = None
        if es_electrico:
            parametros = _construir_parametros_electricos(data.get("parametros_electricos"))
        resultado[nombre] = ConfiguracionTipoBus(
            nombre=nombre,
            descripcion=str(data.get("descripcion", "")),
            es_electrico=es_electrico,
            autonomia_km=_normalizar_float(data.get("autonomia_km")),
            capacidad_pasajeros=_normalizar_int(data.get("capacidad_pasajeros"), default=0) or None,
            consumo_base_por_km=_normalizar_float(data.get("consumo_por_km")),
            parametros_electricos=parametros,
        )
    return resultado


def normalizar_lineas(
    definiciones_lineas: Optional[Dict[str, Any]],
    tipos_disponibles: Sequence[str],
) -> Dict[str, ConfiguracionLinea]:
    """
    Construye la configuración por línea garantizando que cada una tenga
    al menos un tipo disponible.
    """
    resultado: Dict[str, ConfiguracionLinea] = {}
    if not tipos_disponibles:
        return resultado
    for nombre, data in (definiciones_lineas or {}).items():
        if not isinstance(data, dict):
            continue
        clave = str(nombre).strip().upper()
        if not clave:
            continue
        permitidos = _normalizar_lista_tipos(data.get("tipos_permitidos"), tipos_disponibles)
        if not permitidos:
            permitidos = list(tipos_disponibles)
        resultado[clave] = ConfiguracionLinea(
            nombre=clave,
            tipos_permitidos=permitidos,
            frecuencia_objetivo_min=_normalizar_float(data.get("frecuencia_objetivo_min")),
            duracion_optima_min=_normalizar_float(data.get("duracion_optima_min")),
            desviacion_frecuencia_permitida_min=_normalizar_float(data.get("desviacion_frecuencia_permitida_min")),
            desviacion_duracion_permitida_min=_normalizar_float(data.get("desviacion_duracion_permitida_min")),
            metadata={k: v for k, v in data.items() if k not in {"tipos_permitidos"} and not k.startswith("desviacion")},
        )
    return resultado


def normalizar_depositos_por_tipo(
    config: Dict[str, Any],
    tipos_disponibles: Sequence[str],
    permite_recarga_por_defecto: bool,
    posiciones_recarga_por_defecto: int,
    max_buses_default: int,
) -> List[ConfiguracionDeposito]:
    """
    Devuelve la lista de depósitos con sus cupos por tipo.
    """
    depositos = config.get("depositos")
    # Logging para depuración
    if depositos:
        print(f"[normalizar_depositos_por_tipo] Encontrados {len(depositos)} depósitos en configuración")
        for i, dep in enumerate(depositos, 1):
            if isinstance(dep, dict):
                print(f"  {i}. {dep.get('nombre', 'N/A')} (max_buses: {dep.get('max_buses', 'N/A')})")
    if not depositos:
        deposito_unico = config.get("deposito")
        if deposito_unico:
            depositos = [
                {
                    "nombre": deposito_unico,
                    "max_buses": max_buses_default,
                    "permite_recarga": permite_recarga_por_defecto,
                    "posiciones_recarga": posiciones_recarga_por_defecto,
                    "flota_por_tipo": config.get("flota_por_tipo", {}),
                }
            ]
        else:
            depositos = []

    resultado: List[ConfiguracionDeposito] = []
    for dep in depositos:
        if not isinstance(dep, dict):
            continue
        nombre = dep.get("nombre")
        if not nombre:
            continue
        flota_raw = dep.get("flota_por_tipo", {})
        flota_por_tipo = {
            tipo.upper(): _normalizar_int(flota_raw.get(tipo, 0), default=0)
            for tipo in tipos_disponibles
        }
        resultado.append(
            ConfiguracionDeposito(
                nombre=nombre,
                max_buses=_normalizar_int(dep.get("max_buses"), default=max_buses_default),
                permite_recarga=bool(dep.get("permite_recarga", permite_recarga_por_defecto)),
                posiciones_recarga=_normalizar_int(dep.get("posiciones_recarga"), default=posiciones_recarga_por_defecto),
                flota_por_tipo=flota_por_tipo,
            )
        )

    if not resultado:
        deposito_unico = config.get("deposito")
        if deposito_unico:
            resultado.append(
                ConfiguracionDeposito(
                    nombre=deposito_unico,
                    max_buses=max_buses_default,
                    permite_recarga=permite_recarga_por_defecto,
                    posiciones_recarga=posiciones_recarga_por_defecto,
                    flota_por_tipo={tipo: 0 for tipo in tipos_disponibles},
                )
            )

    return resultado

