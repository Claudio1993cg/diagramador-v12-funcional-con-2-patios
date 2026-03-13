from __future__ import annotations

import datetime
from typing import Optional, Union


def formatear_hora(minutos: Optional[int]) -> str:
    """
    Convierte un valor en minutos a una cadena HH:MM plegando las 24 horas.

    Args:
        minutos: Minutos transcurridos desde las 00:00.

    Returns:
        Representación HH:MM legible, siempre con dos dígitos por componente.
    """
    if minutos is None:
        minutos = 0
    fecha_base = datetime.datetime(2025, 1, 1, 0, 0)
    fecha_final = fecha_base + datetime.timedelta(minutes=int(minutos))
    return fecha_final.strftime("%H:%M")


def formatear_hora_deltatime(minutos: Optional[int]) -> str:
    """
    Convierte minutos a formato HH:MM sin ajustar al rango [0, 24).

    Args:
        minutos: Minutos transcurridos desde un origen arbitrario.

    Returns:
        Cadena HH:MM incluso si las horas superan 24.
    """
    if minutos is None:
        return "00:00"
    minutos = int(minutos)
    horas = minutos // 60
    minutos_residuo = minutos % 60
    return f"{horas:02d}:{minutos_residuo:02d}"


def minutos_desde_base_mas_duracion(minutos_inicio: int, duracion_minutos: int) -> int:
    """
    Suma duracion_minutos a minutos_inicio usando timedelta.
    No aplica módulo 24h: el resultado puede ser >= 1440 para mostrar 24:xx, 25:xx (fin siempre >= inicio).
    """
    if minutos_inicio is None:
        minutos_inicio = 0
    if duracion_minutos is None:
        duracion_minutos = 0
    base = datetime.datetime(2025, 1, 1, 0, 0)
    fin = base + datetime.timedelta(minutes=int(minutos_inicio)) + datetime.timedelta(minutes=int(duracion_minutos))
    return int((fin - base).total_seconds() // 60)


def _to_minutes(valor: Union[str, int, float, datetime.time, datetime.datetime, datetime.timedelta, None]) -> int:
    """
    Normaliza múltiples tipos de entrada a minutos enteros.

    Args:
        valor: Representación temporal soportada (str HH:MM, datetime, timedelta, etc.).

    Returns:
        Minutos enteros entre 0 y +inf. Devuelve 0 ante entradas inválidas.
    """
    try:
        if valor is None:
            return 0
        if isinstance(valor, (int, float)):
            return int(valor)
        if isinstance(valor, datetime.time):
            return valor.hour * 60 + valor.minute
        if isinstance(valor, datetime.datetime):
            return valor.hour * 60 + valor.minute
        if isinstance(valor, datetime.timedelta):
            return int(valor.total_seconds() // 60)
        if isinstance(valor, str):
            try:
                partes = valor.strip().split(":")
                horas = int(partes[0])
                minutos = int(partes[1])
                return horas * 60 + minutos
            except Exception:
                return 0
        return 0
    except Exception as exc:
        # Evitar caída del optimizador por entradas inesperadas.
        try:
            msg = f"[ERROR] _to_minutes: valor inválido ({type(valor)}): {valor!r}. {exc}"
            print(msg.encode("utf-8", "replace").decode("utf-8"))
        except Exception:
            pass
        return 0



