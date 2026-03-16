"""Crea datos_salidas.xlsx mínimo para pruebas usando configuración dinámica."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple

from openpyxl import Workbook

BASE_DIR = Path(__file__).resolve().parent
RUTA_SALIDA = BASE_DIR / "datos_salidas.xlsx"
RUTA_CONFIG = BASE_DIR / "configuracion.json"
HEADERS = ["Linea", "Sentido", "Origen", "Destino", "Hora Inicio", "Hora Fin", "Kilometros"]


def _cargar_contexto() -> Tuple[str, List[str]]:
    if not RUTA_CONFIG.exists():
        return "", []
    with RUTA_CONFIG.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    depositos = cfg.get("depositos") or []
    deposito_base = ""
    if depositos and isinstance(depositos[0], dict):
        deposito_base = str(depositos[0].get("nombre", "") or "").strip()
    if not deposito_base:
        deposito_base = str(cfg.get("deposito", "") or "").strip()

    nodos = [str(n).strip() for n in (cfg.get("nodos") or []) if str(n).strip()]
    nodos_sin_deposito = [n for n in nodos if n.upper() != deposito_base.upper()]
    return deposito_base, nodos_sin_deposito


def _armar_viajes(deposito: str, nodos: List[str]) -> List[Tuple[str, str, str, str, str, str, float]]:
    if not deposito:
        deposito = "DEPOSITO_BASE"
    if not nodos:
        nodos = ["NODO_1", "NODO_2", "NODO_3"]

    destinos = nodos[:3]
    viajes: List[Tuple[str, str, str, str, str, str, float]] = []
    base_hora = 6 * 60
    for i, destino in enumerate(destinos, start=1):
        inicio_ida = base_hora + (i - 1) * 40
        fin_ida = inicio_ida + 40
        inicio_vta = fin_ida + 15
        fin_vta = inicio_vta + 40
        linea = f"L{i}"
        viajes.append((linea, "Ida", deposito, destino, f"{inicio_ida // 60:02d}:{inicio_ida % 60:02d}", f"{fin_ida // 60:02d}:{fin_ida % 60:02d}", 10.0 + i))
        viajes.append((linea, "Vuelta", destino, deposito, f"{inicio_vta // 60:02d}:{inicio_vta % 60:02d}", f"{fin_vta // 60:02d}:{fin_vta % 60:02d}", 10.0 + i))
    return viajes


def main() -> None:
    deposito, nodos = _cargar_contexto()
    viajes = _armar_viajes(deposito, nodos)

    wb = Workbook()
    ws = wb.active
    ws.title = "Salidas"
    ws.append(HEADERS)
    for v in viajes:
        ws.append(list(v))
    wb.save(RUTA_SALIDA)
    print(f"Creado {RUTA_SALIDA} con {len(viajes)} viajes.")


if __name__ == "__main__":
    main()
