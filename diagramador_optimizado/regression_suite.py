from __future__ import annotations

import argparse
import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from diagramador_optimizado.cli.main import _auditar_excel_resultado, main as run_pipeline
from diagramador_optimizado.core.domain.logistica import GestorDeLogistica
from diagramador_optimizado.core.engines.fase1_buses import resolver_diagramacion_buses
from diagramador_optimizado.core.engines.fase2_conductores import resolver_diagramacion_conductores
from diagramador_optimizado.core.engines.fase3_union import (
    _pueden_unirse,
    _turno_unido_es_consistente,
    _unir_turnos,
    resolver_union_conductores,
)
from diagramador_optimizado.io.loaders import cargar_salidas_desde_excel


@dataclass
class Scenario:
    name: str
    seed: int
    patch: Dict[str, Any]


def _deep_update(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(base)
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_update(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result


def _scenario_list(mode: str) -> List[Scenario]:
    quick = [
        Scenario("base_seed_42", 42, {}),
        Scenario("seed_7", 7, {}),
        Scenario(
            "union_solo_deposito",
            42,
            {"fase_3_union_conductores": {"union_solo_por_deposito": True}},
        ),
        Scenario(
            "parada_larga_75",
            42,
            {"fase_3_union_conductores": {"parada_larga_umbral_union": 75}},
        ),
    ]
    if mode == "quick":
        return quick
    return quick + [
        Scenario(
            "excepcion_deposito_150",
            42,
            {"fase_3_union_conductores": {"parada_larga_excepcion_depot_min": 150}},
        ),
        Scenario(
            "max_rondas_300",
            42,
            {"fase_3_union_conductores": {"max_rondas_union": 300}},
        ),
        Scenario(
            "ortools_timeout_60",
            42,
            {"fase_3_union_conductores": {"timeout_ortools_segundos": 60}},
        ),
        Scenario(
            "iterativa_2",
            42,
            {"optimizacion_iterativa": {"max_iteraciones": 2}},
        ),
        Scenario(
            "seed_101",
            101,
            {},
        ),
        Scenario(
            "seed_303",
            303,
            {},
        ),
    ]


def _mapa_viajes(viajes: List[Dict[str, Any]], metadata_tareas: Dict[Any, Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
    mapa: Dict[Any, Dict[str, Any]] = {}
    for v in viajes:
        for key in (v.get("id"), v.get("_tmp_id")):
            if key is not None:
                mapa[key] = v
                mapa[str(key)] = v
    for tid, meta in (metadata_tareas or {}).items():
        viaje = meta.get("viaje") if isinstance(meta, dict) else None
        if viaje:
            mapa[tid] = viaje
            mapa[str(tid)] = viaje
    return mapa


def _pending_unions(
    config: Dict[str, Any],
    viajes: List[Dict[str, Any]],
    gestor: GestorDeLogistica,
    turnos_f3: List[Dict[str, Any]],
    metadata_tareas: Dict[Any, Dict[str, Any]],
) -> int:
    if len(turnos_f3) < 2:
        return 0
    f3_cfg = config.get("fase_3_union_conductores") or {}
    limite = int(getattr(gestor, "limite_jornada", 600) or 600)
    descanso = int(f3_cfg.get("descanso_min", 0) or 0)
    max_cambios = int(f3_cfg.get("max_cambios_bus", 999) or 999)
    umbral = f3_cfg.get("parada_larga_umbral_union", None)
    exc = f3_cfg.get("parada_larga_excepcion_depot_min", None)
    union_dep = bool(f3_cfg.get("union_solo_por_deposito", False))
    mapa = _mapa_viajes(viajes, metadata_tareas)

    pending = 0
    n = len(turnos_f3)
    for i in range(n):
        for j in range(i + 1, n):
            found = False
            for ta, tb in ((turnos_f3[i], turnos_f3[j]), (turnos_f3[j], turnos_f3[i])):
                if not _pueden_unirse(
                    ta,
                    tb,
                    mapa,
                    gestor,
                    limite,
                    descanso,
                    max_cambios,
                    parada_larga_umbral_union=umbral,
                    parada_larga_excepcion_depot_min=exc,
                    union_solo_por_deposito=union_dep,
                    restringir_mismo_grupo=True,
                ):
                    continue
                merged = _unir_turnos(ta, tb, gestor, mapa)
                if _turno_unido_es_consistente(merged, mapa, gestor):
                    pending += 1
                    found = True
                    break
            if found:
                continue
    return pending


def _run_structural_checks(config: Dict[str, Any], excel_path: Path) -> Tuple[int, int, int]:
    gestor = GestorDeLogistica(config)
    viajes = cargar_salidas_desde_excel(str(excel_path))
    bloques, _, _ = resolver_diagramacion_buses(config, viajes, gestor, random_seed=42, verbose=False)
    turnos_f2, metadata, _ = resolver_diagramacion_conductores(config, viajes, bloques, gestor, verbose=False)
    turnos_f3, _ = resolver_union_conductores(config, turnos_f2, metadata, viajes, gestor, verbose=False, seed_externo=42)
    pending = _pending_unions(config, viajes, gestor, turnos_f3, metadata)
    return len(turnos_f2), len(turnos_f3), pending


def run_suite(mode: str) -> int:
    root = Path(__file__).resolve().parent
    base_config_path = root / "configuracion.json"
    excel_path = root / "datos_salidas.xlsx"
    output_dir = root / "regression_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    base_config = json.loads(base_config_path.read_text(encoding="utf-8"))
    scenarios = _scenario_list(mode)
    results: List[Tuple[str, bool, str]] = []

    for sc in scenarios:
        scenario_cfg = _deep_update(base_config, sc.patch)
        cfg_path = output_dir / f"config_{sc.name}.json"
        out_path = output_dir / f"resultado_{sc.name}.xlsx"
        cfg_path.write_text(json.dumps(scenario_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        if out_path.exists():
            out_path.unlink()

        ok = True
        msg = "OK"
        try:
            run_pipeline(
                archivo_excel=str(excel_path),
                archivo_config=str(cfg_path),
                archivo_salida=str(out_path),
                random_seed=sc.seed,
            )
            if not out_path.exists():
                raise RuntimeError("No se generó archivo de salida.")
            _auditar_excel_resultado(str(out_path), scenario_cfg)
            f2, f3, pending = _run_structural_checks(scenario_cfg, excel_path)
            if f3 > f2:
                raise RuntimeError(f"Fase 3 aumentó turnos ({f2}->{f3}).")
            if pending > 0:
                raise RuntimeError(f"Fase 3 dejó {pending} pares unibles pendientes.")
            msg = f"OK (F2={f2}, F3={f3}, pendientes={pending})"
        except Exception as exc:
            ok = False
            msg = f"FAIL: {exc}"
        results.append((sc.name, ok, msg))
        print(f"[REGRESION] {sc.name}: {msg}")

    total = len(results)
    fails = [r for r in results if not r[1]]
    print("\n=== RESUMEN REGRESION ===")
    print(f"Escenarios: {total}")
    print(f"OK: {total - len(fails)}")
    print(f"FAIL: {len(fails)}")
    if fails:
        for name, _, msg in fails:
            print(f"- {name}: {msg}")
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Suite de regresión del diagramador.")
    parser.add_argument(
        "--mode",
        choices=("quick", "standard"),
        default="standard",
        help="quick=4 escenarios, standard=10 escenarios",
    )
    args = parser.parse_args()
    raise SystemExit(run_suite(args.mode))


if __name__ == "__main__":
    main()

