from __future__ import annotations

import argparse
from pathlib import Path

from diagramador_optimizado.cli.main import main as run_pipeline
from diagramador_optimizado.regression_suite import run_suite


def run_pre_entrega(mode: str = "standard", skip_pipeline: bool = False) -> int:
    """
    Ejecuta chequeo integral de pre-entrega:
    1) Suite de regresión (quick/standard)
    2) Corrida final del pipeline principal
    """
    print("=" * 80)
    print("PRE-ENTREGA: VALIDACION INTEGRAL")
    print("=" * 80)
    print(f"Modo regresión: {mode}")
    print(f"Ejecutar pipeline final: {'NO' if skip_pipeline else 'SI'}")
    print("=" * 80)

    code = run_suite(mode)
    if code != 0:
        print("\n[PRE-ENTREGA] FALLA en suite de regresion. No se continua.")
        return code

    if not skip_pipeline:
        root = Path(__file__).resolve().parent
        print("\n[PRE-ENTREGA] Regresion OK. Ejecutando corrida final del pipeline...")
        run_pipeline(
            archivo_excel=str(root / "datos_salidas.xlsx"),
            archivo_config=str(root / "configuracion.json"),
            archivo_salida=str(root / "resultado_diagramacion.xlsx"),
            random_seed=42,
        )

    print("\n[PRE-ENTREGA] OK: todas las validaciones pasaron.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Chequeo integral de pre-entrega.")
    parser.add_argument(
        "--mode",
        choices=("quick", "standard"),
        default="standard",
        help="Nivel de cobertura de la regresion.",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Solo corre regresion; omite corrida final del pipeline.",
    )
    args = parser.parse_args()
    raise SystemExit(run_pre_entrega(mode=args.mode, skip_pipeline=args.skip_pipeline))


if __name__ == "__main__":
    main()
