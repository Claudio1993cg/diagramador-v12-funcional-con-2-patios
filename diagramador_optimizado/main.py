"""
Wrapper legacy del punto de entrada CLI.
Reexporta main desde cli.main para compatibilidad con imports antiguos.
"""
from __future__ import annotations

from diagramador_optimizado.cli.main import main  # noqa: F401

__all__ = ["main"]

if __name__ == "__main__":
    main()

