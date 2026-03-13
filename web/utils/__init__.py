"""
Utilidades compartidas para la aplicación web.
"""

from .logging_utils import WebLogger
from .validators import validate_excel_file, validate_config_data

__all__ = ["WebLogger", "validate_excel_file", "validate_config_data"]






