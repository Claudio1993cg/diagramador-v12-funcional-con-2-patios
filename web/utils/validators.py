"""
Validadores para datos de entrada de la aplicación web.
"""

from typing import Optional, Dict, Any
from werkzeug.datastructures import FileStorage


def validate_excel_file(archivo: Optional[FileStorage]) -> tuple[bool, Optional[str]]:
    """
    Valida que un archivo sea un Excel válido.
    
    Args:
        archivo: Archivo subido por el usuario
        
    Returns:
        (es_valido, mensaje_error)
    """
    if not archivo:
        return False, "No se seleccionó ningún archivo"
    
    if archivo.filename == "":
        return False, "No se seleccionó ningún archivo"
    
    if not archivo.filename.lower().endswith(('.xlsx', '.xls')):
        return False, "El archivo debe ser un Excel (.xlsx o .xls)"
    
    return True, None


def validate_config_data(config: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Valida que una configuración tenga los campos requeridos.
    
    Args:
        config: Diccionario de configuración
        
    Returns:
        (es_valido, mensaje_error)
    """
    campos_requeridos = ["deposito", "limite_jornada", "tiempo_toma", "nodos"]
    
    for campo in campos_requeridos:
        if campo not in config:
            return False, f"El archivo Excel no tiene el campo requerido: {campo}"
    
    return True, None






