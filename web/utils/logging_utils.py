"""
Utilidades para logging estructurado en la aplicacion web.
"""

import logging
from datetime import datetime
from typing import Optional
import os


class WebLogger:
    """
    Logger centralizado para la aplicacion web con escritura a archivo y consola.
    """
    
    def __init__(self, log_file: str = "log_app_web.txt"):
        """
        Inicializa el logger.
        
        Args:
            log_file: Ruta del archivo de log
        """
        self.log_file = log_file
        self._ensure_log_file()
    
    def _ensure_log_file(self) -> None:
        """Asegura que el archivo de log existe."""
        try:
            if not os.path.exists(self.log_file):
                with open(self.log_file, "w", encoding="utf-8") as f:
                    f.write("")
        except Exception:
            pass  # Si no se puede crear, continuar sin archivo
    
    def _limpiar_unicode(self, texto: str) -> str:
        """
        Limpia caracteres Unicode problematicos de un texto.
        
        Args:
            texto: Texto a limpiar
            
        Returns:
            Texto limpio con caracteres Unicode reemplazados por ASCII
        """
        if not texto:
            return ""
        
        try:
            # Convertir a string si no lo es
            texto_str = str(texto)
        except Exception:
            return "[texto no pudo ser convertido a string]"
        
        # PRIMERO: Reemplazar caracteres Unicode problematicos comunes por equivalentes ASCII
        # Hacer esto ANTES de cualquier codificacion para evitar errores
        reemplazos_unicode = {
            '\u2264': '<=',  # menor o igual
            '\u2265': '>=',  # mayor o igual
            '\u2260': '!=',  # diferente
            '\u2713': '[OK]',  # checkmark (check)
            '\u2714': '[OK]',  # checkmark pesado
            '\u00f3': 'o',   # o con tilde
            '\u00ed': 'i',   # i con tilde
            '\u00e1': 'a',   # a con tilde
            '\u00e9': 'e',   # e con tilde
            '\u00fa': 'u',   # u con tilde
            '\u00f1': 'n',   # n con tilde
            '\u00c1': 'A',   # A mayuscula con tilde
            '\u00c9': 'E',   # E mayuscula con tilde
            '\u00cd': 'I',   # I mayuscula con tilde
            '\u00d3': 'O',   # O mayuscula con tilde
            '\u00da': 'U',   # U mayuscula con tilde
            '\u00d1': 'N',   # N mayuscula con tilde
        }
        
        # Aplicar reemplazos de forma segura
        for unicode_char, ascii_replacement in reemplazos_unicode.items():
            try:
                texto_str = texto_str.replace(unicode_char, ascii_replacement)
            except Exception:
                # Si falla un reemplazo especifico, continuar con los demas
                continue
        
        # SEGUNDO: Convertir a ASCII reemplazando cualquier otro caracter problematico
        # Esto asegura que el resultado sea completamente ASCII
        try:
            texto_str = texto_str.encode('ascii', errors='replace').decode('ascii')
        except Exception:
            # Si falla, intentar una conversión más agresiva
            try:
                # Primero a UTF-8 con reemplazo, luego a ASCII
                texto_str = texto_str.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                texto_str = texto_str.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                # Si todo falla, devolver un mensaje seguro
                return "[texto no pudo ser procesado]"
        
        # Asegurar que el resultado final es completamente ASCII
        # Verificar que no queden caracteres Unicode problematicos
        try:
            # Intentar codificar de nuevo para verificar
            texto_str.encode('ascii')
        except UnicodeEncodeError:
            # Si todavía hay problemas, hacer una limpieza más agresiva
            try:
                texto_str = texto_str.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                return "[texto no pudo ser procesado]"
        
        return texto_str
    
    def log(self, mensaje: str, level: str = "INFO") -> None:
        """
        Registra un mensaje con timestamp.
        Este método NUNCA debe lanzar excepciones, incluso con problemas de encoding.
        
        Args:
            mensaje: Mensaje a registrar
            level: Nivel de log (INFO, WARNING, ERROR, DEBUG)
        """
        # ENVOLVER TODO EN UN TRY-EXCEPT GLOBAL para asegurar que NUNCA falle
        # Si este método falla, puede interrumpir todo el proceso de optimización
        try:
            # Limpiar el mensaje ANTES de cualquier otra operación
            # Usar try-except para asegurar que siempre tengamos un mensaje limpio
            try:
                mensaje_limpio = self._limpiar_unicode(mensaje)
            except Exception:
                # Si falla la limpieza, intentar una limpieza más agresiva
                try:
                    mensaje_limpio = str(mensaje).encode('ascii', errors='replace').decode('ascii')
                except Exception:
                    mensaje_limpio = "[mensaje no pudo ser procesado]"
            
            # Asegurar que mensaje_limpio es completamente ASCII ANTES de formatear
            try:
                mensaje_limpio = mensaje_limpio.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                mensaje_limpio = "[mensaje no pudo ser procesado]"
            
            # Asegurar que level también sea ASCII
            try:
                level = str(level).encode('ascii', errors='replace').decode('ascii')
            except Exception:
                level = "INFO"
            
            # Obtener marca de tiempo de forma segura
            try:
                marca = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                marca = "TIMESTAMP_ERROR"
            
            # Crear la línea de forma segura - usar concatenación en lugar de f-string para evitar problemas
            try:
                # Usar concatenación simple en lugar de f-string para evitar problemas de encoding
                parte1 = "[" + str(marca) + "] [" + str(level) + "] "
                linea = parte1 + str(mensaje_limpio)
            except Exception:
                # Si falla el formateo, crear una línea simple
                try:
                    linea = "[" + str(marca) + "] [" + str(level) + "] [mensaje truncado]"
                except Exception:
                    linea = "[LOG] " + str(level)
            
            # Asegurar que la línea también sea ASCII antes de escribir
            try:
                linea = str(linea).encode('ascii', errors='replace').decode('ascii')
            except Exception:
                try:
                    linea = "[LOG] " + str(level)
                except Exception:
                    linea = "[LOG]"
            
            # Escribir en consola de forma segura (NUNCA debe fallar)
            try:
                import sys
                # Intentar escribir directamente en el buffer con UTF-8
                if hasattr(sys.stdout, 'buffer'):
                    try:
                        # Codificar a UTF-8 con reemplazo de errores
                        linea_bytes = (linea + "\n").encode('utf-8', errors='replace')
                        sys.stdout.buffer.write(linea_bytes)
                        sys.stdout.buffer.flush()
                    except Exception:
                        # Si falla, intentar codificar a ASCII
                        try:
                            linea_ascii = linea.encode('ascii', errors='replace').decode('ascii')
                            print(linea_ascii, flush=True)
                        except Exception:
                            # Ultimo recurso: imprimir sin formato
                            try:
                                print(f"[{marca}] [{level}] [mensaje truncado]", flush=True)
                            except Exception:
                                # Si incluso esto falla, no hacer nada (no lanzar excepción)
                                pass
                else:
                    # Si no hay buffer, usar print normal
                    try:
                        # Asegurar que la línea sea ASCII antes de imprimir
                        linea_print = linea.encode('ascii', errors='replace').decode('ascii')
                        print(linea_print, flush=True)
                    except Exception:
                        try:
                            print(f"[{marca}] [{level}] [mensaje truncado]", flush=True)
                        except Exception:
                            # Si incluso esto falla, no hacer nada
                            pass
            except Exception:
                # Si todo falla, no hacer nada (no lanzar excepción)
                pass
            
            # Escribir en archivo (siempre UTF-8) - esto NUNCA debe fallar
            try:
                with open(self.log_file, "a", encoding="utf-8") as archivo_log:
                    archivo_log.write(linea + "\n")
            except Exception:
                # Si falla escribir en archivo, no hacer nada (no lanzar excepción)
                # El archivo de log es secundario, no debe interrumpir el flujo
                pass
        except Exception:
            # Si TODO falla, simplemente no hacer nada
            # Este método NUNCA debe lanzar excepciones
            pass
    
    def info(self, mensaje: str) -> None:
        """Registra un mensaje de nivel INFO."""
        self.log(mensaje, "INFO")
    
    def warning(self, mensaje: str) -> None:
        """Registra un mensaje de nivel WARNING."""
        self.log(mensaje, "WARNING")
    
    def error(self, mensaje: str) -> None:
        """Registra un mensaje de nivel ERROR."""
        self.log(mensaje, "ERROR")
    
    def debug(self, mensaje: str) -> None:
        """Registra un mensaje de nivel DEBUG."""
        self.log(mensaje, "DEBUG")


# Instancia global del logger
logger = WebLogger()






