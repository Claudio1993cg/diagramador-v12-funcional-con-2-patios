"""
Ejecuta el diagramador exactamente como lo hace la web (mismo cwd, rutas absolutas).
Sirve para reproducir fallos con "lo cargado" (datos_salidas.xlsx).
"""
import os
import sys
import io

# Base = directorio del script (raíz del proyecto)
_base = os.path.abspath(os.path.dirname(__file__))
_motor = "diagramador_optimizado"
_config = os.path.join(_base, _motor, "configuracion.json")
_excel = os.path.join(_base, _motor, "datos_salidas.xlsx")
_salida = os.path.join(_base, _motor, "resultado_diagramacion.xlsx")

def main():
    if _base not in sys.path:
        sys.path.insert(0, _base)
    os.chdir(_base)

    if not os.path.exists(_excel):
        print(f"ERROR: No existe {_excel}. Carga datos desde la web o crea datos de prueba.")
        return 1
    if not os.path.exists(_config):
        print(f"ERROR: No existe {_config}")
        return 1

    _stdout_buf = io.StringIO()
    _stderr_buf = io.StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = _stdout_buf, _stderr_buf
    try:
        from diagramador_optimizado.main import main as diagramador_main
        diagramador_main(
            archivo_excel=os.path.abspath(_excel),
            archivo_config=os.path.abspath(_config),
            archivo_salida=os.path.abspath(_salida),
            random_seed=42,
        )
    except Exception as e:
        sys.stdout, sys.stderr = old_stdout, old_stderr
        print("=== STDOUT ===")
        print(_stdout_buf.getvalue())
        print("=== STDERR ===")
        print(_stderr_buf.getvalue())
        print("=== EXCEPCIÓN ===")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr

    out = _stdout_buf.getvalue()
    err = _stderr_buf.getvalue()
    if out:
        print(out)
    if err:
        print(err, file=sys.stderr)
    return 0

if __name__ == "__main__":
    sys.exit(main())
