#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Aplicación Web para Configuración del Diagramador
Punto de entrada principal de la aplicación Flask.
"""

from flask import Flask, request, jsonify, render_template_string, make_response, send_file, session, redirect, url_for
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.security import generate_password_hash, check_password_hash

def limpiar_mensaje_unicode(mensaje) -> str:
    """
    Limpia caracteres Unicode problemáticos de un mensaje antes de devolverlo al frontend.
    Esta función es crítica para evitar errores de encoding cuando se serializa a JSON.
    Esta función NUNCA debe fallar, incluso si el mensaje contiene errores de encoding.
    
    Args:
        mensaje: Mensaje a limpiar (puede ser cualquier tipo, se convertirá a string)
        
    Returns:
        Mensaje limpio con caracteres Unicode reemplazados por ASCII
    """
    # ENVOLVER TODO EN UN TRY-EXCEPT GLOBAL para asegurar que NUNCA falle
    try:
        # Paso 1: Convertir a string de forma ULTRA-SEGURA
        # Usar repr() primero porque escapa caracteres Unicode problemáticos
        mensaje_str = None
        try:
            # Intentar usar repr() primero (más seguro para caracteres Unicode)
            try:
                mensaje_repr = repr(mensaje)
                # repr() devuelve algo como "'texto'" o "b'texto'", necesitamos limpiarlo
                if mensaje_repr.startswith("'") and mensaje_repr.endswith("'"):
                    mensaje_str = mensaje_repr[1:-1]  # Quitar comillas simples
                elif mensaje_repr.startswith('"') and mensaje_repr.endswith('"'):
                    mensaje_str = mensaje_repr[1:-1]  # Quitar comillas dobles
                else:
                    mensaje_str = mensaje_repr
                # Limpiar escapes de repr (pero mantener los escapes de Unicode)
                mensaje_str = mensaje_str.replace("\\'", "'").replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
            except Exception:
                # Si repr() falla, intentar str() con manejo de errores
                try:
                    mensaje_str = str(mensaje)
                except UnicodeEncodeError:
                    # Si falla por encoding, usar el nombre del tipo
                    mensaje_str = type(mensaje).__name__
                except Exception:
                    mensaje_str = type(mensaje).__name__
        except Exception:
            # Si todo falla, usar el nombre del tipo
            try:
                mensaje_str = type(mensaje).__name__
            except Exception:
                return "Mensaje no pudo ser procesado"
        
        if mensaje_str is None:
            return "Mensaje no pudo ser procesado"
        
        # Paso 2: Reemplazar caracteres Unicode problemáticos comunes ANTES de cualquier otra operación
        # Esto es crítico porque algunos caracteres pueden causar errores durante el encoding
        reemplazos = {
            '\u2713': '[OK]',  # checkmark
            '\u2714': '[OK]',  # checkmark pesado
            '\u2264': '<=',    # menor o igual
            '\u2265': '>=',    # mayor o igual
            '\u2260': '!=',    # diferente
        }
        
        for unicode_char, replacement in reemplazos.items():
            try:
                if unicode_char in mensaje_str:
                    mensaje_str = mensaje_str.replace(unicode_char, replacement)
            except Exception:
                # Si falla un reemplazo específico, continuar con los demás
                continue
        
        # Paso 3: Convertir a ASCII de forma segura usando bytes directamente
        # Esto evita problemas con f-strings y formateo
        try:
            # Intentar codificar a bytes con UTF-8 primero (más permisivo)
            try:
                mensaje_bytes = mensaje_str.encode('utf-8', errors='replace')
            except Exception:
                # Si falla UTF-8, intentar latin-1 (más permisivo aún)
                try:
                    mensaje_bytes = mensaje_str.encode('latin-1', errors='replace')
                except Exception:
                    # Si todo falla, usar bytes directamente
                    mensaje_bytes = bytes(mensaje_str, 'utf-8', errors='replace')
            
            # Decodificar a ASCII reemplazando caracteres problemáticos
            mensaje_str = mensaje_bytes.decode('ascii', errors='replace')
        except Exception:
            # Si falla, intentar una limpieza más agresiva carácter por carácter
            try:
                resultado = []
                for char in mensaje_str:
                    try:
                        char.encode('ascii')
                        resultado.append(char)
                    except UnicodeEncodeError:
                        # Reemplazar caracteres que no se pueden codificar
                        resultado.append('?')
                mensaje_str = ''.join(resultado)
            except Exception:
                return "Mensaje no pudo ser procesado"
        
        return mensaje_str
    except Exception:
        # Si TODO falla, devolver un mensaje genérico seguro
        # Esto NUNCA debe fallar
        return "Mensaje no pudo ser procesado"
import os
import sys
import traceback
import time
import json
from datetime import datetime
from typing import Any, Dict

# Importar servicios
from web.services.config_service import ConfigService, TIPOS_BUS_DISPONIBLES
from web.services.excel_service import ExcelTripsService, ExcelConfigService
from web.services.optimization_service import OptimizationService, obtener_mensaje_error_seguro
from web.utils.logging_utils import logger

# Carpeta del motor de optimización: aquí están config, Excel de entrada y resultado
MOTOR_DIR = "diagramador_optimizado"

# Raíz del proyecto: carpeta que contiene "web" y "diagramador_optimizado" (desde ubicación de web/, no de app.py)
# Así las rutas son correctas aunque se ejecute desde otra carpeta o app.py esté en otro sitio
def _get_project_root():
    try:
        import web.services.optimization_service as _opt_mod
        # web/services/optimization_service.py -> subir .. = web/, .. = raíz
        _opt_dir = os.path.dirname(os.path.abspath(_opt_mod.__file__))
        return os.path.normpath(os.path.join(_opt_dir, "..", ".."))
    except Exception:
        return os.path.normpath(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))

_base = _get_project_root()
_dir_app = _base
if os.path.isdir(_dir_app) and _dir_app != os.getcwd():
    try:
        os.chdir(_dir_app)
        logger.info(f"Directorio de trabajo fijado al proyecto: {_dir_app}")
    except Exception as e:
        logger.warning(f"No se pudo cambiar al directorio del proyecto: {e}")

# Inicializar Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("DIAGRAMADOR_WEB_SECRET", "diagramador_web_secret_dev_2026")
APP_VERSION = os.environ.get("DIAGRAMADOR_APP_VERSION", "2026.03.13.2")

# Rutas canónicas: TODO en diagramador_optimizado/ (misma carpeta que config)
# - configuracion.json: carga/guardado desde la web
# - datos_salidas.xlsx: al cargar "datos de salida" en la web se guarda aquí
# - resultado_diagramacion.xlsx: salida del motor y descarga desde la web
_config_path = os.path.abspath(os.path.join(_base, MOTOR_DIR, "configuracion.json"))
_trips_path = os.path.abspath(os.path.join(_base, MOTOR_DIR, "datos_salidas.xlsx"))
_result_path = os.path.abspath(os.path.join(os.path.dirname(_config_path), "resultado_diagramacion.xlsx"))

config_service = ConfigService(config_path=_config_path)
excel_trips_service = ExcelTripsService(config_service, trips_file_path=_trips_path)
excel_config_service = ExcelConfigService(config_service, excel_trips_service=excel_trips_service)
optimization_service = OptimizationService(
    config_service, excel_trips_service, motor_dir=MOTOR_DIR, result_file_path=_result_path
)
logger.info(
    f"Rutas en diagramador_optimizado/: config={_config_path}, datos_salidas={_trips_path}, resultado={_result_path}"
)

# Archivo de usuarios para login web
_users_path = os.path.abspath(os.path.join(_base, MOTOR_DIR, "usuarios_web.json"))


def _load_users() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(_users_path):
        return {}
    try:
        with open(_users_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        logger.warning(f"No se pudo leer {_users_path}: {e}")
    return {}


def _load_bootstrap_users_from_env() -> Dict[str, Dict[str, Any]]:
    """
    Carga usuarios bootstrap desde variable de entorno WEB_BOOTSTRAP_USERS_JSON.
    Formato:
    {
      "usuario": {"password": "clave", "is_admin": true},
      ...
    }
    """
    raw = os.environ.get("WEB_BOOTSTRAP_USERS_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for username, info in data.items():
            if not isinstance(info, dict):
                continue
            pwd = str(info.get("password", "")).strip()
            if len(username.strip()) < 1 or len(pwd) < 1:
                continue
            out[str(username).strip()] = {
                "password": pwd,
                "is_admin": bool(info.get("is_admin", False)),
            }
        return out
    except Exception as e:
        logger.warning(f"WEB_BOOTSTRAP_USERS_JSON inválido: {e}")
        return {}


def _save_users(users: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(_users_path), exist_ok=True)
    with open(_users_path, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def _ensure_default_users() -> None:
    users = _load_users()
    changed = False
    defaults = {
        "ccantero": {"password": "Juacko1993", "is_admin": True},
        "dlopez": {"password": "Lopez2803", "is_admin": False},
    }
    env_defaults = _load_bootstrap_users_from_env()
    defaults.update(env_defaults)
    for username, info in defaults.items():
        if username not in users:
            users[username] = {
                "password_hash": generate_password_hash(info["password"]),
                "is_admin": bool(info["is_admin"]),
                "created_at": datetime.now().isoformat(),
            }
            changed = True
        else:
            # Garantizar flag admin para ccantero
            if username == "ccantero" and not bool(users[username].get("is_admin", False)):
                users[username]["is_admin"] = True
                changed = True
    if changed:
        _save_users(users)


def _get_current_user() -> Dict[str, Any] | None:
    username = session.get("username")
    if not username:
        return None
    users = _load_users()
    user = users.get(username)
    if not user:
        session.pop("username", None)
        return None
    return {
        "username": username,
        "is_admin": bool(user.get("is_admin", False)),
    }


def _is_api_request() -> bool:
    if request.path.startswith("/api/"):
        return True
    accepted = request.headers.get("Accept", "")
    return "application/json" in accepted or request.method != "GET"


@app.before_request
def _require_auth():
    public_paths = {"/login", "/favicon.ico", "/healthz", "/api/version"}
    if request.path in public_paths or request.path.startswith("/static/"):
        return None
    if _get_current_user() is not None:
        return None
    if _is_api_request():
        return jsonify({"success": False, "message": "No autorizado. Inicia sesión."}), 401
    # Evitar redirecciones para reducir errores intermitentes en edge.
    return _render_login_page(), 200


def _render_login_page(error_message: str = "") -> str:
    error_html = f"<p style='color:#f87171;margin-top:10px'>{error_message}</p>" if error_message else ""
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Ingreso - Diagramador</title>
  <style>
    body {{ font-family: 'Segoe UI', sans-serif; background:#1f2937; color:#e5e7eb; display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; }}
    .card {{ width:min(420px,92vw); background:#111827; border:1px solid #374151; border-radius:12px; padding:24px; }}
    h1 {{ margin:0 0 8px 0; font-size:1.4rem; color:#60a5fa; }}
    p {{ margin:0 0 18px 0; color:#9ca3af; font-size:.95rem; }}
    label {{ display:block; margin:10px 0 6px; font-size:.9rem; }}
    input {{ width:100%; box-sizing:border-box; border:1px solid #4b5563; background:#0b1220; color:#e5e7eb; border-radius:8px; padding:10px; }}
    button {{ margin-top:14px; width:100%; border:none; border-radius:8px; padding:11px; background:#3b82f6; color:white; font-weight:700; cursor:pointer; }}
    button:hover {{ background:#2563eb; }}
    .hint {{ margin-top:14px; font-size:.8rem; color:#6b7280; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Ingreso al Diagramador</h1>
    <p>Inicia sesión para usar el sistema.</p>
    <form method="post" action="/login">
      <label for="username">Usuario</label>
      <input id="username" name="username" required>
      <label for="password">Contraseña</label>
      <input id="password" name="password" type="password" required>
      <button type="submit">Ingresar</button>
    </form>
    {error_html}
    <div class="hint">Si eres administrador, podrás crear y eliminar usuarios.</div>
  </div>
</body>
</html>"""


_ensure_default_users()


# Función helper para detectar si estamos en un ejecutable
def _is_frozen():
    """Retorna True si estamos ejecutando desde un ejecutable compilado."""
    return getattr(sys, 'frozen', False)


# Cargar template HTML desde archivo separado
def _load_html_template() -> str:
    """Carga el template HTML desde un archivo separado."""
    # Intentar múltiples rutas posibles (para ejecutable y desarrollo)
    posibles_rutas = []
    
    # Ruta desde el directorio actual del script
    posibles_rutas.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "web", "templates", "index.html"))
    
    # Ruta relativa desde __file__
    posibles_rutas.append(os.path.join(os.path.dirname(__file__), "web", "templates", "index.html"))
    
    # Ruta desde MEIPASS si estamos en un ejecutable
    if _is_frozen():
        try:
            posibles_rutas.append(os.path.join(sys._MEIPASS, "web", "templates", "index.html"))
        except AttributeError:
            pass
    
    for template_path in posibles_rutas:
        if os.path.exists(template_path):
            try:
                with open(template_path, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception as e:
                logger.warning(f"Error leyendo template desde {template_path}: {e}")
                continue
    
    # Fallback: template inline (para compatibilidad)
    logger.warning("Template HTML no encontrado en ninguna ruta, usando template inline")
    return "<html><body><h1>Diagramador de Buses y Conductores</h1><p>Template no encontrado</p></body></html>"


# Manejador explícito para 404 (evita que se muestre como "Error interno del servidor")
@app.errorhandler(404)
def not_found(e):
    """Devuelve 404 en JSON para APIs o página HTML amigable en español."""
    if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "application/json":
        return jsonify({"success": False, "message": "Recurso no encontrado"}), 404
    # Página HTML amigable en español en lugar del mensaje por defecto de Werkzeug
    html = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>Página no encontrada</title></head><body>"
        "<h1>Página no encontrada</h1>"
        "<p>La dirección solicitada no existe en esta aplicación.</p>"
        "<p><a href='/'>Volver al Diagramador</a></p>"
        "</body></html>"
    )
    return make_response(html, 404)


# Manejador de errores global
@app.errorhandler(Exception)
def handle_exception(e):
    """Maneja excepciones. Las HTTP (404, etc.) se devuelven con su código; el resto como 500."""
    if isinstance(e, HTTPException):
        return e.get_response()
    msg = str(e)
    # Cualquier excepción que indique 404/Not Found → devolver 404 limpio (nunca "Error interno: 404...")
    if "404" in msg or "Not Found" in msg or "requested URL was not found" in msg or "was not found on the server" in msg:
        return jsonify({"success": False, "message": "Recurso no encontrado"}), 404
    logger.error(f"ERROR NO MANEJADO: {e}")
    traceback.print_exc()
    return jsonify({
        "success": False,
        "message": f"Error interno del servidor: {str(e)}"
    }), 500


@app.route("/login", methods=["GET", "POST"])
def login():
    """Ingreso de usuarios para la web."""
    if request.method == "GET":
        if _get_current_user() is not None:
            return redirect(url_for("index"))
        return _render_login_page()

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    users = _load_users()
    user = users.get(username)
    if not user or not check_password_hash(user.get("password_hash", ""), password):
        return _render_login_page("Usuario o contraseña inválidos."), 401
    session["username"] = username
    return redirect(url_for("index"))


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True, "service": "diagramador-web", "version": APP_VERSION}), 200


@app.route("/api/version", methods=["GET"])
def api_version():
    return jsonify({"success": True, "version": APP_VERSION}), 200


@app.route("/logout", methods=["POST", "GET"])
def logout():
    session.pop("username", None)
    if _is_api_request():
        return jsonify({"success": True, "message": "Sesión cerrada."})
    return redirect(url_for("login"))


@app.route("/api/session", methods=["GET"])
def api_session():
    user = _get_current_user()
    if not user:
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True, "user": user})


@app.route("/api/admin/users", methods=["GET"])
def api_admin_list_users():
    current = _get_current_user()
    if not current or not current.get("is_admin"):
        return jsonify({"success": False, "message": "Acceso solo administrador."}), 403
    users = _load_users()
    listado = []
    for username, data in sorted(users.items(), key=lambda x: x[0].lower()):
        listado.append(
            {
                "username": username,
                "is_admin": bool(data.get("is_admin", False)),
                "created_at": data.get("created_at", ""),
            }
        )
    return jsonify({"success": True, "users": listado})


@app.route("/api/admin/users", methods=["POST"])
def api_admin_create_user():
    current = _get_current_user()
    if not current or not current.get("is_admin"):
        return jsonify({"success": False, "message": "Acceso solo administrador."}), 403
    payload = request.get_json(silent=True) or {}
    username = (payload.get("username") or "").strip()
    password = payload.get("password") or ""
    is_admin = bool(payload.get("is_admin", False))
    if not username or len(username) < 3:
        return jsonify({"success": False, "message": "El usuario debe tener al menos 3 caracteres."}), 400
    if len(password) < 4:
        return jsonify({"success": False, "message": "La contraseña debe tener al menos 4 caracteres."}), 400
    users = _load_users()
    if username in users:
        return jsonify({"success": False, "message": "El usuario ya existe."}), 400
    users[username] = {
        "password_hash": generate_password_hash(password),
        "is_admin": is_admin,
        "created_at": datetime.now().isoformat(),
    }
    _save_users(users)
    return jsonify({"success": True, "message": f"Usuario '{username}' creado."})


@app.route("/api/admin/users/<username>", methods=["DELETE"])
def api_admin_delete_user(username: str):
    current = _get_current_user()
    if not current or not current.get("is_admin"):
        return jsonify({"success": False, "message": "Acceso solo administrador."}), 403
    username = (username or "").strip()
    users = _load_users()
    if username not in users:
        return jsonify({"success": False, "message": "Usuario no encontrado."}), 404
    if username == current.get("username"):
        return jsonify({"success": False, "message": "No puedes eliminar tu propio usuario activo."}), 400
    if users.get(username, {}).get("is_admin"):
        admins = [u for u, d in users.items() if bool(d.get("is_admin", False))]
        if len(admins) <= 1:
            return jsonify({"success": False, "message": "Debe existir al menos un administrador."}), 400
    users.pop(username, None)
    _save_users(users)
    return jsonify({"success": True, "message": f"Usuario '{username}' eliminado."})


@app.route("/api/admin/users/<username>", methods=["PUT"])
def api_admin_update_user(username: str):
    current = _get_current_user()
    if not current or not current.get("is_admin"):
        return jsonify({"success": False, "message": "Acceso solo administrador."}), 403

    username = (username or "").strip()
    payload = request.get_json(silent=True) or {}
    new_username = (payload.get("new_username") or username).strip()
    new_password = payload.get("password")
    set_admin = payload.get("is_admin")

    users = _load_users()
    if username not in users:
        return jsonify({"success": False, "message": "Usuario no encontrado."}), 404
    if not new_username or len(new_username) < 3:
        return jsonify({"success": False, "message": "El usuario debe tener al menos 3 caracteres."}), 400
    if new_username != username and new_username in users:
        return jsonify({"success": False, "message": "Ya existe un usuario con ese nombre."}), 400

    user_data = dict(users.get(username) or {})
    if isinstance(new_password, str) and new_password.strip():
        if len(new_password.strip()) < 4:
            return jsonify({"success": False, "message": "La contraseña debe tener al menos 4 caracteres."}), 400
        user_data["password_hash"] = generate_password_hash(new_password.strip())

    if set_admin is not None:
        set_admin_bool = bool(set_admin)
        if user_data.get("is_admin", False) and not set_admin_bool:
            admins = [u for u, d in users.items() if bool(d.get("is_admin", False))]
            # Si se edita a sí mismo y quedaría sin admin, bloquear.
            if len(admins) <= 1 and username in admins:
                return jsonify({"success": False, "message": "Debe existir al menos un administrador."}), 400
        user_data["is_admin"] = set_admin_bool

    if new_username != username:
        users.pop(username, None)
        users[new_username] = user_data
        if current.get("username") == username:
            session["username"] = new_username
    else:
        users[username] = user_data

    _save_users(users)
    return jsonify({"success": True, "message": f"Usuario '{username}' actualizado.", "username": new_username})


@app.route("/")
def index():
    """Página principal. No se hace precarga pesada aquí para no bloquear el servidor (un solo hilo)."""
    try:
        # Cargar configuración (rápido: solo lectura de JSON)
        config = config_service.load_config()
        
        # No leer datos_salidas.xlsx en cada carga de página: bloqueaba el servidor y dejaba
        # "Preparando optimización..." colgado. Los nodos se actualizan al subir el Excel.
        
        # Obtener conexiones dinámicamente
        conexiones = config_service.get_all_connections()
        tipos_bus_codigos = TIPOS_BUS_DISPONIBLES
        lineas_disponibles = list(config.get("lineas", {}).keys())
        depositos_nombres = set(
            d.get("nombre", "") for d in (config.get("depositos") or [])
            if isinstance(d, dict) and d.get("nombre")
        )
        if config.get("deposito"):
            depositos_nombres.add(config["deposito"])
        nodos_para_relevo = [n for n in (config.get("nodos") or []) if n and str(n).strip() not in depositos_nombres]
        
        # Cargar template HTML
        html_template = _load_html_template()
        
        # Renderizar con datos
        timestamp = int(time.time())
        html_content = render_template_string(
            html_template,
            config=config,
            conexiones=conexiones,
            timestamp=timestamp,
            app_version=APP_VERSION,
            tipos_bus_codigos=tipos_bus_codigos,
            lineas_disponibles=lineas_disponibles,
            nodos_para_relevo=nodos_para_relevo,
            current_user=_get_current_user(),
            is_admin=bool((_get_current_user() or {}).get("is_admin", False)),
        )
        
        # Crear respuesta con headers para evitar caché
        response = make_response(html_content)
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        response.headers['Last-Modified'] = datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')
        response.headers['ETag'] = f'"{timestamp}"'
        return response
        
    except HTTPException:
        raise  # 404, etc.: que las maneje el errorhandler
    except Exception as e:
        logger.error(f"Error cargando la página: {e}")
        traceback.print_exc()
        error_msg = f"Error cargando la página: {str(e)}<br><br>Traceback:<br><pre>{traceback.format_exc()}</pre>"
        return error_msg, 500


@app.route("/procesar-excel", methods=["POST"])
def procesar_excel():
    """Procesa el archivo Excel subido."""
    try:
        archivo = request.files.get("archivo")
        resultado = excel_trips_service.process_upload(archivo)
        return jsonify(resultado)
    except Exception as e:
        try:
            error_str = limpiar_mensaje_unicode(str(e))
            logger.error(f"ERROR en procesar_excel: {error_str}")
        except Exception:
            logger.error("ERROR en procesar_excel: [error no pudo ser procesado]")
        traceback.print_exc()
        try:
            error_str = limpiar_mensaje_unicode(str(e))
            mensaje_error = limpiar_mensaje_unicode(f"Error procesando archivo: {error_str}")
        except Exception:
            mensaje_error = "Error procesando archivo"
        return jsonify({"success": False, "message": mensaje_error})


@app.route("/guardar-configuracion", methods=["POST"])
def guardar_configuracion():
    """Guarda la configuración del sistema."""
    try:
        resultado = config_service.update_from_form(request.form)
        return jsonify(resultado)
    except Exception as e:
        try:
            error_str = limpiar_mensaje_unicode(str(e))
            logger.error(f"Error guardando configuracion: {error_str}")
        except Exception:
            logger.error("Error guardando configuracion: [error no pudo ser procesado]")
        traceback.print_exc()
        try:
            error_str = limpiar_mensaje_unicode(str(e))
            mensaje_error = limpiar_mensaje_unicode(f"Error guardando configuracion: {error_str}")
        except Exception:
            mensaje_error = "Error guardando configuracion"
        return jsonify({"success": False, "message": mensaje_error})


@app.route("/api/rutas-resultado")
def api_rutas_resultado():
    """Diagnóstico: devuelve las rutas donde se busca el resultado (solo para verificación)."""
    try:
        rutas = optimization_service.get_result_search_paths()
        canonica = optimization_service.get_canonical_result_path()
        return jsonify({
            "success": True,
            "rutas": rutas,
            "canonica": canonica,
            "cwd": os.getcwd(),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/ejecutar-optimizacion", methods=["POST"])
def ejecutar_optimizacion():
    """Ejecuta la optimización en un hilo separado."""
    try:
        resultado = optimization_service.start_optimization()
        return jsonify(resultado)
    except Exception as e:
        try:
            error_str = limpiar_mensaje_unicode(str(e))
            logger.error(f"ERROR en ejecutar_optimizacion: {error_str}")
        except Exception:
            logger.error("ERROR en ejecutar_optimizacion: [error no pudo ser procesado]")
        traceback.print_exc()
        try:
            error_str = limpiar_mensaje_unicode(str(e))
            mensaje_error = limpiar_mensaje_unicode(f"Error al iniciar la optimizacion: {error_str}")
        except Exception:
            mensaje_error = "Error al iniciar la optimizacion"
        return jsonify({
            "success": False,
            "message": mensaje_error
        }), 500


def _resetar_optimizacion_impl():
    """Implementación compartida del reseteo de estado."""
    optimization_service.reset_optimization_state()
    return jsonify({"success": True, "message": "Estado de optimizacion reseteado"})


@app.route("/resetar-optimizacion", methods=["POST", "GET"])
def resetar_optimizacion():
    """Resetea el estado 'optimización en ejecución'. Acepta GET y POST."""
    logger.info("[resetar-optimizacion] Ruta llamada")
    try:
        return _resetar_optimizacion_impl()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ERROR en resetar_optimizacion: {e}")
        return jsonify({"success": False, "message": "Error al resetear el estado"}), 500


@app.route("/api/resetar-optimizacion", methods=["POST", "GET"])
def api_resetar_optimizacion():
    """Misma acción que /resetar-optimizacion por si la ruta base falla (404)."""
    logger.info("[api/resetar-optimizacion] Ruta llamada")
    try:
        return _resetar_optimizacion_impl()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ERROR en api resetar_optimizacion: {e}")
        return jsonify({"success": False, "message": "Error al resetear el estado"}), 500


@app.route("/progreso")
def obtener_progreso():
    """Obtiene el progreso de la optimización."""
    try:
        progreso = optimization_service.get_progress()
        return jsonify(progreso)
    except Exception as e:
        # Obtener mensaje de error de forma segura
        try:
            if hasattr(e, 'args') and e.args and isinstance(e.args[0], str):
                error_bytes = e.args[0].encode('utf-8', errors='replace')
                error_str = error_bytes.decode('ascii', errors='replace')
            else:
                error_str = type(e).__name__
            error_str = limpiar_mensaje_unicode(error_str)
            logger.error("Error obteniendo progreso: " + error_str)
        except Exception:
            logger.error("Error obteniendo progreso: [error no pudo ser procesado]")
            error_str = "Error desconocido"
        
        try:
            mensaje_error = limpiar_mensaje_unicode("Error: " + error_str)
        except Exception:
            mensaje_error = "Error obteniendo progreso"
        return jsonify({
            "success": False,
            "is_running": False,
            "progress": 0,
            "message": mensaje_error,
        })


@app.route("/resultados")
def obtener_resultados():
    """Obtiene un resumen de los resultados de la optimización."""
    try:
        resumen = optimization_service.get_results_summary()
        return jsonify(resumen)
    except Exception as e:
        # Obtener mensaje de error de forma segura
        try:
            if hasattr(e, 'args') and e.args and isinstance(e.args[0], str):
                error_bytes = e.args[0].encode('utf-8', errors='replace')
                error_str = error_bytes.decode('ascii', errors='replace')
            else:
                error_str = type(e).__name__
            error_str = limpiar_mensaje_unicode(error_str)
            logger.error("Error obteniendo resultados: " + error_str)
        except Exception:
            logger.error("Error obteniendo resultados: [error no pudo ser procesado]")
            error_str = "Error desconocido"
        
        try:
            mensaje_error = limpiar_mensaje_unicode("Error obteniendo resultados: " + error_str)
        except Exception:
            mensaje_error = "Error obteniendo resultados"
        return jsonify({
            "success": False,
            "message": mensaje_error,
            "resultados": {"total_buses": 0, "total_conductors": 0},
        })


def _buscar_resultado_en_carpeta(carpeta: str) -> "str|None":
    """Busca resultado_diagramacion.xlsx en una carpeta; devuelve ruta absoluta si existe y tiene tamaño > 0."""
    if not carpeta or not os.path.isdir(carpeta):
        return None
    canonico = os.path.join(carpeta, "resultado_diagramacion.xlsx")
    if os.path.isfile(canonico) and os.path.getsize(canonico) > 0:
        return os.path.abspath(canonico)
    try:
        for nombre in os.listdir(carpeta):
            if nombre.startswith("resultado_diagramacion") and nombre.endswith(".xlsx"):
                ruta = os.path.join(carpeta, nombre)
                if os.path.isfile(ruta) and os.path.getsize(ruta) > 0:
                    return os.path.abspath(ruta)
    except Exception:
        pass
    return None


@app.route("/descargar-excel")
def descargar_excel():
    """Descarga el archivo Excel de resultados (diagramador_optimizado/resultado_diagramacion.xlsx)."""
    try:
        rutas_posibles = list(optimization_service.get_result_search_paths())
        ruta_canonica = optimization_service.get_canonical_result_path()
        if ruta_canonica not in rutas_posibles:
            rutas_posibles.insert(0, ruta_canonica)
        # Incluir búsqueda en la carpeta del motor por si el archivo está con otro nombre o path
        dir_motor = os.path.dirname(ruta_canonica)
        if dir_motor and dir_motor not in rutas_posibles:
            rutas_posibles.append(dir_motor)

        archivo_encontrado = None
        for ruta in rutas_posibles:
            ruta_abs = os.path.abspath(ruta)
            # Limpiar la ruta antes de loguearla
            try:
                ruta_limpia = limpiar_mensaje_unicode(str(ruta_abs))
                existe = os.path.exists(ruta_abs)
                logger.info(f"Verificando: {ruta_limpia} (existe: {existe})")
            except Exception:
                # Si falla, simplemente no loguear
                pass
            
            if os.path.isfile(ruta_abs):
                if os.path.exists(ruta_abs):
                    tamaño = os.path.getsize(ruta_abs)
                    if tamaño > 0:
                        archivo_encontrado = ruta_abs
                        try:
                            archivo_limpio = limpiar_mensaje_unicode(str(archivo_encontrado))
                            logger.info("[OK] Archivo encontrado: " + archivo_limpio + " (" + str(tamaño) + " bytes)")
                        except Exception:
                            try:
                                logger.info("Archivo encontrado: " + str(archivo_encontrado))
                            except Exception:
                                pass
                        break
            elif os.path.isdir(ruta_abs):
                archivo_encontrado = _buscar_resultado_en_carpeta(ruta_abs)
                if archivo_encontrado:
                    break
        
        if not archivo_encontrado and dir_motor:
            archivo_encontrado = _buscar_resultado_en_carpeta(dir_motor)
        
        if archivo_encontrado:
            return send_file(
                archivo_encontrado,
                as_attachment=True,
                download_name="resultado_diagramacion.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            # Mensaje con la ruta canónica (siempre diagramador_optimizado/resultado_diagramacion.xlsx)
            try:
                ruta_esperada = optimization_service.get_canonical_result_path()
                ruta_esperada_limpia = limpiar_mensaje_unicode(str(ruta_esperada))
                logger.error(f"Archivo no encontrado. Ruta esperada: {ruta_esperada_limpia}")
                mensaje_error = limpiar_mensaje_unicode(
                    f"El archivo de resultados no existe. Buscado en: {ruta_esperada_limpia}"
                )
            except Exception:
                ruta_esperada_limpia = "diagramador_optimizado/resultado_diagramacion.xlsx"
                mensaje_error = "El archivo de resultados no existe. Ubicacion esperada: diagramador_optimizado/resultado_diagramacion.xlsx"
            # Asegurar que también buscamos en la ruta fija de app (por si hay diferencia de normalización)
            if _result_path not in rutas_posibles:
                ruta_extra = os.path.abspath(_result_path)
                if os.path.exists(ruta_extra) and os.path.isfile(ruta_extra) and os.path.getsize(ruta_extra) > 0:
                    return send_file(
                        ruta_extra,
                        as_attachment=True,
                        download_name="resultado_diagramacion.xlsx",
                        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            # Si el navegador pidió HTML (ej. al hacer clic en Descargar), devolver página amigable
            if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "text/html":
                ruta_para_html = ruta_esperada_limpia.replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;")
                html = (
                    "<!DOCTYPE html><html><head><meta charset='utf-8'><title>Archivo no encontrado</title></head><body>"
                    "<h1>Archivo de resultados no encontrado</h1>"
                    "<p>Ejecuta primero una <strong>optimización</strong> desde el Diagramador y, cuando termine, usa de nuevo el botón Descargar.</p>"
                    "<p><strong>Ubicación esperada del archivo:</strong><br><code style='background:#f0f0f0;padding:4px 8px;word-break:break-all;'>" + ruta_para_html + "</code></p>"
                    "<p>Si ya ejecutaste la optimización y el archivo existe en esa ruta, reinicia la aplicación y vuelve a intentar descargar.</p>"
                    "<p><a href='/'>Volver al Diagramador</a></p>"
                    "</body></html>"
                )
                return make_response(html, 404)
            return jsonify({
                "success": False,
                "message": mensaje_error
            }), 404
    except Exception as e:
        # SOLUCION DEFINITIVA: Usar la función helper global para obtener el mensaje de error
        # Esta función convierte directamente a bytes y luego a ASCII, evitando str() que puede fallar
        # Obtener mensaje de error de forma segura
        error_str_limpio = "Error desconocido al descargar archivo"
        try:
            error_str = obtener_mensaje_error_seguro(e)
            # Aplicar limpieza adicional
            error_str_limpio = limpiar_mensaje_unicode(error_str)
        except Exception:
            error_str_limpio = "Error desconocido al descargar archivo"
        
        # Intentar loguear de forma segura
        # Asegurarse de que el mensaje esté completamente limpio antes de concatenar
        try:
            # Limpiar el mensaje una vez más antes de concatenar
            error_str_limpio_final = limpiar_mensaje_unicode(error_str_limpio)
            # Asegurarse de que sea ASCII
            try:
                error_str_limpio_final = error_str_limpio_final.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                error_str_limpio_final = "Error desconocido"
            
            # Construir mensaje de forma segura
            mensaje_log = "Error descargando Excel: " + error_str_limpio_final
            # Limpiar el mensaje completo también
            mensaje_log = limpiar_mensaje_unicode(mensaje_log)
            try:
                mensaje_log = mensaje_log.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                mensaje_log = "Error descargando Excel: [error no pudo ser procesado]"
            
            logger.error(mensaje_log)
        except Exception:
            try:
                logger.error("Error descargando Excel: [error no pudo ser logueado]")
            except Exception:
                pass
        
        # Intentar obtener traceback de forma segura
        try:
            import traceback
            traceback_str = traceback.format_exc()
            # Limpiar el traceback múltiples veces para asegurar
            traceback_limpio = limpiar_mensaje_unicode(traceback_str)
            # Limpiar una vez más
            try:
                traceback_limpio = traceback_limpio.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                traceback_limpio = "[traceback no pudo ser procesado]"
            logger.error(traceback_limpio)
        except Exception:
            pass
        
        # Crear el mensaje final de forma segura
        # Asegurarse de que el mensaje esté completamente limpio antes de devolverlo
        try:
            # Limpiar el mensaje una vez más antes de concatenar
            error_str_limpio_final = limpiar_mensaje_unicode(error_str_limpio)
            # Asegurarse de que sea ASCII
            try:
                error_str_limpio_final = error_str_limpio_final.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                error_str_limpio_final = "Error desconocido"
            
            # Construir mensaje de forma segura
            mensaje_error = "Error descargando archivo: " + error_str_limpio_final
            # Limpiar el mensaje completo también
            mensaje_error = limpiar_mensaje_unicode(mensaje_error)
            try:
                mensaje_error = mensaje_error.encode('ascii', errors='replace').decode('ascii')
            except Exception:
                mensaje_error = "Error descargando archivo"
        except Exception:
            mensaje_error = "Error descargando archivo"
        
        # Asegurarse de que el mensaje final esté completamente limpio antes de jsonify
        try:
            mensaje_error = limpiar_mensaje_unicode(mensaje_error)
            mensaje_error = mensaje_error.encode('ascii', errors='replace').decode('ascii')
        except Exception:
            mensaje_error = "Error descargando archivo"
        
        return jsonify({"success": False, "message": mensaje_error}), 500


@app.route("/descargar-template-excel")
def descargar_template_excel():
    """Descarga un template Excel para cargar viajes comerciales."""
    try:
        template_io = excel_trips_service.generate_trips_template()
        return send_file(
            template_io,
            as_attachment=True,
            download_name="template_viajes.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        logger.error(f"Error generando template Excel: {e}")
        mensaje_error = limpiar_mensaje_unicode(f"Error generando template: {str(e)}")
        return jsonify({"success": False, "message": mensaje_error}), 500


@app.route("/descargar-template-config")
def descargar_template_config():
    """Descarga un template Excel con la configuración actual."""
    try:
        template_io = excel_config_service.generate_config_template()
        return send_file(
            template_io,
            as_attachment=True,
            download_name="template_configuracion.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        try:
            error_str = obtener_mensaje_error_seguro(e)
            error_str = limpiar_mensaje_unicode(error_str)
            logger.error("Error generando template de configuracion: " + error_str)
        except Exception:
            logger.error("Error generando template de configuracion: [error no pudo ser procesado]")
        try:
            error_str = obtener_mensaje_error_seguro(e)
            error_str = limpiar_mensaje_unicode(error_str)
            mensaje_error = limpiar_mensaje_unicode("Error generando template: " + error_str)
        except Exception:
            mensaje_error = "Error generando template"
        return jsonify({"success": False, "message": mensaje_error}), 500


@app.route("/importar-configuracion", methods=["POST"])
def importar_configuracion():
    """Importa configuración desde un archivo Excel."""
    try:
        archivo = request.files.get("archivo")
        resultado = excel_config_service.process_config_upload(archivo)
        return jsonify(resultado)
    except Exception as e:
        try:
            error_str = obtener_mensaje_error_seguro(e)
            error_str = limpiar_mensaje_unicode(error_str)
            logger.error("Error importando configuracion: " + error_str)
        except Exception:
            logger.error("Error importando configuracion: [error no pudo ser procesado]")
        traceback.print_exc()
        try:
            error_str = obtener_mensaje_error_seguro(e)
            error_str = limpiar_mensaje_unicode(error_str)
            mensaje_error = limpiar_mensaje_unicode("Error importando configuracion: " + error_str)
        except Exception:
            mensaje_error = "Error importando configuracion"
        return jsonify({"success": False, "message": mensaje_error})


@app.route("/estado-archivo", methods=["GET"])
def estado_archivo():
    """Verifica si hay un archivo Excel cargado."""
    try:
        archivo_cargado = excel_trips_service.get_trips_file_path() is not None
        nombre_archivo = excel_trips_service.STANDARD_FILE_NAME if archivo_cargado else None
        
        response = jsonify({
            "archivo_cargado": archivo_cargado,
            "nombre_archivo": nombre_archivo
        })
        response.headers['Content-Type'] = 'application/json'
        return response
    except Exception as e:
        logger.error(f"Error en estado_archivo: {e}")
        traceback.print_exc()
        response = jsonify({
            "archivo_cargado": False,
            "nombre_archivo": None,
            "error": str(e)
        })
        response.headers['Content-Type'] = 'application/json'
        return response, 500


@app.route("/favicon.ico")
def favicon():
    """Ruta para el favicon - retorna 204 (No Content) para evitar errores 500."""
    return "", 204


if __name__ == "__main__":
    logger.info("Iniciando aplicación web del diagramador...")
    logger.info("Servicios inicializados:")
    logger.info(f"  - ConfigService: {config_service.config_path}")
    logger.info(f"  - ExcelTripsService: {getattr(excel_trips_service, '_trips_file_path', 'datos_salidas.xlsx')}")
    logger.info(f"  - OptimizationService: {optimization_service.get_result_file()}")
    
    # Detectar si estamos en un ejecutable compilado
    import sys
    is_frozen = getattr(sys, 'frozen', False)
    
    app.run(debug=not is_frozen, host="127.0.0.1", port=5000, use_reloader=False)

