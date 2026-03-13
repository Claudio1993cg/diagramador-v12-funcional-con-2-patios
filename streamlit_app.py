from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import streamlit as st
from werkzeug.security import check_password_hash, generate_password_hash


BASE_DIR = Path(__file__).resolve().parent
MOTOR_DIR = BASE_DIR / "diagramador_optimizado"
CONFIG_PATH = MOTOR_DIR / "configuracion.json"
TRIPS_PATH = MOTOR_DIR / "datos_salidas.xlsx"
RESULT_PATH = MOTOR_DIR / "resultado_diagramacion.xlsx"
USERS_PATH = MOTOR_DIR / "usuarios_web.json"


def _load_users() -> Dict[str, Dict[str, Any]]:
    if not USERS_PATH.exists():
        return {}
    try:
        return json.loads(USERS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_users(users: Dict[str, Dict[str, Any]]) -> None:
    USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    USERS_PATH.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")


def _ensure_default_users() -> None:
    users = _load_users()
    changed = False
    defaults = {
        "ccantero": {"password": "Juacko1993", "is_admin": True},
        "dlopez": {"password": "Lopez2803", "is_admin": False},
    }
    for username, info in defaults.items():
        if username not in users:
            users[username] = {
                "password_hash": generate_password_hash(info["password"]),
                "is_admin": bool(info["is_admin"]),
                "created_at": datetime.now().isoformat(),
            }
            changed = True
    if changed:
        _save_users(users)


def _authenticate(username: str, password: str) -> bool:
    users = _load_users()
    user = users.get(username)
    if not user:
        return False
    return check_password_hash(user.get("password_hash", ""), password)


def _is_admin(username: str) -> bool:
    users = _load_users()
    return bool(users.get(username, {}).get("is_admin", False))


def _run_optimizer() -> tuple[int, str]:
    cmd = [sys.executable, "-m", "diagramador_optimizado.cli.main"]
    proc = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        errors="replace",
    )
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode, output


def _render_login() -> None:
    st.title("Ingreso al Diagramador")
    st.caption("Versión web para Streamlit Cloud")
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        submit = st.form_submit_button("Ingresar")
    if submit:
        if _authenticate(username.strip(), password):
            st.session_state["auth_user"] = username.strip()
            st.session_state["is_admin"] = _is_admin(username.strip())
            st.rerun()
        st.error("Usuario o contraseña inválidos.")


def _render_admin() -> None:
    st.subheader("Administración de usuarios")
    users = _load_users()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Crear usuario")
        with st.form("create_user_form"):
            new_user = st.text_input("Usuario nuevo")
            new_pass = st.text_input("Contraseña", type="password")
            new_admin = st.checkbox("Administrador", value=False)
            create = st.form_submit_button("Crear")
        if create:
            if len(new_user.strip()) < 3:
                st.error("El usuario debe tener al menos 3 caracteres.")
            elif len(new_pass.strip()) < 4:
                st.error("La contraseña debe tener al menos 4 caracteres.")
            elif new_user.strip() in users:
                st.error("Ese usuario ya existe.")
            else:
                users[new_user.strip()] = {
                    "password_hash": generate_password_hash(new_pass.strip()),
                    "is_admin": bool(new_admin),
                    "created_at": datetime.now().isoformat(),
                }
                _save_users(users)
                st.success(f"Usuario '{new_user.strip()}' creado.")
                st.rerun()

    with col2:
        st.markdown("#### Modificar / Eliminar")
        if not users:
            st.info("No hay usuarios.")
        else:
            selected = st.selectbox("Usuario", sorted(users.keys()))
            cur = users[selected]
            with st.form("edit_user_form"):
                edit_user = st.text_input("Nuevo usuario", value=selected)
                edit_pass = st.text_input("Nueva contraseña (opcional)", type="password")
                edit_admin = st.checkbox("Administrador", value=bool(cur.get("is_admin", False)))
                c1, c2 = st.columns(2)
                update = c1.form_submit_button("Guardar cambios")
                delete = c2.form_submit_button("Eliminar usuario")
            if update:
                if len(edit_user.strip()) < 3:
                    st.error("El usuario debe tener al menos 3 caracteres.")
                elif edit_user.strip() != selected and edit_user.strip() in users:
                    st.error("Ya existe un usuario con ese nombre.")
                else:
                    data = dict(cur)
                    data["is_admin"] = bool(edit_admin)
                    if edit_pass.strip():
                        if len(edit_pass.strip()) < 4:
                            st.error("La contraseña debe tener al menos 4 caracteres.")
                            st.stop()
                        data["password_hash"] = generate_password_hash(edit_pass.strip())
                    users.pop(selected, None)
                    users[edit_user.strip()] = data
                    _save_users(users)
                    if st.session_state.get("auth_user") == selected:
                        st.session_state["auth_user"] = edit_user.strip()
                        st.session_state["is_admin"] = bool(edit_admin)
                    st.success("Usuario actualizado.")
                    st.rerun()
            if delete:
                if selected == st.session_state.get("auth_user"):
                    st.error("No puedes eliminar tu usuario activo.")
                else:
                    if bool(cur.get("is_admin", False)):
                        admins = [u for u, d in users.items() if bool(d.get("is_admin", False))]
                        if len(admins) <= 1:
                            st.error("Debe existir al menos un administrador.")
                            st.stop()
                    users.pop(selected, None)
                    _save_users(users)
                    st.success(f"Usuario '{selected}' eliminado.")
                    st.rerun()

    st.markdown("#### Usuarios actuales")
    view_rows = []
    for uname, data in sorted(_load_users().items(), key=lambda x: x[0].lower()):
        view_rows.append(
            {
                "usuario": uname,
                "admin": "Sí" if bool(data.get("is_admin", False)) else "No",
                "creado": str(data.get("created_at", "")),
            }
        )
    st.dataframe(view_rows, use_container_width=True, hide_index=True)


def _render_app() -> None:
    st.title("Diagramador - Web (Streamlit)")
    st.caption(f"Usuario: {st.session_state.get('auth_user')}")

    if st.button("Cerrar sesión"):
        st.session_state.pop("auth_user", None)
        st.session_state.pop("is_admin", None)
        st.rerun()

    tab1, tab2, tab3 = st.tabs(["Carga y Ejecución", "Configuración JSON", "Admin Usuarios"])

    with tab1:
        st.subheader("Archivos de entrada")
        up_excel = st.file_uploader("Subir datos_salidas.xlsx", type=["xlsx", "xls"])
        if up_excel is not None:
            TRIPS_PATH.write_bytes(up_excel.getbuffer())
            st.success(f"Archivo guardado en: {TRIPS_PATH.name}")

        if st.button("Ejecutar optimización", type="primary"):
            code, output = _run_optimizer()
            st.text_area("Log de ejecución", output, height=320)
            if code == 0 and RESULT_PATH.exists():
                st.success("Optimización completada correctamente.")
            else:
                st.error(f"La ejecución terminó con código {code}.")

        if RESULT_PATH.exists():
            st.download_button(
                label="Descargar resultado_diagramacion.xlsx",
                data=RESULT_PATH.read_bytes(),
                file_name="resultado_diagramacion.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("Aún no existe resultado generado.")

    with tab2:
        st.subheader("Editar configuración (JSON)")
        if not CONFIG_PATH.exists():
            st.error(f"No se encontró {CONFIG_PATH}")
        else:
            raw = CONFIG_PATH.read_text(encoding="utf-8")
            edited = st.text_area("configuracion.json", raw, height=450)
            c1, c2 = st.columns(2)
            if c1.button("Guardar configuración JSON"):
                try:
                    parsed = json.loads(edited)
                    CONFIG_PATH.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
                    st.success("Configuración guardada.")
                except Exception as e:
                    st.error(f"JSON inválido: {e}")
            if c2.button("Recargar desde disco"):
                st.rerun()

    with tab3:
        if st.session_state.get("is_admin"):
            _render_admin()
        else:
            st.warning("Solo administradores pueden gestionar usuarios.")


def main() -> None:
    st.set_page_config(page_title="Diagramador Streamlit", layout="wide")
    _ensure_default_users()
    if "auth_user" not in st.session_state:
        _render_login()
    else:
        _render_app()


if __name__ == "__main__":
    main()

