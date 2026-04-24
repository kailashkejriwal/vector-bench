import sys
import logging
import os
import pathlib
import socket
import subprocess
import traceback

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from . import config

log = logging.getLogger("vectordb_bench")


def main():
    log.info(f"all configs: {config().display()}")
    run_streamlit()


def _first_free_port(preferred: int, span: int = 20) -> int:
    """Bind test so Streamlit does not fail immediately when the preferred port is taken."""
    for port in range(preferred, preferred + span):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return port
            except OSError:
                continue
    return preferred


def run_streamlit():
    project_root = pathlib.Path(__file__).resolve().parents[1]
    app_file = pathlib.Path(__file__).parent / "frontend" / "vdbbench.py"
    preferred = int(os.environ.get("VDB_STREAMLIT_PORT", "8509"))
    port = _first_free_port(preferred)
    if port != preferred:
        log.warning("Streamlit port %s in use; using %s (set VDB_STREAMLIT_PORT to force)", preferred, port)
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{project_root}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(project_root)
    )
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_file),
        "--server.address",
        "0.0.0.0",
        "--server.port",
        str(port),
        "--logger.level",
        "info",
        "--theme.base",
        "light",
        "--theme.primaryColor",
        "#3670F2",
        "--theme.secondaryBackgroundColor",
        "#F0F2F6",
    ]
    log.debug(f"cmd: {cmd}")
    try:
        subprocess.run(cmd, check=True, env=env, cwd=str(project_root))
    except KeyboardInterrupt:
        log.info("exit streamlit...")
    except Exception as e:
        log.warning(f"exit, err={e}\nstack trace={traceback.format_exc(chain=True)}")


if __name__ == "__main__":
    main()
