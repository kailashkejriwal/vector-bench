import logging
import os
import pathlib
import socket
import subprocess
import traceback

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
    import sys
    venv_dir = pathlib.Path(sys.executable).parent.parent
    streamlit_path = venv_dir / "bin" / "streamlit"
    preferred = int(os.environ.get("VDB_STREAMLIT_PORT", "8509"))
    port = _first_free_port(preferred)
    if port != preferred:
        log.warning("Streamlit port %s in use; using %s (set VDB_STREAMLIT_PORT to force)", preferred, port)
    cmd = [
        str(streamlit_path),
        "run",
        f"{pathlib.Path(__file__).parent}/frontend/vdbbench.py",
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
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        log.info("exit streamlit...")
    except Exception as e:
        log.warning(f"exit, err={e}\nstack trace={traceback.format_exc(chain=True)}")


if __name__ == "__main__":
    main()
