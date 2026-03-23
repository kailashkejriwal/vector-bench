import sys
import logging
import pathlib
import subprocess
import traceback

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from . import config

log = logging.getLogger("vectordb_bench")


def main():
    log.info(f"all configs: {config().display()}")
    run_streamlit()


def run_streamlit():
    import sys
    venv_dir = pathlib.Path(sys.executable).parent.parent
    streamlit_path = venv_dir / "bin" / "streamlit"
    cmd = [
        str(streamlit_path),
        "run",
        f"{pathlib.Path(__file__).parent}/frontend/vdbbench.py",
	"--server.address",
	"0.0.0.0",
	"--server.port",
	"8501",
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
