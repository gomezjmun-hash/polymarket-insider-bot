"""Configuracion de logging con colores."""
import logging
import sys
import io

import colorlog


def setup_logging(level: str = "INFO") -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)

    # Fuerza UTF-8 en la consola de Windows
    if sys.platform == "win32":
        try:
            sys.stdout = io.TextIOWrapper(
                sys.stdout.buffer, encoding="utf-8", errors="replace"
            )
            sys.stderr = io.TextIOWrapper(
                sys.stderr.buffer, encoding="utf-8", errors="replace"
            )
        except AttributeError:
            pass

    handler = colorlog.StreamHandler(sys.stdout)
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s]%(reset)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        },
    ))

    root = logging.getLogger()
    root.setLevel(numeric)
    root.handlers.clear()
    root.addHandler(handler)

    # File handler opcional: solo si LOG_FILE está definido (no en Railway/Docker)
    import os
    log_file = os.getenv("LOG_FILE", "")
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        root.addHandler(file_handler)

    # Silencia librerías ruidosas
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
