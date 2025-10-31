import logging
import os
from logging.config import dictConfig

LOG_DIR = "/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging():
    dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
            },
        },
        "handlers": {
            "default": {
                "class": "logging.FileHandler",
                "filename": os.path.join(LOG_DIR, "starvers.log"),
                "formatter": "default",
                "level": "INFO",
            },
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": "INFO",
            }
        },
        "root": {
            "handlers": ["default", "console"],
            "level": "INFO",
        },
        "loggers": {
            "uvicorn.error": {
                "level": "INFO",
                "handlers": ["default", "console"],
                "propagate": False
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["default", "console"],
                "propagate": False
            },
        },
    })

def get_logger(name: str, log_file_name: str = None) -> logging.Logger:
    if log_file_name is None:
        return logging.getLogger(name)

    logger = logging.getLogger(name)
    if not logger.handlers:
        fh = logging.FileHandler(f"/data/logs/{log_file_name}")
        formatter = logging.Formatter(f"[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
        fh.setFormatter(formatter)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
        logger.setLevel(logging.INFO)
    return logger