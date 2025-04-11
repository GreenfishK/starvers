import logging
import os
from logging.config import dictConfig

LOG_DIR = "./logs"
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

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

def get_tracking_logger(repository_name: str) -> logging.Logger:
    logger = logging.getLogger(f"tracking_task_{repository_name}")
    if not logger.handlers:
        # Create file handler only once per task
        fh = logging.FileHandler(f"./logs/tracking_{repository_name}.log")
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
        fh.setFormatter(formatter)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
        logger.setLevel(logging.INFO)
    return logger