import logging
import os
import sys
from pathlib import Path

# Walk up one frame to the caller of setup_logging()


def setup_logging(log_file_name: str, sub_dir: str | None = None) -> tuple[Path, logging.Logger]:
    """
    Sets up a logger that logs messages in `LOG_DIR` to a file that is of the
    same name as the script that invokes this function.
    log_file_name: The name of the log file without extension.
    sub_dir: Optional sub-directory under output/logs in which to place the log
        file. When omitted, the log file is placed in a directory named after
        `log_file_name` (backward-compatible behavior). When provided, the file
        is written to output/logs/<sub_dir>/<log_file_name>.log.
    """
    
    LOG_BASE_DIR: Path = Path(os.environ["RUN_DIR"]) / "output" / "logs"
    LOG_DIR: Path = LOG_BASE_DIR / sub_dir if sub_dir else LOG_BASE_DIR / f"{log_file_name}"
    LOG_FILE = LOG_DIR / f"{log_file_name}.log"

    # Create a logger
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)

    # Create the directory if it doesn't exist
    os.makedirs(LOG_DIR, exist_ok=True)

    # Create a file handler that logs messages to a file
    # encoding should be utf-8 and the file should be in a+ mode

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a+")
    file_handler.setLevel(logging.INFO)

    # Create a console handler that logs messages to the console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Create a formatter and set it for both handlers
    formatter = logging.Formatter('%(asctime)s %(name)s:%(levelname)s:%(message)s', 
                                  datefmt="%Y-%m-%d %A %H:%M:%S")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return LOG_BASE_DIR, logger