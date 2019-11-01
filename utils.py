import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_logger(log_path, max_bytes=100000, backup_count=10):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s: %(levelname)s: %(message)s",
                        datefmt="%d/%m/%Y %I:%M:%S %p",
                        handlers=[logging.StreamHandler(),
                                  RotatingFileHandler(Path(log_path),
                                                      maxBytes=max_bytes,
                                                      backupCount=backup_count)])
    return logging.getLogger()
