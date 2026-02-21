import logging
import os
import sys
from dotenv import load_dotenv
load_dotenv()

def getlogger(name: str):
    logger = logging.getLogger(name)
    logger.propagate = False
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(numeric_level)

    if not logger.handlers:
        sh = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger