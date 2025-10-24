import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

os.makedirs('log', exist_ok=True)

file_handler = TimedRotatingFileHandler(
    filename=f'log/ingestion_{datetime.now().strftime("%Y-%m-%d")}.log',
    when='midnight',
    interval=1,
    backupCount=30
)
file_handler.suffix = "%Y-%m-%d"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        file_handler,
        logging.StreamHandler()
    ]
)

def get_logger(name):
    return logging.getLogger(name)

