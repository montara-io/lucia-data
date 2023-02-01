import os

from common.logger import get_logger

logger = get_logger()
def get_mode():
    mode = os.getenv('MODE')
    if not mode:
        logger.info("MODE not set, defaulting to development")
        mode = 'development'
    else:
        logger.info(f"MODE set to {mode}")

    return mode
