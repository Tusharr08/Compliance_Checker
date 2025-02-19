import logging

def setup_logger(name="compliance_logger", log_file="compliance.log"):
    #Set up logger to log messages to a file.

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()

logger.info("Starting Compliance Check...")
logger.error("Import order violation in notebook abc.json")