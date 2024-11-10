import logging
import tomllib
from pathlib import Path
# Configuring the main Logger
def load_logging_config():
    with open("pyproject.toml","rb") as file:
        config = tomllib.load(file)
    return config.get("tool",{},).get("logging", {})

def setup_logger(logger_name):
    logger = logging.getLogger(logger_name)
    level = log_levels.get(f"{logger_name}", "INFO")
    logger.setLevel(getattr(logging, level, logging.INFO))

    if logger_name in main_loggers:
        logger.addHandler(main_file_handler)

        if console_logging:
            logger.addHandler(console_handler)
    log_file_path = Path(f"Logs/{logger_name}.log")
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    return logger

logging_config = load_logging_config()
main_loggers = logging_config.get("main_loggers",[])
log_levels: dict = {setting[:setting.find("_level")]: value.upper() for setting, value in logging_config.items() if "_level" in setting}
console_logging = logging_config.get("console_logging", False)

if console_logging:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

#Setup the main file handler
main_log_file = Path("Logs/main.log")
main_file_handler =  logging.FileHandler(main_log_file)
main_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
loggers = {name: setup_logger(name) for name in log_levels.keys()}


