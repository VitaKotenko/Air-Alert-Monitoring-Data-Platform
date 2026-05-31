import logging
import os


def setup_logging(log_file_name="pipeline.log"):
    log_file_path = os.path.join("logs", log_file_name)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
        filemode="w",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
