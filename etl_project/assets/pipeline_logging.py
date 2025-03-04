import logging
import time
from pathlib import Path


class PipelineLogging:
    def __init__(self, pipeline_name: str, log_folder_path: str):
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(self.pipeline_name)
        logger.setLevel(logging.INFO)
        self.file_path = (
            Path(self.log_folder_path) / f"{self.pipeline_name}_{time.time()}.log"
        )
        file_handler = logging.FileHandler(self.file_path)
        file_handler.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger

    def get_logs(self):
        with open(self.file_path, "r") as file:
            return file.read()
