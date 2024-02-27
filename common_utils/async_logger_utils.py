# coding: utf8
"""
异步日志
"""
import logging
from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.handlers.files import AsyncTimedRotatingFileHandler
from pathlib import Path
import os

# 获取当前文件路径
current_path = os.path.abspath(__file__)
# 获取上级目录
parent_path = os.path.dirname(current_path)
# 获取上上级目录
file_path = os.path.dirname(parent_path)

LOG_FILE_PATH = "{}/log/%s.log".format(file_path)


class AsyncLogConfig:
    def __init__(self, log_file=LOG_FILE_PATH, log_name="run"):
        """异步日志，按日期切割"""
        log_file = log_file % log_name
        logger = Logger(name=log_name, level=logging.DEBUG)
        logfile_path = Path(log_file)
        logfile_path.parent.mkdir(parents=True, exist_ok=True)
        logfile_path.touch(exist_ok=True)
        atr_file_handler = AsyncTimedRotatingFileHandler(
            filename=str(logfile_path),
            interval=1,
            backup_count=5,
            encoding="utf8"
        )
        formatter = Formatter(
            "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s")
        atr_file_handler.formatter = formatter
        logger.add_handler(atr_file_handler)
        self.logger = logger


async_logger = AsyncLogConfig().logger
