# coding: utf8
"""
同步日志
"""
import logging
import logging.handlers
import os

# 获取当前文件路径
current_path = os.path.abspath(__file__)
# 获取上级目录
parent_path = os.path.dirname(current_path)
# 获取上上级目录
file_path = os.path.dirname(parent_path)

LOG_FILE_PATH = "{}/log/%s.log".format(file_path)


def log_config(log_file=LOG_FILE_PATH, log_name="run", log_level=logging.DEBUG, max_bytes=1000 * 1024 * 1024,
               backup_count=10):
    logging.basicConfig()
    handler = logging.handlers.RotatingFileHandler(r"{}".format(log_file), maxBytes=max_bytes, backupCount=backup_count)
    formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger(log_name)
    logger.addHandler(handler)
    logger.setLevel(log_level)
    return logger


logger = log_config()
