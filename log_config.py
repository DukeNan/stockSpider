# -*- coding:utf-8 -*-
# @Time    : 2022/8/10 17:01 CST
# @Author  : shaun
# @GitHub  : https://github.com/DukeNan
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # 设置全局日志level，不设置默认WARN

formatter = logging.Formatter('%(asctime)s- %(levelname)s: %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

if __name__ == '__main__':
    pass
