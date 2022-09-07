# -*- coding:utf-8 -*-
# @Time    : 2022/8/11 22:29 CST
# @Author  : shaun
# @GitHub  : https://github.com/DukeNan
# 跟新stock_basic表信息
import sys

sys.path.append('..')

import pandas as pd
import tushare as ts

from clients import clients
from log_config import logger
from settings import API_TOKEN


def run():
    logger.info("==============更新：stock_basic================")
    ts_pro = ts.pro_api(API_TOKEN)
    df = ts_pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    df.rename(index=str, columns={"symbol": "stock_id", 'name': 'stock_name'}, inplace=True)

    with clients.mysql_db.connect() as conn:
        sql = "select stock_id from stock_basic"
        basic_df = pd.read_sql(sql, conn)
        df = df.append(basic_df, ignore_index=True)
        df.drop_duplicates(subset=["stock_id"], keep=False, inplace=True)
        df.dropna(subset=['ts_code'], axis=0, inplace=True)
        df.to_sql("stock_basic", conn, if_exists='append', index=False)

    for _, ser in df.iterrows():
        logger.info(ser.to_json(date_format='ISO8601', force_ascii=False))

    logger.info("===================更新完成===================")


if __name__ == '__main__':
    run()
