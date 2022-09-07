# -*- coding:utf-8 -*-
# @Time    : 2022/7/14 19:59 CST
# @Author  : shaun
# @GitHub  : https://github.com/DukeNan
"""
关于股票板块信息
地址：
    新浪行业  ：http://finance.sina.com.cn/stock/sl/#sinaindustry_1
    概念     ：http://finance.sina.com.cn/stock/sl/#concept_1
    地域     ：http://finance.sina.com.cn/stock/sl/#area_1
    行业     ：http://finance.sina.com.cn/stock/sl/#industry_1
    启明星行业：http://finance.sina.com.cn/stock/sl/#qmxindustry_1
"""
import sys

sys.path.append("..")

from datetime import datetime

import akshare as ak
import pandas as pd
from sqlalchemy import text

from clients import clients
from log_config import logger


def update_stock_sector():
    now = datetime.now()
    today = now.date()
    sector_map = {
        '新浪行业': 'new',
        '概念': 'gn',
        '地域': 'dy',
        '行业': 'hangye',
        '启明星行业': 'qmx',
    }
    columns_map = {
        '板块': 'sector_name',
        'label': 'sector_label',
        '平均价格': 'avg_price',
        '公司家数': 'stock_num',
        '总成交量': 'vol',
        '总成交额': 'amount',
        '涨跌幅': 'pct_chg',
        '涨跌额': 'change',
        "个股-涨跌幅": "stock_pct_chg",
        "个股-当前价": "stock_price",
        "个股-涨跌额": "stock_change",
        "股票代码": "stock_id",
        "股票名称": "stock_name",
    }
    # 获取数据
    df_list = []
    for sector, type_label in sector_map.items():
        df = ak.stock_sector_spot(sector)
        df = df.rename(columns=columns_map)
        df['trade_date'] = datetime.now().date()
        df['type_label'] = type_label
        df['stock_id'] = df['stock_id'].str.extract(r'(\d+)')
        df_list.append(df)
        logger.info("=========更新板块：{}==========".format(sector))

    new_df = pd.concat(df_list, axis=0, ignore_index=True)

    # 更新数据库
    with clients.mysql_db.connect() as conn:
        sql_count = text("select count(1) from sector_spot_history where trade_date =:trade_date limit 1")
        flag = conn.execute(sql_count, trade_date=today).scalar()
        conn.execute(text("truncate sector_spot"))
        logger.info("========更新：sector_spot==========")
        new_df.to_sql("sector_spot", conn, if_exists="append", index=False)

        if not flag and now.hour > 15:
            logger.info("========更新：sector_spot_history==========")
            new_df.to_sql("sector_spot_history", conn, if_exists="append", index=False)


if __name__ == '__main__':
    update_stock_sector()
