# -*- coding:utf-8 -*-
# @Time    : 2022/8/10 17:00 CST
# @Author  : shaun
# @GitHub  : https://github.com/DukeNan
import sys

sys.path.append("..")
import gc
import time
from datetime import timedelta, datetime

import pandas as pd
import tushare as ts
from sqlalchemy import text

from clients import clients
from log_config import logger
from settings import API_TOKEN


class UpdateDbServer(object):
    def __init__(self):
        self.db_conn = None
        self.mongo_conn = None
        self.ts_pro = ts.pro_api(API_TOKEN)
        self.stock_num = 0

    def close(self):
        if self.db_conn:
            self.db_conn.close()
        if self.mongo_conn:
            self.mongo_conn.close()
        gc.collect()

    def db_init(self):
        self.db_conn = clients.mysql_db.connect()

    def arr_splits(self, item, n):
        return [item[i:i + n] for i in range(0, len(item), n)]

    def get_ts_code_list(self, n=100):
        results = self.db_conn.execute("select ts_code from stock_basic").fetchall()
        self.stock_num = len(results)
        results = list(map(lambda x: x[0], results))
        ts_code_list = self.arr_splits(results, n)
        return [','.join(item) for item in ts_code_list]

    def update_db(self, data: dict):
        sql = 'update stock_daily_history set ma_5=:ma_5, ma_10=:ma_10, ma_20=:ma_20 ' \
              'where stock_id=:stock_id and trade_date=:trade_date'
        self.db_conn.execute(text(sql), data)

    def get_df(self, ts_code, start_date='20220801', end_date='20220812') -> pd.DataFrame:
        df = ts.pro_bar(ts_code=ts_code, api=self.ts_pro, start_date=start_date, end_date=end_date, ma=[5, 10, 20])
        df.rename(columns={'ma5': 'ma_5', 'ma10': 'ma_10', 'ma20': 'ma_20'}, inplace=True)
        df["stock_id"] = df["ts_code"].apply(lambda x: x.split('.')[0])
        df.drop(["ma_v_5", "ma_v_10", "ma_v_20"], axis=1, inplace=True)
        return df

    def get_trade_cal(self, today):
        """获取交易日"""
        format_str = '%Y%m%d'
        start_date = today - timedelta(days=60)
        df = self.ts_pro.trade_cal(exchange='',
                                   start_date=start_date.strftime(format_str),
                                   end_date=today.strftime(format_str))
        df = df[df["is_open"] == 1].tail(20)
        df.sort_values('cal_date', ascending=True, inplace=True)
        ser = df['cal_date'].apply(lambda x: datetime.strptime(x, format_str).date())
        return ser.values[0], ser.values[-1]

    def update_ma(self):
        """更新stock_daily_history均线"""
        logger.info("======开始更新：stock_daily_history===========")
        update_sql = text("UPDATE stock_daily_history as t1, stock_daily as t2 "
                          "set t1.ma_5=t2.ma_5, t1.ma_10=t2.ma_10, t1.ma_20=t2.ma_20 "
                          "WHERE t1.stock_id=t2.stock_id and t1.trade_date=t2.trade_date")
        self.db_conn.execute(update_sql)
        logger.info("======更新结束：stock_daily_history===========")

    def update_stock_daily(self, today):
        """更新当天的stock_daily"""
        if self.db_conn(text("select count(1) from stock_daily_history where trade_date =:trade_date")).scalar() == 0:
            return
        sql = text("select 1 from stock_daily where trade_date =:trade_date limit 1")
        if self.db_conn.execute(sql, trade_date=today).scalar():
            return
        # 清空表数据
        self.db_conn.execute("truncate stock_daily")
        num = 0
        start_date, end_date = map(lambda x: x.strftime('%Y%m%d'), self.get_trade_cal(today))
        for ts_code in self.get_ts_code_list(100):
            num += 1
            df = self.get_df(ts_code, start_date, end_date)
            df = df[df["trade_date"] == today.strftime('%Y%m%d')]
            df.to_sql("stock_daily", self.db_conn, if_exists="append", index=False)
            process = min(10000 * num / 4667, 100)
            logger.info(f"=======进度：{process:.2f}%=========")
            time.sleep(5)
        # 跟新字段stock_name
        logger.info("=======更新stock_daily字段：stock_name和行业==========")
        update_sql = text("update stock_daily as t1 , stock_basic as t2 \
                            set t1.stock_name=t2.stock_name, \
                            t1.new_label=t2.new_label, \
	                        t1.gn_label=t2.gn_label, \
	                        t1.dy_label=t2.dy_label, \
	                        t1.hangye_label=t2.hangye_label \
                            WHERE t1.stock_id=t2.stock_id")
        self.db_conn.execute(update_sql)

    def run(self):
        today = datetime.now().date()
        # today = datetime(2022, 9, 6).date()
        self.db_init()
        self.update_stock_daily(today)
        self.update_ma()
        self.close()


if __name__ == '__main__':
    stock = UpdateDbServer()
    stock.run()
