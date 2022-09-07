# -*- coding:utf-8 -*-
# @Time    : 2022/8/15 23:18 CST
# @Author  : shaun
# @GitHub  : https://github.com/DukeNan
# 板块详情 (启明星暂无详情数据)
# 接口数据较为完备，后期考虑拓展
import time

import akshare as ak
from sqlalchemy import text

from clients import clients
from log_config import logger


def get_sector_list(conn):
    # 启明星暂无详情数据
    querySet = conn.execute("select id, sector_label, sector_name, type_label "
                            "from sector_spot where type_label !='qmx' order by id ").fetchall()
    return querySet


def get_df(sector):
    stock_sector_detail_df = ak.stock_sector_detail(sector)
    stock_sector_detail_df = stock_sector_detail_df.loc[:, ["code"]]
    return stock_sector_detail_df


def update_stock_basic(conn, type_label: str, sector_label: str, stock_ids: list):
    sql = text(f"update stock_basic set {type_label}=:sector_label where stock_id in :stock_ids")
    result = conn.execute(sql, sector_label=sector_label, stock_ids=stock_ids)
    return result


def run():
    with clients.mysql_db.connect() as conn:
        sector_list = get_sector_list(conn)
        total = len(sector_list)
        num = 0
        for sector in sector_list:
            num += 1
            logger.info(f"==========开始更新：{sector.sector_name}, {sector.sector_label} ===============")
            df = get_df(sector.sector_label)
            if df.empty:
                continue
            stock_ids = [row.code for _, row in df.iterrows()]
            type_label = sector.type_label + '_label'
            result = update_stock_basic(conn, type_label, sector.sector_label, stock_ids)
            logger.info(f"progress: {num * 100 / total:.2f}%,total:{result.rowcount}, data:{dict(sector)}")
            time.sleep(2)


if __name__ == '__main__':
    run()


