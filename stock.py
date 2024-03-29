import json
import time
from datetime import datetime
from pathlib import Path

import tushare as ts

from clients import clients
from settings import API_TOKEN, PRODUCT


def get_ts_code_list(items, num=5):
    """分割指定长度的数组"""
    return [items[i:i + num] for i in range(0, len(items), num)]


class StockSpider:
    def __init__(self, token=None):
        if not token:
            token = API_TOKEN
        self.pro = ts.pro_api(token=token)
        self.conn = None

    def get_stock_basic(self):
        """获取上市公司基本信息"""
        df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        df.rename(index=str, columns={"symbol": "stock_id", 'name': 'stock_name'}, inplace=True)
        with clients.mysql_db.connect() as conn:
            df.to_sql('stock_basic', conn, if_exists='append', index=False)

    def get_stock_daily(self, ts_code, trade_date, stock_name_map):
        df = self.pro.daily(ts_code=ts_code, trade_date=trade_date)
        if df.empty:
            return df
        df['stock_id'] = df['ts_code'].apply(lambda x: x.split('.')[0])
        df['stock_name'] = df['ts_code'].apply(lambda x: stock_name_map[x].stock_name)
        return df

    def update_stock_daily(self, ts_date=datetime.now().date().strftime('%Y%m%d')):
        with clients.mysql_db.connect() as conn:
            self.conn = conn
            stock_list = self.conn.execute(
                'select id, ts_code, stock_id, stock_name from stock_basic order by id').fetchall()
            stock_name_map = {item.ts_code: item for item in stock_list}
            ts_code_list = get_ts_code_list(stock_list, num=100)
            total = len(ts_code_list)

            for index, items in enumerate(ts_code_list):
                ts_code = ','.join(entity.ts_code for entity in items)
                df = self.get_stock_daily(ts_code, ts_date, stock_name_map)
                if df.empty:
                    continue
                try:
                    df.to_sql('stock_daily_history', self.conn, if_exists='append', index=False)
                except Exception as e:
                    # print(e)
                    continue
                print('------------更新进度：{:.2f}%------------------'.format((index + 1) / total * 100))


def update_time(is_product=PRODUCT):
    """
    代码仓库每隔三十天自动提交一次，防止GitHub actions超时自动取消
    """
    if not is_product:
        return 'No changes'
    path = Path(__file__).resolve().parent.joinpath('update_time.json')
    now = time.time()
    data_json = json.dumps({'update_time': now})

    if path.exists():
        with path.open() as f:
            index_date = json.loads(f.read())
            if index_date['update_time'] + 86400 * 30 > now:
                return

    path.unlink(missing_ok=True)
    with path.open("w+") as f:
        f.write(data_json)

    return 'update github'


if __name__ == '__main__':
    spider = StockSpider()
    spider.update_stock_daily()
    update_time()
