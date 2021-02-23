import time
from datetime import datetime

import tushare as ts

from clients import clients
from settings import API_TOKEN


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

    def get_init_stock_daily(self, ts_code, stock_name_map):
        df = self.pro.daily(ts_code=ts_code, start_date='20180101', end_date='20210221')
        for ts_code, group in df.groupby('ts_code'):
            stock = stock_name_map[ts_code]
            group.sort_values(by='trade_date', inplace=True)
            group['stock_id'] = ts_code.split('.')[0]
            group['stock_name'] = stock.stock_name

            info = '--------------id:{}, stock_id:{}, stock_name:{}, 百分比:{:.2f}%-----------'. \
                format(stock.id,
                       stock.stock_id,
                       stock.stock_name,
                       (stock.id / 4179) * 100)
            print(info)

            group.to_sql('stock_daily', self.conn, if_exists='append', index=False)

    def init_stock_daily(self):
        with clients.mysql_db.connect() as conn:
            self.conn = conn
            stock_list = self.conn.execute(
                'select id, ts_code, stock_id, stock_name from stock_basic where id >=3608 order by stock_id').fetchall()
            stock_name_map = {item.ts_code: item for item in stock_list}

            ts_code_list = get_ts_code_list(stock_list)
            for items in ts_code_list:
                ts_code = ','.join(entity.ts_code for entity in items)
                self.get_init_stock_daily(ts_code, stock_name_map)
                time.sleep(5)

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
                    df.to_sql('stock_daily', self.conn, if_exists='append', index=False)
                except Exception as e:
                    # print(e)
                    continue
                print('------------更新进度：{:.2f}%------------------'.format((index + 1) / total * 100))


if __name__ == '__main__':
    spider = StockSpider()
    spider.update_stock_daily()
