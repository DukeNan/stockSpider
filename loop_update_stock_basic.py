import json
import time
from concurrent.futures import ThreadPoolExecutor

import tushare as ts
from sqlalchemy import text

from clients import clients
from settings import API_TOKEN

ts.set_token(API_TOKEN)
pro = ts.pro_api()


def update_or_insert(data):
    update_sql = "update stock_basic set ts_code=:ts_code, stock_name=:stock_name, area=:area, " \
                 "industry=:industry, list_date=:list_date where stock_id=:stock_id"

    insert_sql = "insert into stock_basic(ts_code, stock_id, stock_name, area, industry, list_date) " \
                 "values (:ts_code, :stock_id, :stock_name, :area, :industry, :list_date)"
    with clients.mysql_db.connect() as conn:
        result = conn.execute(text(update_sql), **data)
        stock_id = data.get('stock_id')
        if result.rowcount > 0:
            return '[update | {}]: {}'.format(stock_id, json.dumps(data, ensure_ascii=False))
        conn.execute(text(insert_sql), **data)
    return '[update | {}]: {}'.format(stock_id, json.dumps(data, ensure_ascii=False))


def run():
    begin = time.time()
    print('========begin: {}'.format(begin))
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    with ThreadPoolExecutor(10) as executor:
        for item in df.to_dict('records'):
            data = {
                'ts_code': item['ts_code'],
                'stock_id': item['symbol'],
                'stock_name': item['name'],
                'area': item['area'],
                'industry': item['industry'],
                'list_date': item['list_date']
            }
            future = executor.submit(update_or_insert, data)
            print(future.result())
    end = time.time()
    print('========end: {}'.format(begin))
    print('========cost_time: {}s'.format(end - begin))


if __name__ == '__main__':
    run()
