"""
  中间键客户端调用入口
"""

from cached_property import threaded_cached_property as lazy_property

import settings


class Clients:
    @lazy_property
    def mysql_db(self):
        from sqlalchemy import create_engine
        return create_engine(settings.MYSQL_DB, **settings.SQLALCHEMY_ENGINE_CONFIG)


clients = Clients()

if __name__ == '__main__':
    pass
