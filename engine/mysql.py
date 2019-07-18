import pymysql
from pymysql.connections import Connection
from pymongo.collection import Collection
from typing import List, Union
from engine import Engine, EnginePool


class MySQL(Engine):
    def __init__(self, host: str, user: str, passwd: str, db: str, port: int):
        self.db = pymysql.connect(
            host,
            user=user,
            passwd=passwd,
            db=db,
            port=port,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.db.cursor()

    def get(self, query: Union[dict, str] = None, offset: int = None, limit: int = None) -> List:
        self.cursor.execute(f'{query} LIMIT {offset}, {limit}')
        return self.cursor.fetchall()

    def count(self, connection: pymysql.Connection = None, query: Union[dict, str] = None) -> int:
        self.cursor.execute(query)
        return self.cursor.rowcount


class MySQLPool(EnginePool):
    def __init__(self, host: str, user: str, passwd: str, db: str, port: int):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port

    def get(self,
            connection: Union[Connection, Collection],
            query: Union[dict, str] = None,
            offset: int = None,
            limit: int = None) -> List:
        cursor = connection.cursor()
        cursor.execute(f'{query} LIMIT {offset}, {limit}')
        return cursor.fetchall()

    def count(self, connection: Union[Connection, Collection], query: Union[dict, str] = None) -> int:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.rowcount
