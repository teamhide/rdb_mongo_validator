import pymysql
from pymysql.connections import Connection
from pymongo.collection import Collection
from typing import Union
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

    def get(self, query: Union[dict, str] = None, offset: int = None, limit: int = None) -> dict:
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

    def make_connection(self) -> Connection:
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            db=self.db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def get(self,
            connection: Union[Connection, Collection],
            query: Union[dict, str] = None,
            offset: int = None,
            limit: int = None) -> dict:
        cursor = connection.cursor()
        cursor.execute(f'{query} LIMIT {offset}, {limit}')
        return cursor.fetchall()

    def count(self, connection: Union[Connection, Collection], query: Union[dict, str] = None) -> int:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.rowcount
