import abc
import pymysql
import pymongo
from pymysql.connections import Connection
from pymongo.collection import Collection


class RDB:
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

    def get(self, query: str, offset: int, limit: int):
        self.cursor.execute('{} LIMIT {}, {}'.format(query, offset, limit))
        return self.cursor.fetchall()

    def count(self, query: str) -> int:
        self.cursor.execute(query)
        return self.cursor.rowcount


class RDBPool:
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

    @staticmethod
    def get(connection: Connection, query: str, offset: int, limit: int):
        cursor = connection.cursor()
        cursor.execute('{} LIMIT {}, {}'.format(query, offset, limit))
        return cursor.fetchall()

    @staticmethod
    def count(connection: Connection, query: str) -> int:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.rowcount


class NoSQLEngine:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, offset: int = None, limit: int = None):
        pass

    @abc.abstractmethod
    def count(self) -> int:
        pass


class MongoDB(NoSQLEngine):
    def __init__(self, host: str, db: str, collection: str):
        self.conn = pymongo.MongoClient(host)
        self.db = self.conn.get_database(db)
        self.collection = self.db.get_collection(collection)

    def get(self, offset: int = None, limit: int = None):
        return self.collection.find().skip(offset).limit(limit)

    def count(self) -> int:
        return self.collection.count()


class MongoDBPool(NoSQLEngine):
    def __init__(self, host: str, db: str, collection: str):
        self.host = host
        self.db = db
        self.collection = collection

    def make_connection(self) -> Collection:
        conn = pymongo.MongoClient(self.host)
        db = conn.get_database(self.db)
        return db.get_collection(self.collection)

    def get(self, offset: int = None, limit: int = None):
        return collection.find().skip(offset).limit(limit)

    @staticmethod
    def count(collection: Collection) -> int:
        return collection.estimated_document_count()
