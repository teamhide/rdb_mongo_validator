from typing import List, Union
import pymongo
from pymongo.collection import Collection
from pymysql.connections import Connection
from engine import Engine, EnginePool


class MongoDB(Engine):
    def __init__(self, host: str, db: str, collection: str):
        self.host = host
        self.conn = pymongo.MongoClient(self.host)
        self.db = self.conn.get_database(db)
        self.collection = self.db.get_collection(collection)

    def get(self, query: Union[dict, str] = None, offset: int = None, limit: int = None) -> List:
        if query:
            data = self.collection.find(**query)
        else:
            data = self.collection.find()
        if offset:
            data.skip(offset)
        if limit:
            data.limit(limit)
        return data

    def count(self, query: Union[dict, str] = None) -> int:
        return self.collection.estimated_document_count()


class MongoDBPool(EnginePool):
    def __init__(self, host: str, db: str, collection: str):
        self.host = host
        self.conn = pymongo.MongoClient(host)
        self.db = self.conn.get_database(db)
        self.collection = self.db.get_collection(collection)

    def make_connection(self) -> Collection:
        conn = pymongo.MongoClient(self.host)
        db = conn.get_database(self.db)
        return db.get_collection(self.collection)

    def get(self,
            connection: Union[Connection, Collection],
            query: Union[dict, str] = None,
            offset: int = None,
            limit: int = None) -> List:
        if query:
            data = connection.find(**query)
        else:
            data = connection.find()
        if offset:
            data.skip(offset)
        if limit:
            data.limit(limit)
        return data

    def count(self, connection: Union[Connection, Collection], query: Union[dict, str] = None) -> int:
        return connection.estimated_document_count()
