import abc
from typing import List, Union
from pymysql.connections import Connection
from pymongo.collection import Collection


class Engine:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, query: Union[dict, str] = None, offset: int = None, limit: int = None) -> List:
        pass

    @abc.abstractmethod
    def count(self, query: Union[dict, str] = None) -> int:
        pass


class EnginePool:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self,
            connection: Union[Connection, Collection],
            query: Union[dict, str] = None,
            offset: int = None,
            limit: int = None) -> List:
        pass

    @abc.abstractmethod
    def count(self, connection: Union[Connection, Collection], query: Union[dict, str] = None) -> int:
        pass
