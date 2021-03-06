from typing import Optional, NoReturn, Union
import os
from multiprocessing import Pool
from collections import namedtuple
from datetime import datetime
import logging
from pprint import pprint
from config import RDBConfig, MongoConfig, check_time, MYSQL_QUERY
from engine.mysql import MySQLPool
from engine.mongodb import MongoDBPool


class Validate:
    def __init__(self, rdb: MySQLPool, mongo: MongoDBPool, rdb_query: str):
        self.rdb = rdb
        self.mongo = mongo
        self.rdb_query = rdb_query
        self.increase_count = 10000
        logging.basicConfig(filename='validation_result.log', level=logging.DEBUG)

    @check_time
    def run(self) -> None:
        connections = self._get_connection()
        connection = connections.connection
        collection = connections.collection
        count = self.validate_count(connection=connection, collection=collection)
        print("[*] Count : {}".format(count))
        start_point = [0, 2000, 4000, 6000, 8000]
        with Pool(processes=5) as p:
            p.map(self.validate, start_point)

    def validate(self, offset: int) -> None:
        process_id = os.getpid()
        offset = offset
        limit = 2000
        connections = self._get_connection()
        connection = connections.connection
        collection = connections.collection

        while True:
            print("[PID:{}] NOW : {} - {}".format(process_id, offset, limit))
            rdb = self.rdb.get(connection=connection, query=self.rdb_query, offset=offset, limit=limit)
            mongo = self.mongo.get(connection=collection, offset=offset, limit=limit)
            if len(rdb) == 0:
                print("[PID:{}] Done".format(process_id))
                break
            self._validate_data(rdb_data=rdb, mongo_data=mongo)
            offset += self.increase_count

    def _get_connection(self) -> namedtuple:
        connections = namedtuple('connections', ['connection', 'collection'])
        return connections(connection=self.rdb.make_connection(), collection=self.mongo.make_connection())

    def _validate_data(self, rdb_data: dict, mongo_data: dict) -> Optional[NoReturn]:
        # 일반 필드
        keys = [
            'user_id',
            'user_pw',
            'birthday',
            'nickname',
            'created_at',
            'updated_at',
        ]
        # Embedded 필드
        attachment_keys = [
            'attachment_id',
            'attachment_body',
            'created_at',
            'updated_at'
        ]
        for rdb, mongo in zip(rdb_data, mongo_data):
            for key in keys:
                self._validate_by_column(
                    row_id=rdb['user_id'],
                    rdb_data=rdb[key],
                    mongo_data=mongo[key],
                    column_name=key
                )
            if rdb['attachments'] and mongo['attachments_id']:
                rdb_attachment = self._convert_attachments(
                    rdb_id=rdb['attachments_id'],
                    rdb_body=rdb['attachments_body'],
                    rdb_created_at=rdb['attachments_created_at'],
                    rdb_updated_at=rdb['attachments_updated_at']
                )
                for attachment_key in attachment_keys:
                    self._validate_by_column(
                        row_id=rdb['appeal_id'],
                        rdb_data=rdb_attachment[attachment_key],
                        mongo_data=mongo['attachments'][0][attachment_key],
                        column_name=attachment_key
                    )

    @staticmethod
    def _convert_attachments(rdb_id: int,
                             rdb_body: str,
                             rdb_created_at: datetime,
                             rdb_updated_at: datetime) -> dict:
        return {
            'attachment_id': rdb_id,
            'body': rdb_body,
            'created_at': rdb_created_at,
            'updated_at': rdb_updated_at
        }

    def _validate_by_column(self, row_id: int, rdb_data: str, mongo_data: str, column_name: str) -> Optional[NoReturn]:
        try:
            assert rdb_data == mongo_data
        except AssertionError:
            self._print_log(
                '[*] RowId: {} / Field : {} / RDB : {} / Mongo : {}'
                .format(row_id, column_name, rdb_data, mongo_data))

    def validate_count(self, connection, collection) -> Union[NoReturn, int]:
        rdb_count = self.rdb.count(connection=connection, query=self.rdb_query)
        mongo_count = self.mongo.count(connection=collection)
        try:
            assert rdb_count == mongo_count
        except AssertionError:
            self._print_log('[*] Different / RDB : {} / Mongo : {}'.format(rdb_count, mongo_count))
        return rdb_count

    @staticmethod
    def _print_log(logging_object: str) -> None:
        pprint(logging_object)
        logging.debug(logging_object)


if __name__ == '__main__':
    validator = Validate(
        rdb=MySQLPool(
            host=RDBConfig.HOST,
            user=RDBConfig.DB_USER,
            passwd=RDBConfig.DB_PASS,
            db=RDBConfig.DB_NAME,
            port=RDBConfig.PORT
        ),
        mongo=MongoDBPool(
            host=MongoConfig.CLIENT,
            db=MongoConfig.DB_NAME,
            collection=MongoConfig.APPEALS_COLLECTION
        ),
        rdb_query=MYSQL_QUERY
    )
    validator.run()
