from functools import wraps
import time


class RDBConfig:
    HOST = ''
    PORT = 3306
    DB_NAME = ''
    DB_USER = ''
    DB_PASS = ''


class MongoConfig:
    CLIENT = 'mongodb://localhost:27017/'
    DB_NAME = ''
    APPEALS_COLLECTION = ''


MYSQL_QUERY = """
SELECT * FROM users
"""


def check_time(func):
    @wraps(func)
    def measure(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print("[*] FUNCTION `{}` estimated time : {}".format(func.__name__, (end - start)))
        return result
    return measure
