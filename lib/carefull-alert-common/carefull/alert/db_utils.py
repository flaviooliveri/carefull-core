import os

import psycopg2


def connect():
    params = {
        'host': os.environ.get("DB_HOST"),
        'database': os.environ.get("DB_NAME"),
        'user': os.environ.get("DB_USER"),
        'password': os.environ.get("DB_PASSWORD")
    }
    return psycopg2.connect(**params)
