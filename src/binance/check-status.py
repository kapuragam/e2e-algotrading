import time
import psycopg2
import datetime
from src.util.binance_api import ping_binance


def execute_db_query():
    start = time.time()
    conn = psycopg2.connect("dbname=postgres user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    query = """SELECT * FROM AAVEDOWNUSDT_KLINE_5m ORDER BY open_time DESC LIMIT 5;"""
    cursor.execute(query)
    conn.commit()
    print(cursor.fetchall())
    end = time.time()
    print(end - start)
    print("%s")


def test():
    start = time.time()
    # print(ping_binance())
    current_utc_millis = int(datetime.datetime.utcnow().timestamp() * 1000)
    print(str(current_utc_millis - (current_utc_millis % 300000)))
    end = time.time()
    print("Took : " + str(end - start))


if __name__ == "__main__":
    test()
