import time
import psycopg2

from src.util.Constants import INTERVALS, INTERVAL_TIME_DELTA_MAPPING
from src.util.Utils import get_binance_trading_symbols, get_kline_table_name
from src.util.binance_api import get_all_historical_klines
from src.util.timescale_db import insert_into_kline_table, fetch_all_from_start_end_table, insert_into_start_end_table


def fetch_and_store_historical_klines(symbols, intervals, conn):
    start_end_rows = fetch_all_from_start_end_table(conn)
    start_end_rows = dict(start_end_rows)
    new_start_end_rows = []
    for symbol in symbols:
        for interval in intervals:
            table_name = get_kline_table_name(symbol, interval)
            end_time = start_end_rows[table_name]
            current_millis = time.time() * 1000
            if current_millis - end_time > INTERVAL_TIME_DELTA_MAPPING[interval]:
                klines = get_all_historical_klines(symbol, interval, end_time)
                if len(klines) > 0:
                    insert_into_kline_table(symbol, interval, klines, conn)
                    new_start_end_rows.append((table_name, klines[-1][6]))
            else:
                print("Not required for : " + table_name)
    insert_into_start_end_table(new_start_end_rows, conn)


if __name__ == '__main__':
    start = time.time()
    fetch_and_store_historical_klines(get_binance_trading_symbols(), INTERVALS, psycopg2.connect(
        "dbname=postgres user=postgres password=password host=localhost port=5432"))
    end = time.time()
    print("Fetching latest klines took : " + str(end-start))

