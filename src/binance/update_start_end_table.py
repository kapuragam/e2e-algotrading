import psycopg2

from src.util.Utils import get_binance_trading_symbols
from src.util.Constants import INTERVALS
from src.util.timescale_db import get_last_entry_of_table, insert_into_start_end_table


def populate_start_end_table(conn):
    rows = []
    for symbol in get_binance_trading_symbols():
        for interval in INTERVALS:
            kline_table_name = symbol + '_' + "KLINE" + "_" + interval
            print(kline_table_name)
            result = get_last_entry_of_table(kline_table_name, conn)
            rows.append((kline_table_name, result[0][1]))
    insert_into_start_end_table(rows, conn)


if __name__ == '__main__':
    conn = psycopg2.connect("dbname=postgres user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    populate_start_end_table(conn)
    cursor.execute("SELECT * FROM start_end")
    print(cursor.fetchall())
