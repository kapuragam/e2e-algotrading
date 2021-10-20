import psycopg2
import binance.enums
from src.util.Utils import get_binance_trading_symbols, get_linebreak_table_name, get_kline_table_name

# Chunk times so that every interval has 10,000 entries per chunk
"""
86400000 1 day in millis
3000000000 in millis 5m
9000000000 in millis 15m
18000000000 in millis 30m
36000000000 in millis 1h
72000000000 in millis 2h
144000000000 in millis 4h
432000000000 in millis 12h
864000000000 in millis 1d
2592000000000 in millis 3d
6048000000000 in millis 1w
"""

chunk_time_for_interval = {binance.enums.KLINE_INTERVAL_5MINUTE: 3000000000,
                           binance.enums.KLINE_INTERVAL_15MINUTE: 9000000000,
                           binance.enums.KLINE_INTERVAL_30MINUTE: 18000000000,
                           binance.enums.KLINE_INTERVAL_1HOUR: 36000000000,
                           binance.enums.KLINE_INTERVAL_2HOUR: 72000000000,
                           binance.enums.KLINE_INTERVAL_4HOUR: 144000000000,
                           binance.enums.KLINE_INTERVAL_12HOUR: 432000000000,
                           binance.enums.KLINE_INTERVAL_1DAY: 864000000000,
                           binance.enums.KLINE_INTERVAL_3DAY: 2592000000000,
                           binance.enums.KLINE_INTERVAL_1WEEK: 6048000000000}

intervals = [binance.enums.KLINE_INTERVAL_1WEEK,
             binance.enums.KLINE_INTERVAL_3DAY,
             binance.enums.KLINE_INTERVAL_1DAY,
             binance.enums.KLINE_INTERVAL_12HOUR,
             binance.enums.KLINE_INTERVAL_4HOUR,
             binance.enums.KLINE_INTERVAL_2HOUR,
             binance.enums.KLINE_INTERVAL_1HOUR,
             binance.enums.KLINE_INTERVAL_30MINUTE,
             binance.enums.KLINE_INTERVAL_15MINUTE,
             binance.enums.KLINE_INTERVAL_5MINUTE]


def remove_tables(conn, symbols):
    cursor = conn.cursor()
    drop_table = "DROP TABLE IF EXISTS {0}"
    for symbol in symbols[0:2]:
        for interval in intervals:
            drop_kline_table_command = drop_table.format(get_kline_table_name(symbol, interval))
            drop_linebreak_table_command = drop_table.format(get_linebreak_table_name(symbol, interval))

            print(drop_kline_table_command)
            cursor.execute(drop_kline_table_command)
            conn.commit()

            print(drop_linebreak_table_command)
            cursor.execute(drop_linebreak_table_command)
            conn.commit()


def create_tables(conn, symbols):
    cursor = conn.cursor()
    create_kline_table = """CREATE TABLE IF NOT EXISTS {0} (open_time BIGINT PRIMARY KEY, close_time BIGINT, 
    open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION, 
    base_asset_volume DOUBLE PRECISION, quote_asset_volume DOUBLE PRECISION, number_of_trades BIGINT);"""

    create_linebreak_table = """CREATE TABLE IF NOT EXISTS {0} (open_time BIGINT PRIMARY KEY, close_time BIGINT, 
            open DOUBLE PRECISION, close DOUBLE PRECISION, base_asset_volume DOUBLE PRECISION, 
            quote_asset_volume DOUBLE PRECISION, number_of_trades BIGINT);"""

    create_hypertable = "SELECT create_hypertable('{0}', 'open_time', chunk_time_interval => {1});"

    create_start_end_table = """CREATE TABLE IF NOT EXISTS start_end (table_name VARCHAR(100) PRIMARY KEY, end_time 
    BIGINT); """

    for symbol in symbols:
        for interval in intervals:
            kline_table_name = get_kline_table_name(symbol, interval)
            linebreak_table_name = get_linebreak_table_name(symbol, interval)

            # Kline
            create_kline_table_command = create_kline_table.format(kline_table_name)
            print(create_kline_table_command)
            cursor.execute(create_kline_table_command)
            conn.commit()

            # Kline Hypertable
            create_kline_hypertable_command = create_hypertable.format(kline_table_name,
                                                                       str(chunk_time_for_interval[interval]))
            print(create_kline_hypertable_command)
            cursor.execute(create_kline_hypertable_command)
            conn.commit()

            # Linebreak
            create_linebreak_table_command = create_linebreak_table.format(linebreak_table_name)
            print(create_linebreak_table_command)
            cursor.execute(create_linebreak_table_command)
            conn.commit()

            # Linebreak Hypertable
            create_linebreak_hypertable_command = create_hypertable.format(linebreak_table_name,
                                                                           str(chunk_time_for_interval[interval]))
            print(create_linebreak_hypertable_command)
            cursor.execute(create_linebreak_hypertable_command)
            conn.commit()

    # Start End Table
    cursor.execute(create_start_end_table)
    conn.commit()


def main():
    conn = psycopg2.connect("dbname=postgres user=postgres password=password host=localhost port=5432")
    symbols = get_binance_trading_symbols()

    # remove_tables(conn, symbols)
    create_tables(conn, symbols)

    cursor = conn.cursor()
    cursor.close()


if __name__ == "__main__":
    main()
