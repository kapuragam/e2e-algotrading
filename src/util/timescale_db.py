import time
from psycopg2.extras import execute_values
from src.util.Utils import get_kline_table_name


def insert_into_kline_table(symbol, interval, klines, conn):
    """
    :param symbol: The ticker symbol. Ex: BTCUSDT
    :param interval: The interval. Ex: 1m, 4h, 1D
    :param klines: List of tuples containing values : [()]
    :param conn: TimescaleDB connection object
    """
    start = time.time()
    cursor = conn.cursor()
    table_name = get_kline_table_name(symbol, interval).lower()
    query_head = """INSERT INTO {0} (open_time, close_time, open, high, low, close, \
                base_asset_volume, quote_asset_volume, number_of_trades) VALUES """.format(table_name)
    query_tail = """%s ON CONFLICT (open_time) DO UPDATE SET open=EXCLUDED.open, \
     high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, base_asset_volume=EXCLUDED.base_asset_volume, \
     quote_asset_volume=EXCLUDED.quote_asset_volume, number_of_trades=EXCLUDED.number_of_trades;"""
    query = query_head + query_tail
    rows = [(str(kline[0]), str(kline[6]), str(kline[1]), str(kline[2]), str(kline[3]),
             str(kline[4]), str(kline[5]), str(kline[7]), str(kline[8])) for kline in klines]
    execute_values(cursor, query, rows)
    conn.commit()
    end = time.time()
    print("Insert took : " + str(end - start))


def get_last_entry_of_table(table_name, conn):
    start = time.time()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM {} ORDER BY open_time DESC LIMIT 1;".format(table_name))
    end = time.time()
    print("Fetching last entry from {} took : {}".format(table_name, str(end - start)))
    return cursor.fetchall()


def insert_into_start_end_table(rows, conn):
    """
    :param rows: List of tuples to be inserted
    :type rows: [()]

    :param conn: Timescaledb connection
    :type conn: psycopg2.connect()
    """
    start = time.time()

    # Just using execute_values will not commit changes into db
    execute_values(conn.cursor(), """INSERT INTO start_end (table_name, end_time) \
    VALUES %s ON CONFLICT (table_name) DO UPDATE SET end_time=EXCLUDED.end_time""", rows)

    # A separate commit statement is required
    conn.commit()
    end = time.time()
    print("Inserting values into start_end took : {}".format(str(end - start)))


def fetch_all_from_start_end_table(conn):
    """
    Fetches all entries from start_end table
    :param conn: Timescaledb connection
    :type conn: psycopg2.connect()
    """
    start = time.time()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM start_end;")
    end = time.time()
    print("Fetching all entries from start_end took : {}".format(str(end - start)))
    return cursor.fetchall()
