import time
import json
import psycopg2
import websocket
from src.util.Utils import get_binance_trading_symbols


conn = psycopg2.connect("dbname=postgres user=postgres password=password host=localhost port=5432")
cursor = conn.cursor()
insert_query = """INSERT INTO {0} (open_time, close_time, open, high, low, close, 
    base_asset_volume, quote_asset_volume, number_of_trades) VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}) ON CONFLICT DO NOTHING;"""


def insert_into_timescaledb(msg):
    symbol = msg['s']
    if not symbol[0].isnumeric():
        interval = msg['k']['i']
        table_name = symbol + '_' + "KLINE" + "_" + interval
        query = insert_query.format(table_name, msg['k']['t'], msg['k']['T'], msg['k']['o'],
                                    msg['k']['h'], msg['k']['l'], msg['k']['c'],
                                    msg['k']['v'], msg['k']['q'], msg['k']['n'])
        cursor.execute(query)
        conn.commit()


def on_message(ws, msg):
    msg = json.loads(msg)
    if msg['k']['x']:
        print(msg)
        start = time.time()
        insert_into_timescaledb(msg)
        end = time.time()
        print("Write took : " + str(end - start))


def on_error(ws, error):
    print("Error : " + str(error))


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


if __name__ == "__main__":
    #websocket.enableTrace(True)
    streams = []
    for symbol in get_binance_trading_symbols():
        streams.append(symbol.lower() + '@' + 'kline_5m')

    streams = '/'.join(streams)
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/" + streams,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()
