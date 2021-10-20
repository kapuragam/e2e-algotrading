import re
import time
import psycopg2
from binance.streams import ThreadedWebsocketManager
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Binance
from src.util.Constants import INTERVALS
from src.util.Utils import get_binance_trading_symbols
from src.util.binance_api import ping_binance

api_key = 'evIXEYc45kDXxQmFxO4q95MS03pRpevPp1z8CKAc9EYnfiIX9ufdvLhyaop9RLDP'
api_secret = 'NGYY4frB6GPfNGiw0509vnDfGAtM16JcnPpkuys82ZStT8s2zDFKPn237P7Zw33d'

# InfluxDB
org = "org"
bucket = "bucket"
token = "qVFrOWlOtJ4CDqai36xfwuwaoz0wn19fX2QZw6XthEITZ7HKOl-wOlpKLrb3FzyyEh3hq83YKRObuAICfgN0EQ=="

conn = psycopg2.connect("dbname=postgres user=postgres password=password host=localhost port=5432")
cursor = conn.cursor()
insert_query = """INSERT INTO {0} (kline_open_time, kline_close_time, open, high, low, close, 
    base_asset_volume, quote_asset_volume) VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}) ON CONFLICT DO NOTHING;"""


def insert_into_influxdb(write_api, msg):
    point = Point((msg['stream']).replace('@', '_')) \
        .field("klineOpenTime", int(msg['data']['k']['t'])) \
        .field("klineCloseTime", int(msg['data']['k']['T'])) \
        .field("open", float(msg['data']['k']['o'])) \
        .field("high", float(msg['data']['k']['h'])) \
        .field("low", float(msg['data']['k']['l'])) \
        .field("close", float(msg['data']['k']['c'])) \
        .field("baseAssetVolume", float(msg['data']['k']['v'])) \
        .field("quoteAssetVolume", float(msg['data']['k']['q'])) \
        .time(int(msg['data']['k']['t']) * 1000000)

    write_api.write(bucket, org, point)


def insert_into_timescaledb(msg):
    symbol = msg['stream'].split('@')[0].upper()
    if not symbol[0].isnumeric():
        interval = msg['stream'].split('@')[1].split('_')[1]
        table_name = symbol + '_' + "KLINE" + "_" + interval
        query = insert_query.format(table_name, msg['data']['k']['t'], msg['data']['k']['T'], msg['data']['k']['o'],
                                    msg['data']['k']['h'], msg['data']['k']['l'], msg['data']['k']['c'],
                                    msg['data']['k']['v'], msg['data']['k']['q'], msg['data']['k']['q'])
        cursor.execute(query)
        conn.commit()


def run(streams):
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()
    print("Started websocket at : " + str(time.time()*1000))

    def handle_socket_message(msg):
        if msg['data']['k']['x']:
            print(msg)
            start = time.time()
            # insert_into_influxdb(write_api, msg)
            # insert_into_timescaledb(msg)
            end = time.time()
            print("Write took : " + str(end - start))

    twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
    for i in range(60):
        if not ping_binance():
            print("Error while pinging binance at : " + str(time.time()*1000))
            twm.stop()
            print("Stopped websocket")
            return
        time.sleep(60)
    twm.stop()
    print("Stopped websocket")


def call(streams):
    while True:
        if ping_binance():
            run(streams)
        else:
            print("Could not connect to binance")
            time.sleep(60)


def main():
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    streams = []
    for symbol in get_binance_trading_symbols():
        streams.append(symbol.lower() + '@' + 'kline_5m')
    streams = '/'.join(streams)
    print(streams)

    #call(streams)


if __name__ == "__main__":
    main()
