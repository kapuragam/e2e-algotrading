import re
import json
import urllib.request
import psycopg2
import pickle
import time


def get_api_response(url):
    """
    Basic function used by several other methods
    to make REST API calls to Binance

    :param url: URL of the request to be made
    """

    # api key : evIXEYc45kDXxQmFxO4q95MS03pRpevPp1z8CKAc9EYnfiIX9ufdvLhyaop9RLDP
    # api secret : NGYY4frB6GPfNGiw0509vnDfGAtM16JcnPpkuys82ZStT8s2zDFKPn237P7Zw33d

    headers = {'Accept': 'application/json', 'User-Agent': 'binance/python',
               'X-MBX-APIKEY': "evIXEYc45kDXxQmFxO4q95MS03pRpevPp1z8CKAc9EYnfiIX9ufdvLhyaop9RLDP"}
    request = urllib.request.Request(url=url, headers=headers, method='GET')
    response = urllib.request.urlopen(request)
    return response.read().decode("utf8")


def get_all_tickers():
    url = 'https://api.binance.com/api/v3/ticker/price'
    return get_api_response(url)


def get_candlesticks(symbol, interval, start_time):
    """
    Method to get one quantum (1000) of candlesticks. They can be 1000 days, hours, or minutes.

    :param symbol: The ticker symbol for which data is to be obtained. Example: BTCUSDT
    :type symbol: str

    :param interval: The interval for which we want data. Example: Days (1d), Hours(1h), Minutes (1m)
    :type interval: str

    :param start_time: The time in epoch millis starting from which data is to be obtained
    :type symbol: str
    """
    total_data = []
    url = 'https://api.binance.com/api/v3/klines'
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": str(start_time),
        "limit": 1000
    }
    query_string = urllib.parse.urlencode(params)
    url = url + "?" + query_string
    data = get_api_response(url)
    return data


def get_all_candlesticks(symbol, interval, start_time):
    """
    Method to get one all of the candlesticks for a particular cryptocurrency.

    :param symbol: The ticker symbol for which data is to be obtained. Example: BTCUSDT
    :type symbol: str

    :param interval: The interval for which we want data. Example: Days (1d), Hours(1h), Minutes (1m)
    :type interval: str

    :param start_time: The time in epoch millis starting from which data is to be obtained
    :type symbol: str
    """
    total_data = []
    t2 = time.time()
    batch_data = json.loads(get_candlesticks(symbol, interval, start_time))
    t3 = time.time()
    print("::Fetched {} entries for {}, took {}"
          .format(str(len(batch_data)), symbol + "_" + interval, str(t3 - t2)))
    total_data.extend(batch_data)
    while len(batch_data) >= 1000:
        t0 = time.time()
        batch_data = json.loads(get_candlesticks(symbol, interval, batch_data[-1][0] + 1))
        t1 = time.time()
        print("::Fetched {} entries for {}, took {}"
              .format(str(len(batch_data)), symbol + "_" + interval, str(t1-t0)))
        total_data.extend(batch_data)
    return total_data


def insert_into_pgdb(candlestick_data, ticker, interval, cur, conn):
    table_name = ticker + "_" + interval
    cur.execute("CREATE TABLE IF NOT EXISTS " + table_name + " (open_time bigint PRIMARY KEY, open numeric(30,10), " +
                "high numeric(30,10), low numeric(30,10), close numeric(30,10), volume numeric(30,10), " +
                "close_time bigint, quote_asset_volume numeric(30,10), number_of_trades int);")

    for entry in candlestick_data:
        cur.execute("INSERT INTO " + table_name + " (open_time, open, high, low, close, volume, " +
                    "close_time, quote_asset_volume, number_of_trades) VALUES (%s, %s, %s, %s, %s, %s, " +
                    "%s, %s, %s) ON CONFLICT DO NOTHING;", (str(entry[0]), str(entry[1]), str(entry[2]), str(entry[3]),
                                                            str(entry[4]), str(entry[5]), str(entry[6]), str(entry[7]),
                                                            str(entry[8])))

    conn.commit()


def main():
    #conn = psycopg2.connect("dbname=postgres user=postgres password=secret")
    #cur = conn.cursor()
    t2 = time.time()
    counter = 0
    interval = "30m"
    all_tickers = json.loads(get_all_tickers())
    usdt_tickers = [{"symbol": item["symbol"]} for item in all_tickers if re.search("USDT$", item["symbol"])]
    total_number_of_tickers = len(usdt_tickers)
    for item in usdt_tickers[-261::-1]:
        counter += 1
        t0 = time.time()
        ticker = item["symbol"]
        table_name = ticker + "_" + interval
        candlestick_data = get_all_candlesticks(ticker, interval, 1502942400000)
        #insert_into_pgdb(candlestick_data, item["symbol"], "1d", cur, conn)
        with open(table_name, 'wb') as f:
            pickle.dump(candlestick_data, f, pickle.HIGHEST_PROTOCOL)
        t1 = time.time()
        print("---------------------Finished pulling and storing data for : {}, took {}---------------------"
              .format(table_name + " " + str(counter) + "/" + str(total_number_of_tickers), str(t1-t0)))
    #cur.close()
    #conn.close()
    t3 = time.time()
    print(":::::::::::::Finished pulling and storing data for : {}, took {}".format(interval, str(t3-t2)))


if __name__ == '__main__':
    main()
