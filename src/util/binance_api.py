import time
import json
import urllib.request
import datetime

from src.util.Constants import INTERVAL_TIME_DELTA_MAPPING


def get_api_response(url):
    """
    Basic function used by several other methods
    to make REST API calls to Binance

    :param url: URL of the request to be made
    """

    headers = {'Accept': 'application/json', 'User-Agent': 'binance/python',
               'X-MBX-APIKEY': "evIXEYc45kDXxQmFxO4q95MS03pRpevPp1z8CKAc9EYnfiIX9ufdvLhyaop9RLDP"}
    request = urllib.request.Request(url=url, headers=headers, method='GET')
    response = urllib.request.urlopen(request)
    return response.read().decode("utf8")


def ping_binance():
    try:
        url = 'https://api.binance.com/api/v3/ping'
        get_api_response(url)
        return True
    except Exception as e:
        print("Binance ping failed at {0} : {1}".format(str(datetime.datetime.now()), str(e)))
        return False


def get_historical_klines(symbol, interval, start_time, end_time):
    """
    Function to get 'limit' number of klines starting from start_time

    :param symbol: The ticker symbol for which data is to be obtained. Example: BTCUSDT
    :type symbol: str

    :param interval: The interval for which we want data. Example: Days (1d), Hours(1h), Minutes (1m)
    :type interval: str

    :param start_time: The time in epoch millis starting from which data is to be obtained
    :type start_time: int

    :param end_time: The time in epoch millis up to which data is to be obtained.
    :type end_time: int
    """
    url = 'https://api.binance.com/api/v3/klines'
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": str(start_time),
        "endTime": str(end_time),
        "limit": 1000
    }
    query_string = urllib.parse.urlencode(params)
    url = url + "?" + query_string
    data = get_api_response(url)
    return data


def get_last_n_completed_klines(symbol, interval, n):
    current_utc_millis = int(datetime.datetime.utcnow().timestamp() * 1000)
    interval_time_delta = INTERVAL_TIME_DELTA_MAPPING[interval]
    end_time = current_utc_millis - (current_utc_millis % interval_time_delta)
    start_time = end_time - (n * interval_time_delta)
    return get_all_historical_klines(symbol, interval, start_time, end_time)


def get_all_historical_klines(symbol, interval, start_time, end_time):
    """
    Method to get all of the candlesticks for a particular cryptocurrency.

    :param symbol: The ticker symbol for which data is to be obtained. Example: BTCUSDT
    :type symbol: str

    :param interval: The interval for which we want data. Example: Days (1d), Hours(1h), Minutes (1m)
    :type interval: str

    :param start_time: The time in epoch millis starting from which data is to be obtained
    :type start_time: int

    :param end_time: The time in epoch millis up to which data is to be obtained
    :type end_time: int
    """

    total_data = []
    total_time_delta = 1000 * INTERVAL_TIME_DELTA_MAPPING[interval]
    while start_time < end_time:
        t0 = time.time()
        temp_end_time = start_time + total_time_delta
        if temp_end_time > end_time:
            temp_end_time = end_time
        batch_data = json.loads(get_historical_klines(symbol, interval, start_time, temp_end_time - 1))
        start_time += total_time_delta
        total_data.extend(batch_data)
        t1 = time.time()
        print("::Fetched {} entries for {}, took {}"
              .format(str(len(batch_data)), symbol + "_" + interval, str(t1 - t0)))
    return total_data


if __name__ == '__main__':
    data = get_last_n_completed_klines('ETHUSDT', '1d', 2000)
    print(len(data))
    print(data[0][0])
    print(data[-1][0])
