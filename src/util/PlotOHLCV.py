import time
import mplfinance as mpf
import pandas as pd
from influxdb import InfluxDBClient
import Utils


client = InfluxDBClient(host='localhost', port=8086)
client.switch_database('instruments')


def draw_chart(instrument, interval_label):
    measurement = instrument["tradingsymbol"] + "_" + interval_label
    t0 = time.time()
    result = client.query("select * from " + measurement)
    result_points = list(result.get_points(measurement=measurement))
    date = []
    open = []
    high = []
    low = []
    close = []
    t1 = time.time()
    for item in result_points:
        date.append(item["time"])
        open.append((item["open"]))
        high.append(item["high"])
        low.append((item["low"]))
        close.append((item["close"]))
    data = {"Date": date,"Open": open, "High": high, "Low": low, "Close": close}
    df = pd.DataFrame(data, index=pd.to_datetime(date, format="%Y-%m-%dT%H:%M:%SZ"))
    t2 = time.time()
    print(df.head(3))
    print(df.tail(3))
    mpf.plot(df, type='candle')

    print(t2-t1)



def main():
    instrument_data = Utils.get_instrument_data()
    for instrument in instrument_data[0:1]:
        draw_chart(instrument, "1D")


if __name__ == "__main__":
    main()



