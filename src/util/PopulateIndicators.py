import time
from influxdb import InfluxDBClient
import Utils


client = InfluxDBClient(host='localhost', port=8086)
client.switch_database('instruments')


def populate_indicators(instrument, interval_label):
    t0 = time.time()
    measurement = instrument["tradingsymbol"] + "_" + interval_label
    result = client.query("select * from " + measurement)
    result_points = list(result.get_points(measurement=measurement))
    #calculate_rsi(result_points, 14, measurement)
    print(len(result_points))
    t1 = time.time()
    print(t1 - t0)



def main():
    instrument_data = Utils.get_instrument_data()
    for instrument in instrument_data[0:1]:
        populate_indicators(instrument, "1D")


if __name__ == "__main__":
    main()