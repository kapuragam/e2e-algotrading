import time
from datetime import datetime
from influxdb import InfluxDBClient

client = InfluxDBClient(host='localhost', port=8086)
client.switch_database('instruments')


def is_green_candle(lb_entry):
    return lb_entry["close"] > lb_entry["open"]


def get_lb_entry(result_point, last_n_lb_entries):
    last_n_open_close = []
    for item in last_n_lb_entries:
        last_n_open_close.append(item["open"])
        last_n_open_close.append(item["close"])
    if result_point["close"] > max(last_n_open_close) or result_point["close"] < min(last_n_open_close):
        lb_close = result_point["close"]
        if (result_point["close"] > max(last_n_open_close) and (is_green_candle(last_n_lb_entries[-1]))) or \
                (result_point["close"] < min(last_n_open_close) and not (is_green_candle(last_n_lb_entries[-1]))):
            lb_open = last_n_lb_entries[-1]["close"]
        if (result_point["close"] > max(last_n_open_close) and not is_green_candle(last_n_lb_entries[-1])) or \
                (result_point["close"] < min(last_n_open_close) and (is_green_candle(last_n_lb_entries[-1]))):
            lb_open = last_n_lb_entries[-1]["open"]
        entry = {
            "time": result_point["time"],
            "open": float(lb_open),
            "close": float(lb_close)
        }
        return entry
    else:
        return None


def insert_db(lb_measurement, lb_entries_list):
    t0 = time.time()
    points = []
    for item in lb_entries_list:
        entry = {
            "measurement": lb_measurement,
            "time": int(datetime.strptime(item["time"], "%Y-%m-%dT%H:%M:%SZ").timestamp()),
            "fields": {
                "open": float(item["open"]),
                "close": float(item["close"])
            }
        }
        points.append(entry)
    status = client.write_points(points, time_precision='s')
    t1 = time.time()
    if status:
        print("Inserted line break data into {}, took {}".format(lb_measurement, t1 - t0))


def calculate_n_line_break(result_points, n, measurement):
    t0 = time.time()
    lb_measurement = measurement + "_" + "LB" + str(n)
    lb_entries_list = []
    for i in range(0, len(result_points)):
        if i == 0:
            entry = {
                "time": result_points[i]["time"],
                "open": float(result_points[i]["open"]),
                "close": float(result_points[i]["close"])
            }
            lb_entries_list.append(entry)
        else:
            lb_entry = get_lb_entry(result_points[i], lb_entries_list[-n:])
            if lb_entry is not None:
                lb_entries_list.append(lb_entry)
    t1 = time.time()
    print("Calculated {} line break data for {}, took {}. Contains {} entries.".format(n, measurement, t1 - t0, len(lb_entries_list)))
    insert_db(lb_measurement, lb_entries_list)


def populate_n_line_break(instrument, interval_label):
    t0 = time.time()
    measurement = instrument["tradingsymbol"] + "_" + interval_label
    print("-----------POPULATE LINE BREAK DATA FOR {}-----------".format(measurement))
    t1 = time.time()
    result = client.query("select * from " + '"' + measurement + '"') # measurement names with - or special char need to be in double quotes.
    result_points = list(result.get_points(measurement=measurement))
    t2 = time.time()
    print("Fetched {} entries from {}, took {}".format(len(result_points),measurement, t2 - t1))
    calculate_n_line_break(result_points, 3, measurement)
    calculate_n_line_break(result_points, 6, measurement)
    t3 = time.time()
    print("-----------FINISHED POPULATING LINE BREAK DATA FOR {}, took {}-----------".format(measurement, t3 - t0))


def main():
    pass


if __name__ == "__main__":
    main()
