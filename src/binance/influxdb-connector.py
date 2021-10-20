from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def main():

    org = "org"
    bucket = "bucket"
    token = "eblcafN7bmp47l_Y20l3EVk0JI6F69WA_7vq6LiHDiTBQm10R817JgZVAY-_2bHz7Db3qT0ZZHsER0xqMOq0PQ=="

    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=True)

    write_api = client.write_api(write_options=SYNCHRONOUS)

    point = Point("kline-BTCUSDT-2m")\
        .field("klineOpenTime", 1629910080000) \
        .field("klineCloseTime", 1629910139999) \
        .field("open", 3202.54) \
        .field("high", 3208.39) \
        .field("low", 3202.54) \
        .field("close", 3208.27) \
        .field("baseAssetVolume", 163.55772) \
        .field("quoteAssetVolume", 524270.164434) \
        .time(datetime.fromtimestamp(1629910080), WritePrecision.MS)

    print(write_api.write(bucket, org, point))


if __name__ == "__main__":
    main()
