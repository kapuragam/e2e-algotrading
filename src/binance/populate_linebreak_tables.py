import psycopg2
import binance.enums

intervals = [binance.enums.KLINE_INTERVAL_1MINUTE, binance.enums.KLINE_INTERVAL_3MINUTE,
             binance.enums.KLINE_INTERVAL_5MINUTE, binance.enums.KLINE_INTERVAL_15MINUTE,
             binance.enums.KLINE_INTERVAL_30MINUTE, binance.enums.KLINE_INTERVAL_1HOUR,
             binance.enums.KLINE_INTERVAL_2HOUR, binance.enums.KLINE_INTERVAL_4HOUR,
             binance.enums.KLINE_INTERVAL_6HOUR, binance.enums.KLINE_INTERVAL_8HOUR,
             binance.enums.KLINE_INTERVAL_12HOUR, binance.enums.KLINE_INTERVAL_1DAY,
             binance.enums.KLINE_INTERVAL_3DAY, binance.enums.KLINE_INTERVAL_1WEEK]

# Use pyalgotrading package from python algorithmic trading cookbookN

def main():
    conn = psycopg2.connect("dbname=postgres user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()



if __name__ == "__main__":
    main()