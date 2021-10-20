import binance.enums

INTERVALS = [binance.enums.KLINE_INTERVAL_5MINUTE, binance.enums.KLINE_INTERVAL_15MINUTE,
             binance.enums.KLINE_INTERVAL_30MINUTE, binance.enums.KLINE_INTERVAL_1HOUR,
             binance.enums.KLINE_INTERVAL_2HOUR, binance.enums.KLINE_INTERVAL_4HOUR,
             binance.enums.KLINE_INTERVAL_1DAY, binance.enums.KLINE_INTERVAL_1WEEK]

INTERVAL_TIME_DELTA_MAPPING = {binance.enums.KLINE_INTERVAL_5MINUTE: 300000,
                               binance.enums.KLINE_INTERVAL_15MINUTE: 900000,
                               binance.enums.KLINE_INTERVAL_30MINUTE: 1800000,
                               binance.enums.KLINE_INTERVAL_1HOUR: 3600000,
                               binance.enums.KLINE_INTERVAL_2HOUR: 7200000,
                               binance.enums.KLINE_INTERVAL_4HOUR: 14400000,
                               binance.enums.KLINE_INTERVAL_1DAY: 86400000,
                               binance.enums.KLINE_INTERVAL_1WEEK: 604800000}
