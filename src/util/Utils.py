import re


def get_binance_trading_symbols():
    symbols = []
    with open('../../data/binance/current-trading-symbols.txt', 'r') as f:
        for line in f:
            symbol = line.split()[0]
            symbols.append(symbol)
    r = re.compile(r"^[a-zA-Z]")
    symbols = list(filter(r.match, symbols))
    return symbols


def get_kline_table_name(symbol, interval):
    return symbol + '_' + "KLINE" + "_" + interval


def get_linebreak_table_name(symbol, interval):
    return symbol + '_' + "LINEBREAK" + "_" + interval