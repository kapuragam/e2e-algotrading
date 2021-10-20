import time
from src.util.binance_api import ping_binance


def main():
    while True:
        time.sleep(1)
        ping_binance()


if __name__ == "__main__":
    main()
