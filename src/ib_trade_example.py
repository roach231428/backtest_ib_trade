import sys

sys.path.append("src")
import logging
from datetime import datetime

from broker.InteractiveBrokers import InteractiveBrokers
from grabber.YahooFinance import YahooFinanceGrabber
from strategy.MomentWilliamsR import MomentWilliamsR
from trader.intradayTrader import IntradayTrader

cur_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(name)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"F:/Data/logs/trade/trade_{cur_time}.log",
    filemode="a",
)
logger = logging.getLogger(__name__)

# Create a console handler and set its level to INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and attach it to the console handler
console_formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d - %(name)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console_handler.setFormatter(console_formatter)

# Add the console handler to the root logger (to capture logs from all loggers)
root_logger = logging.getLogger()
root_logger.addHandler(console_handler)

BUFFER_TIME = 10
INIT_CASH = 10000
CMO_PERIOD = 10
CMO_SMA_PERIOD = 10
WILLIAMS_PERIOD = 14
WILLIAMS_LOWER = -60
WILLIAMS_UPPER = -40
SMA_PERIOD = 25
BBANDS_PERIOD = 10 * 5


if __name__ == "__main__":
    quote_ticker = "SOXL"
    hedge_ticker = "SOXS"
    interval = "1m"
    period = "2d"
    stop_loss = 0.005

    trader = IntradayTrader(
        tickers=[quote_ticker, hedge_ticker], run_interval=0.01, buffer_time=BUFFER_TIME
    )
    trader.setBroker(InteractiveBrokers(clientId=1))
    trader.setStrategy(
        MomentWilliamsR(
            quote_ticker=quote_ticker,
            hedge_ticker=hedge_ticker,
            stop_loss=stop_loss,
            cmo_period=CMO_PERIOD,
            cmo_sma_period=CMO_SMA_PERIOD,
            williams_period=WILLIAMS_PERIOD,
            williams_lower=WILLIAMS_LOWER,
            williams_upper=WILLIAMS_UPPER,
            sma_period=SMA_PERIOD,
            bbands_period=BBANDS_PERIOD,
        )
    )
    trader.addGrabber(
        YahooFinanceGrabber(tickers=quote_ticker, interval=interval, period=period)
    )
    trader.addGrabber(
        YahooFinanceGrabber(tickers=hedge_ticker, interval=interval, period=period)
    )
    trader.start()
