import logging
import sys
from datetime import datetime
from os import makedirs
from os.path import exists, isfile

import pandas as pd

sys.path.append("src")
from grabber.YahooFinance import YahooFinanceGrabber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def update_yf_data(ticker: str, interval: str, datadir: str = "F:/Data/Stock") -> None:
    if not exists(f"{datadir}/{interval}"):
        makedirs(f"{datadir}/{interval}")
    file_path = f"{datadir}/{interval}/{ticker}.csv"
    yf = YahooFinanceGrabber(ticker, interval)

    if isfile(file_path):
        # Load old stock data if exists
        old_data = pd.read_csv(file_path)
        old_data["Datetime"] = pd.to_datetime(old_data["Datetime"])
        old_data["Datetime"] = [x.tz_localize(None) for x in old_data["Datetime"]]

        # Calculate the days gap and download new stock price data
        today = datetime.now().date()
        lastest_date = old_data["Datetime"].sort_values().iloc[-1].date()
        earliest_date = old_data["Datetime"].sort_values().iloc[0].date()
        period = min(int(yf.max_period[interval][:-1]), (today - lastest_date).days + 1)
        if int(yf.max_period[interval][:-1]) > (today - earliest_date).days + 1:
            period = yf.max_period[interval][:-1]
        stock_data = yf.getHistoricalData(period=f"{period}d")
        stock_data.reset_index(inplace=True)
        stock_data["Datetime"] = [x.tz_localize(None) for x in stock_data["Datetime"]]

        # Check if split shares
        intersect = old_data[["Datetime", "Close"]].merge(
            stock_data[["Datetime", "Close"]], on="Datetime"
        )
        intersect["ratio"] = intersect["Close_y"] / intersect["Close_x"]
        ratio = intersect["ratio"].round(5).value_counts()
        if len(ratio) > 1 and abs(ratio.index[0] - ratio.index[1]) > 0.1:
            logger.error(
                "Error: Multiple ratios of close prices between splitting shares found."
            )
            return
        ratio = ratio.index[0]
        if round(ratio, 1) != 1:  # Shares splitting happens
            logger.info(f"Stock splits detected in {ticker}, split ratio: {ratio}")
            old_data[["Open", "High", "Low", "Close", "Adj Close"]] *= ratio
            old_data["Volume"] /= ratio

        # Append the new stock data
        stock_data = (
            pd.concat([old_data, stock_data])
            .sort_values("Datetime")
            .drop_duplicates("Datetime", keep="last")
        )
    else:
        stock_data = yf.getHistoricalData()
        stock_data.reset_index(inplace=True)
    stock_data.sort_values("Datetime", inplace=True)
    stock_data.to_csv(file_path, index=False)


if __name__ == "__main__":
    workdir = "F:/Data/Stock"
    today = datetime.now().date()
    if not exists(f"{workdir}/logs"):
        makedirs(f"{workdir}/logs")
    logging.basicConfig(
        filename=f"{workdir}/logs/{today}.log",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger()

    tickers = [
        "DDM",
        "DIA",
        "DXD",
        "QID",
        "QLD",
        "QQQ",
        "SDS",
        "SOXL",
        "SOXQ",
        "SOXS",
        "SPXU",
        "SQQQ",
        "SSO",
        "TQQQ",
        "UPRO",
        "VOO",
        "VTI",
        "VT",
        "IEF",
        "IEI",
        "SHY",
        "TLT",
        "TMF",
        "AAPL",
        "AMD",
        "AMZN",
        "ARKF",
        "DELL",
        "DOCU",
        "GOOGL",
        "IBM",
        "INTC",
        "META",
        "MSFT",
        "MU",
        "NVDA",
        "SE",
        "TSLA",
        "TSM",
        "UMC",
        "W",
    ]
    intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]

    for interval in intervals:
        for ticker in tickers:
            update_yf_data(ticker, interval)
