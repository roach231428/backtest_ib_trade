from typing import Dict, List

import pandas as pd
import yfinance as yf

from src.grabber.base import DataGrabberBase


class YahooFinanceGrabber(DataGrabberBase):
    max_period = {
        "1m": "7d",
        "2m": "7d",
        "5m": "60d",
        "15m": "60d",
        "30m": "60d",
        "60m": "730d",
        "90m": "60d",
        "1h": "730d",
    }

    def __init__(
        self, tickers: str | List[str], interval: str = "1m", period: str = "max"
    ) -> None:
        super().__init__(tickers, interval=interval, period=period)

    def getHistoricalData(
        self,
        interval: str | None = None,
        start: str | None = None,
        end: str | None = None,
        period: str | None = None,
    ) -> pd.DataFrame:
        interval = self.interval if interval is None else interval
        period = self.period if period is None else period
        if period == "max" and interval in self.max_period:
            period = self.max_period[interval]
        period_text = f"last {period[:-1]} days" if period != "max" else "maximum days"
        ticker_msg = (
            self.tickers if isinstance(self.tickers, str) else ", ".join(self.tickers)
        )
        self.logger.info(f"Getting {ticker_msg} {self.interval} {period_text} data...")
        return yf.download(
            self.tickers,
            interval=interval,
            period=period,
            start=start,
            end=end,
            progress=False,
        )

    def getLatestData(self) -> Dict[str, Dict[str, float]]:
        hist_df = self.getHistoricalData(period="2d")
        timestamp = hist_df.index[-1]
        hist_row = hist_df.iloc[-1, :]
        if isinstance(self.tickers, str):
            return {
                self.tickers: {
                    "Open": hist_row["Open"],
                    "High": hist_row["High"],
                    "Low": hist_row["Low"],
                    "Close": hist_row["Close"],
                    "Adj Close": hist_row["Adj Close"],
                    "Volume": hist_row["Volume"],
                    "Datetime": timestamp,
                }
            }

        res = dict()
        for tick in self.tickers:
            if len(self.tickers) > 1:
                res[tick] = {
                    "Open": hist_row["Open"][tick],
                    "High": hist_row["High"][tick],
                    "Low": hist_row["Low"][tick],
                    "Close": hist_row["Close"][tick],
                    "Adj Close": hist_row["Adj Close"][tick],
                    "Volume": hist_row["Volume"][tick],
                    "Datetime": timestamp,
                }
            else:
                res[tick] = {
                    "Open": hist_row["Open"],
                    "High": hist_row["High"],
                    "Low": hist_row["Low"],
                    "Close": hist_row["Close"],
                    "Adj Close": hist_row["Adj Close"],
                    "Volume": hist_row["Volume"],
                    "Datetime": timestamp,
                }
        return res
