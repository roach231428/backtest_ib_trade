import logging
import re
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import schwab
import pytz

from .base import DataGrabberBase


class SchwabGrabber(DataGrabberBase):
    _max_period = {
        "1m": "48d",
        "5m": "270d",
        "10m": "270d",
        "15m": "270d",
        "30m": "270d",
        "1d": "20y",
        "1w": "20y",
        "1M": "20y",
    }

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        tickers: List[str],
        interval: str,
        period: str = "max",
        name: str = "",
        token_path: str = "token1.json",
        callback_url: str = "https://127.0.0.1:8182/",
        account: str | None = None,
        client: schwab.client.Client | schwab.client.AsyncClient | None = None,
    ) -> None:
        """
        Initializes a new instance of the SchwabGrabber class.

        Args:
            api_key (str): The API key for authentication.
            api_secret (str): The API secret for authentication.
            tickers (List[str]): The list of tickers to grab data for.
            interval (str): The interval at which to grab data.
            period (str, optional): The period of data to grab. Defaults to "max".
            name (str, optional): The name of the grabber. Defaults to "".
            token_path (str, optional): The path to the token file. Defaults to "./token1.json".
            callback_url (str, optional): The callback URL for authentication. Defaults to "https://127.0.0.1".
            client (schwab.client.Client | schwab.client.AsyncClient, optional): The client object. Defaults to None.

        Returns:
            None

        Initializes a new instance of the SchwabGrabber class.
        If client is None, it creates a new client object using the provided API key, API secret, token path, and callback URL.
        If token path is None, it creates a new client object using the provided API key, API secret, callback URL, and token path.
        The account hash value is retrieved from the client object.
        """

        super().__init__(tickers, interval=interval, period=period, name=name)
        self.logger = logging.getLogger(__name__)

        if client is None:
            self.client = schwab.auth.easy_client(
                api_key=api_key,
                app_secret=api_secret,
                callback_url=callback_url,
                token_path=token_path,
            )
        else:
            self.client = client

        acc_res = self.client.get_account_numbers().json()
        account_hash_map = {x["accountNumber"]: x["hashValue"] for x in acc_res}
        if account is not None:
            if account not in account_hash_map.keys():
                self.account_hash = account_hash_map[account]
                self.logger.error("Account %s not found.", account)
        else:
            account = list(account_hash_map.keys())[0]
        self.logger.info("Using account %s.", account)
        self.account_hash = account_hash_map[account]

    def convert_timestamp(self, ts: int, tz: str = "America/New_York") -> datetime:
        if ts > 1e12:
            ts /= 1000
        return (
            datetime.fromtimestamp(ts, tz=pytz.timezone(tz)).replace(tzinfo=None)
        )

    def getHistoricalData(
        self,
        tickers: str | List[str] = None,
        interval: str = None,
        period: str = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        tickers = self.tickers if (tickers is None) else tickers

        if start is None:
            start = datetime(year=1971, month=1, day=1)
        if end is None:
            end = datetime.utcnow() + timedelta(days=7)

        PriceHistory = schwab.client.Client.PriceHistory
        time_text = ""

        # Setting the period
        period = (
            self.period
            if (not isinstance(period, str)) and (period is None)
            else period
        )
        if period != "max" and period is not None:
            period_type_abbr = period[-1].lower()
            period_num = int(re.search(r"\d+", period).group())
            if period_type_abbr == "d":
                period_type = PriceHistory.PeriodType.DAY
                valid_periods = {
                    x.value: x
                    for x in [
                        PriceHistory.Period.ONE_DAY,
                        PriceHistory.Period.TWO_DAYS,
                        PriceHistory.Period.THREE_DAYS,
                        PriceHistory.Period.FOUR_DAYS,
                        PriceHistory.Period.FIVE_DAYS,
                        PriceHistory.Period.TEN_DAYS,
                    ]
                }
            elif period_type_abbr == "m":
                period_type = PriceHistory.PeriodType.MONTH
                valid_periods = {
                    x.value: x
                    for x in [
                        PriceHistory.Period.ONE_MONTH,
                        PriceHistory.Period.TWO_MONTHS,
                        PriceHistory.Period.THREE_MONTHS,
                        PriceHistory.Period.SIX_MONTHS,
                    ]
                }
            elif period_type_abbr == "y":
                period_type = PriceHistory.PeriodType.YEAR
                valid_periods = {
                    x.value: x
                    for x in [
                        PriceHistory.Period.ONE_YEAR,
                        PriceHistory.Period.TWO_YEARS,
                        PriceHistory.Period.THREE_YEARS,
                        PriceHistory.Period.FIVE_YEARS,
                        PriceHistory.Period.TEN_YEARS,
                        PriceHistory.Period.FIFTEEN_YEARS,
                        PriceHistory.Period.TWENTY_YEARS,
                    ]
                }
            else:
                period_type = PriceHistory.PeriodType.DAY
                period_num = PriceHistory.Period.ONE_DAY
                valid_periods = {
                    PriceHistory.Period.ONE_DAY.value: PriceHistory.Period.ONE_DAY
                }
            if period_num not in valid_periods:
                err_msg = f"Invalid period: {period}. Valid values are "
                for valid_period in valid_periods.keys():
                    err_msg += f"{valid_period}{period_type_abbr}, "
                err_msg = err_msg[:-2] + "."
                period_val = list(valid_periods.values())[0]
                err_msg += (
                    f"The period will be set as {period_val.value} {period_type.value}."
                )
                self.logger.warning(err_msg)
            else:
                period_val = valid_periods[period_num]
            time_text = f"last {period_num} {period_type.value}"
        else:
            period_type = PriceHistory.PeriodType.DAY
            period_val = PriceHistory.Period.ONE_DAY
            time_text = f"from {start.date()} to {end.date()}"

        # Setting interval
        interval = self.interval if interval is None else interval
        interval_type = interval[-1].lower()
        interval_num = int(re.search(r"\d+", interval).group())
        if interval_type == "m":
            interval_type = PriceHistory.FrequencyType.MINUTE
            valid_intervals = {
                x.value: x
                for x in [
                    PriceHistory.Frequency.EVERY_MINUTE,
                    PriceHistory.Frequency.EVERY_FIVE_MINUTES,
                    PriceHistory.Frequency.EVERY_TEN_MINUTES,
                    PriceHistory.Frequency.EVERY_FIFTEEN_MINUTES,
                    PriceHistory.Frequency.EVERY_THIRTY_MINUTES,
                ]
            }
        elif interval_type == "d":
            interval_type = PriceHistory.FrequencyType.DAILY
            valid_intervals = {
                PriceHistory.Frequency.DAILY.value: PriceHistory.Frequency.DAILY
            }
        elif interval_type == "w":
            interval_type = PriceHistory.FrequencyType.WEEKLY
            valid_intervals = {
                PriceHistory.Frequency.WEEKLY.value: PriceHistory.Frequency.WEEKLY
            }
        elif interval_type == "M":
            interval_type = PriceHistory.FrequencyType.MONTHLY
            valid_intervals = {
                PriceHistory.Frequency.MONTHLY.value: PriceHistory.Frequency.MONTHLY
            }
        else:
            interval_type = PriceHistory.FrequencyType.DAILY
            valid_intervals = {
                PriceHistory.Frequency.DAILY.value: PriceHistory.Frequency.DAILY
            }
        if interval_num not in valid_intervals:
            err_msg = (
                f"Invalid interval: {interval_num}{interval_type}. Valid values are "
            )
            for valid_interval in valid_intervals.keys():
                err_msg += f"{valid_interval}{interval_type}, "
            err_msg = err_msg[:-2] + "."
            raise ValueError(err_msg)
        else:
            interval_val = valid_intervals[interval_num]

        ticker_list = self.tickers if isinstance(tickers, list) else [tickers]
        res_df = pd.DataFrame()
        for ticker in ticker_list:
            self.logger.info(
                f"Getting {ticker} {interval_val.value} {interval_type.value} {time_text} data..."
            )

            res = self.client.get_price_history(
                symbol=ticker,
                period_type=period_type,
                period=period_val,
                frequency_type=interval_type,
                frequency=interval_val,
                start_datetime=start,
                end_datetime=end,
                need_extended_hours_data=True,
            )
            candle_df = pd.DataFrame.from_dict(res.json()["candles"])
            candle_df["datetime"] = (
                pd.to_datetime(candle_df["datetime"], unit="ms")
                .dt.tz_localize("UTC")
                .dt.tz_convert("America/New_York")
                .dt.tz_localize(None)
            )
            candle_df.columns = [x.capitalize() for x in candle_df.columns]
            candle_df["Adj Close"] = candle_df["Close"]
            candle_df = candle_df.sort_index()
            if res_df.empty:
                res_df = candle_df
            else:
                if not isinstance(res_df.columns, pd.MultiIndex):
                    res_df.columns = pd.MultiIndex.from_arrays(
                        [
                            res_df.columns,
                            [ticker_list[0]] * len(res_df.columns),
                        ]
                    )
                candle_df.columns = pd.MultiIndex.from_arrays(
                    [
                        candle_df.columns,
                        [ticker] * len(candle_df.columns),
                    ]
                )
                candle_df = candle_df.sort_index(axis=1)
                res_df = res_df.sort_index(axis=1).merge(candle_df, on="Datetime")
        return res_df.set_index("Datetime")


if __name__ == "__main__":
    import json

    with open("credentials/schwab/api.json") as f:
        credentials = json.load(f)
    grabber = SchwabGrabber(
        api_key=credentials["api_key"],
        api_secret=credentials["api_secret"],
        tickers="AAPL",
        interval="1m",
    )
    df = grabber.getHistoricalData()
    df.head()
