import aiofiles
import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Callable

import pandas as pd
import schwab
import pytz
import json

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
        tickers: str | List[str],
        interval: str,
        period: str = "max",
        name: str = "",
        token_path: str = "credentials/schwab/token.json",
        data_dir: str = "./data",
        callback_url: str = "https://127.0.0.1:8182",
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
            # self.client = schwab.auth.client_from_manual_flow(
            #     api_key=api_key,
            #     app_secret=api_secret,
            #     callback_url=callback_url,
            #     token_path=token_path,
            # )
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
        self.stream_client = schwab.streaming.StreamClient(self.client, account_id=account)
        self.locks: Dict[str, asyncio.Lock] = dict()
        self.data_dir = data_dir

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

    def convert_timestamp(self, ts: int | str, tz: str = "America/New_York") -> datetime:
        if isinstance(ts, str):
            ts = int(ts)
        if ts > 1e12:
            ts = int(ts / 1000)
        return datetime.fromtimestamp(ts, tz=pytz.timezone(tz)).replace(tzinfo=None)

    async def async_append_to_csv(self, df: pd.DataFrame, filename: str):
        if filename not in self.locks:
            self.locks[filename] = asyncio.Lock()
        lock = self.locks[filename]
        async with self.acquire_lock(lock):
            file_exists = os.path.exists(filename)
            async with aiofiles.open(filename, mode='a') as f:
                # Convert DataFrame to CSV string
                csv_buffer = df.to_csv(index=False, header=not file_exists, mode='a')
                await f.write(csv_buffer)

    async def sub_l2_book_eq(self, handle_func: Callable, tickers: str | List[str] = []):
        if len(tickers) == 0:
            tickers = self.tickers
        await self.stream_client.login()
        # Always add handlers before subscribing because many streams start sending
        # data immediately after success, and messages with no handlers are dropped.
        self.stream_client.add_nasdaq_book_handler(handle_func)
        await self.stream_client.nasdaq_book_subs(tickers)

        self.stream_client.add_nyse_book_handler(handle_func)
        await self.stream_client.nyse_book_subs(tickers)

        while True:
            await self.stream_client.handle_message()

    async def sub_l1_eq(self, handle_func: Callable, tickers: str | List[str]= []):
        if len(tickers) == 0:
            tickers = self.tickers
        await self.stream_client.login()
        # Always add handlers before subscribing because many streams start sending
        # data immediately after success, and messages with no handlers are dropped.
        self.stream_client.add_level_one_equity_handler(handle_func)
        await self.stream_client.level_one_equity_subs(tickers)
        while True:
            await self.stream_client.handle_message()

    async def sub_both_books_eq(
        self,
        handle_func_l1: Callable,
        handle_func_l2: Callable,
        tickers: str | List[str] = [],
    ):
        if len(tickers) == 0:
            tickers = self.tickers
        await self.stream_client.login()
        # Always add handlers before subscribing because many streams start sending
        # data immediately after success, and messages with no handlers are dropped.
        self.stream_client.add_nasdaq_book_handler(handle_func_l2)
        await self.stream_client.nasdaq_book_subs(tickers)

        self.stream_client.add_nyse_book_handler(handle_func_l2)
        await self.stream_client.nyse_book_subs(tickers)

        self.stream_client.add_level_one_equity_handler(handle_func_l1)
        await self.stream_client.level_one_equity_subs(tickers)

        while True:
            await self.stream_client.handle_message()

    async def parse_orderbook_message(self, message: Dict):
        for content in message["content"]:
            ticker = content["key"]
            dt = self.convert_timestamp(content["BOOK_TIME"])

            df_bids = pd.json_normalize(content["BIDS"], 'BIDS', ['BID_PRICE', 'TOTAL_VOLUME', 'NUM_BIDS'])
            df_bids = df_bids.rename({"BID_PRICE": "PRICE", "BID_VOLUME": "VOLUME", "NUM_BIDS": "NUM_TRADES"}, axis=1)
            df_bids["BID_ASK"] = "B"
            df_asks = pd.json_normalize(content["ASKS"], 'ASKS', ['ASK_PRICE', 'TOTAL_VOLUME', 'NUM_ASKS'])
            df_asks = df_asks.rename({"ASK_PRICE": "PRICE", "ASK_VOLUME": "VOLUME", "NUM_ASKS": "NUM_TRADES"}, axis=1)
            df_asks["BID_ASK"] = "A"
            df_book = pd.concat([df_bids, df_asks])
            if df_book.empty:
                continue
            df_book["DATETIME"] = dt
            df_book = df_book.drop(["SEQUENCE"], axis=1)
            # df_book["TIMESTAMP"] = content["BOOK_TIME"]

            save_dir = os.path.join(self.data_dir, "l2_book", str(dt.year), str(dt.month), str(dt.day))
            os.makedirs(save_dir, exist_ok=True)
            opt_file = os.path.join(save_dir, f"l2_book_{ticker}_{dt.strftime('%Y%m%d')}.csv")
            await self.async_append_to_csv(df_book, opt_file)

            if ticker == "QQQ" and message["service"] == "NASDAQ_BOOK":
                self.print_orderbook(ticker, dt, df_book)

    async def parse_l1_book_message(self, message: Dict):
        for content in message["content"]:
            row = pd.Series(content)
            if len(row) == 0:
                continue
            ticker = content["key"]
            row = pd.Series({
                "DATETIME": self.convert_timestamp(content["QUOTE_TIME_MILLIS"]),
                "BID_PRICE": content["BID_PRICE"],
                "ASK_PRICE": content["ASK_PRICE"],
                "LAST_PRICE": content["LAST_PRICE"],
                "HIGH_PRICE": content["HIGH_PRICE"],
                "LOW_PRICE": content["LOW_PRICE"],
                "OPEN_PRICE": content["OPEN_PRICE"],
                "CLOSE_PRICE": content["CLOSE_PRICE"],
                "BID_VOLUME": content["BID_SIZE"],
                "ASK_VOLUME": content["ASK_SIZE"],
                "LAST_SIZE": content["LAST_SIZE"],
                "TOTAL_VOLUME": content["TOTAL_VOLUME"],
            })
            # row.rename({"QUOTE_TIME_MILLIS": "DATETIME"}, inplace=True)
            # row.index = [x.replace("_SIZE", "_VOLUME") for x in row.index]
            # row["DATETIME"] = self.convert_timestamp(row["DATETIME"])
            # row.drop(
            #     ["key", "delayed", "assetMainType", "assetSubType", "cusip"],
            #     axis=0,
            #     inplace=True,
            #     errors="ignore",
            # )
            dt = row["DATETIME"]
            save_dir = os.path.join(self.data_dir, "l1_book", str(dt.year), str(dt.month), str(dt.day))
            os.makedirs(save_dir, exist_ok=True)
            opt_file = os.path.join(save_dir, f"l1_book_{ticker}_{dt.strftime('%Y%m%d')}.csv")
            # await self.async_append_to_csv(pd.DataFrame([row]), opt_file)

    @asynccontextmanager
    async def acquire_lock(self, lock: asyncio.Lock):
        await lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def print_message(self, message):
        print(json.dumps(message, indent=4))

    def print_orderbook(self, ticker: str, dt: datetime, df_book: pd.DataFrame):
        os.system('cls')
        print(ticker, "\t", dt)
        print("  PRICE ", "NUM_TRADES", " EXCHANGE", "VOLUME")
        print(
            df_book
            .query("BID_ASK == 'A'")[["PRICE", "NUM_TRADES", "EXCHANGE", "VOLUME"]]
            .sort_values("PRICE", ascending=False)
            .to_string(index=False, header=False, col_space=7)
        )
        print("-------------------------------------")
        print(
            df_book
            .query("BID_ASK == 'B'")[["PRICE", "NUM_TRADES", "EXCHANGE", "VOLUME"]]
            .sort_values("PRICE", ascending=False)
            .to_string(index=False, header=False, col_space=7)
        )
        print("")


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
