import abc
import datetime
import logging
from typing import Dict, List

import pandas as pd
import pytz


class DataGrabberBase(abc.ABC):

    datatime: datetime.datetime

    def __init__(
        self,
        tickers: str | List[str] = "",
        interval: str = "",
        period: str = "max",
        name: str = "",
    ) -> None:
        """
        Initializes an instance of the class.

        Args:
            tickers (str | List[str]): A string or a list of strings representing the tickers.
            interval (str): The time interval between data points (e.g., '1d', '1h', '30m').
            period (str, optional): The time period to retrieve data for (e.g., '1d', '1y', 'max').
            name (str): The name of the instance. If not provided, a default name
                        is generated based on tickers.

        Returns:
            None

        Attributes:
            tickers (str | List[str]): A string or a list of strings representing the tickers.
            interval (str): The time interval between data points.
            period (str): The time period to retrieve data for.
            name (str): The name of the instance.
            data (pd.DataFrame): A Pandas DataFrame to store retrieved data.

        Notes:
            - If 'name' is not provided, it is automatically generated based on 'tickers'
              (or a list of tickers).
        """

        super().__init__()
        self.tickers: str | List[str] = tickers
        self.interval: str = interval
        self.period: str = period
        if name == "":
            self.name = tickers if isinstance(tickers, str) else ", ".join(tickers)
        else:
            self.name = name
        self.data: pd.DataFrame = pd.DataFrame()
        self.datatime: datetime.datetime = datetime.datetime(
            1990, 1, 1, tzinfo=pytz.UTC
        )

    def set_tickers(self, tickers: str | List[str]) -> None:
        self.tickers = tickers

    def add_tickers(self, tickers: str | List[str]) -> None:
        if isinstance(tickers, str):
            if isinstance(self.tickers, str):
                self.tickers = [self.tickers, tickers]
            else:
                self.tickers.append(tickers)
        else:
            if isinstance(self.tickers, str):
                self.tickers = tickers + [self.tickers]
            else:
                self.tickers.extend(tickers)

    def set_interval(self, interval: str) -> None:
        self.interval = interval

    def set_period(self, period: str) -> None:
        self.period = period

    def updateData(
        self, time: datetime.datetime = datetime.datetime.now(pytz.UTC)
    ) -> int:
        """
        Update the data stored in the object if the specified time has passed the
        defined interval since the last update.

        Args:
            time (datetime.datetime, optional): The current time to use as a reference
                for the update check. Defaults to the current UTC time.

        Returns:
            int: An integer indicating the result of the update:
                - 0: Data was up-to-date as the specified interval has not passed.
                - 1: Data was updated.
                - 2: Data was not updated, but it's within the next interval, indicating
                    that the data may not have refreshed yet.
                - (-1): Data was updated, but it's considered stale.

        The function checks if the time elapsed since the last update (self.datatime)
        is greater than or equal to the specified interval. If it is, the function
        fetches new data using 'getHistoricalData' and updates the timestamp. If the
        time difference is less than the interval but greater than half of the interval,
        it returns 2 to indicate that the data might not have refreshed yet. If the time
        difference is less than the interval, it returns -1 to indicate that the data is
        stale and should be updated.

        Note: 'self.interval2seconds' is assumed to be a helper method for converting
        intervals to seconds.
        """
        time_timestamp = time.timestamp()
        sec_diff = time_timestamp - self.datatime.timestamp()
        interval_sec = self.interval2seconds(self.interval)
        if sec_diff < interval_sec:
            return 0
        new_data = self.getHistoricalData()
        if new_data.shape[0] == 0:
            logging.error(f"Getting data {self.name} error. No data retrieved.")
            return -2
        self.data = new_data
        self.datatime = self.data.index[-1].astimezone(pytz.UTC)
        sec_diff = time_timestamp - self.datatime.timestamp()
        interval_sec = self.interval2seconds(self.interval)
        if sec_diff < interval_sec:
            return 1
        elif interval_sec <= sec_diff < 2 * interval_sec:
            # Data may not update yet
            logging.warn(
                f"Data {self.name} is not update yet. Latest update time: {self.datatime}."
            )
            return 2
        else:
            # Data is stale
            logging.error(
                f"Data {self.name} is too old. Latest update time: {self.datatime}"
            )
            return -1

    def interval2seconds(self, interval: str) -> int:
        """
        Converts a time interval represented as a string into the equivalent number of seconds.

        Args:
            interval (str): A time interval string, e.g., "5s" for 5 seconds, "2h" for 2 hours, etc.

        Returns:
            int: The total number of seconds equivalent to the provided interval.

        Raises:
            ValueError: If the provided interval string is not in the expected format.

        Example:
            To convert 3 days to seconds: interval2seconds("3d") will return 259200.

        Supported units:
        - "s" for seconds
        - "m" for minutes
        - "h" for hours
        - "d" for days
        - "w" for weeks
        - "M" for months (assumed as 30 days per month)
        - "y" for years (assumed as 365 days per year)

        Note that the function raises a ValueError if an unsupported
        unit is provided in the interval string.

        """
        unit = interval[-1]
        number = int(interval[:-1])
        if unit == "s":
            return number
        elif unit == "m":
            return number * 60
        elif unit == "h":
            return number * 60 * 60
        elif unit == "d":
            return number * 60 * 60 * 24
        elif unit == "w":
            return number * 60 * 60 * 24 * 7
        elif unit == "M":
            return number * 60 * 60 * 24 * 30
        elif unit == "y":
            return number * 60 * 60 * 24 * 365
        else:
            raise ValueError(f"Invalid interval: {interval}")

    @abc.abstractmethod
    def getHistoricalData(
        self,
        interval: str = "",
        start: str = "",
        end: str = "",
        period: str = "",
    ) -> pd.DataFrame:
        """
        Retrieves historical data for a given ticker symbol. If the start, end, and period
        parameters are not provided, will take the maximum period data for the interval.

        Parameters:
            start (str, optional): The start date of the data.
            end (str, optional): The end date of the data.
            period (str, optional): The time period to retrieve data for.

        Returns:
            pd.DataFrame: A DataFrame containing the historical data with columns
            ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'] and the index
            is the timestamp in datetime.datetime format.

        Raises:
            NotImplementedError: This method needs to be implemented in a subclass.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def getLatestData(self) -> Dict[str, Dict[str, float]]:
        """
        Retrieves the latest data for a list of tickers.

        Returns:
            Dict[str, Dict[str, float]]: A dictionary where the keys are ticker symbols
                and the values are dictionaries containing the following data for each ticker:
                'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'.

        Raises:
            NotImplementedError: This method needs to be implemented in a subclass.
        """

        raise NotImplementedError

    def getLatestCloseData(self) -> float | Dict[str, float]:
        """
        Retrieves the latest close data for the given tickers and interval.

        Returns:
            float | Dict[str, float]: If a single ticker is provided, returns
                                      the latest close value for that ticker.
                                      If a list of tickers is provided,
                                      returns a dictionary mapping each ticker
                                      to its latest close value.
        """

        data = self.getLatestData()
        if isinstance(self.tickers, str):
            return data[self.tickers]["Close"]
        else:
            return {t: data[t]["Close"] for t in self.tickers}
