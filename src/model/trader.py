import sys

sys.path.append("src")
import abc
import datetime
import logging
from typing import Dict, List

import pytz

from model.broker import BrokerBase
from model.data_grabber import DataGrabberBase
from model.strategy import StrategyBase


class TraderBase(abc.ABC):
    def __init__(self, tickers: List[str], sleep_interval: float, buffer_time: float):
        """
        Initialize the YourClassName instance with provided parameters.

        Parameters:
            tickers (List[str]): List of ticker names or symbols to be traded.
            sleep_interval (float): The sleep time interval, in seconds, for each loop.
            buffer_time (float): The time, in seconds, to allow for data retrieval and processing.

        Attributes:
            tickers (List[str]): List of tickers to be traded.
            sleep_interval (float): Sleep time interval for each loop.
            strategy (StrategyBase): Strategy to be used for trading (initialized as None).
            broker (BrokerBase): Broker for executing trading orders (initialized as None).
            grabbers (Dict[str, DataGrabberBase]): Data grabbers for collecting ticker data.
            break_flag (bool): Flag to indicate if the program should stop execution.
            buffer_time (float): Time allowed for data retrieval and processing.

        This constructor initializes the class with the provided parameters and default
        attribute values.
        """
        self.tickers = tickers
        self.sleep_interval = sleep_interval
        self.strategy: StrategyBase = None
        self.broker: BrokerBase = None
        self.grabbers: Dict[str, DataGrabberBase] = dict()
        self.break_flag: bool = False
        self.buffer_time = buffer_time

    def setStrategy(self, strategy: StrategyBase) -> None:
        self.strategy = strategy

    def setBroker(self, broker: BrokerBase) -> None:
        self.broker = broker

    def addGrabber(self, grabber: DataGrabberBase) -> None:
        self.grabbers[grabber.name] = grabber

    def start(self) -> None:
        """
        Starts the trading system by performing the necessary initialization steps.

        Raises:
            Exception: If the broker is not set yet, please set it using setBroker().
            Exception: If the strategy is not set yet, please set it using setStrategy().
            Exception: If no data grabber is added, please add one using addGrabber().

        Returns:
            None
        """

        if self.broker is None:
            raise Exception("Broker is not set yet. Please set with setBroker().")
        logging.info("Initializing broker...")
        self.broker.start()

        if self.strategy is None:
            raise Exception("Strategy is not set yet. Please set with setStrategy().")
        self.strategy.setBroker(self.broker)
        logging.info("Broker added to strategy.")

        if len(self.grabbers) == 0:
            raise Exception("No data grabber. Please add using addGrabber().")
        for grabber in self.grabbers.values():
            self.strategy.addGrabber(grabber)
        logging.info("Data grabber added to strategy.")
        logging.info("Initializing strategy...")
        self.updateData()
        self.strategy.init()
        self.run()

    @abc.abstractmethod
    def run(self) -> None:
        """
        Runs the main loop of the trading program.

        This function continuously executes the trading strategy until a break condition is met.
        The break condition is set when the `break_flag` attribute is True.

        Parameters:
            None

        Returns:
            None
        """

        raise NotImplementedError

    def stop(self) -> None:
        logging.info("Stopping trade...")
        self.broker.stop()

    def updateData(
        self, time: datetime.datetime = datetime.datetime.now(pytz.UTC)
    ) -> Dict[str, int]:
        """
        Update data from multiple grabbers based on the specified time and their defined intervals.

        Args:
            self (object): The instance of the class containing the grabbers and strategy.
            time (datetime.datetime, optional): The current time to use as a reference
                for the update check. Defaults to the current UTC time.

        Returns:
            Dict[str, int]: A dictionary mapping grabber names to update results, where:
                - 0: Data was up-to-date as the specified interval has not passed.
                - 1: Data was updated.
                - 2: Data was not updated, but it's within the next interval, indicating
                    that the data may not have refreshed yet.
                - (-1): Data was updated, but it's considered stale.
        """
        res = dict()
        for name, grabber in self.grabbers.items():
            result = grabber.updateData(time)
            if result == 1:
                self.strategy.addData(grabber.data, grabber.name)
            res[name] = result
        return res
