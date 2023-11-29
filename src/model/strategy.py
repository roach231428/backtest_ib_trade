import abc
from typing import Dict, List

import numpy as np
import pandas as pd

from model.broker import BrokerBase
from model.data_grabber import DataGrabberBase


class Parameter:
    pass


class StrategyBase(abc.ABC):
    cash_left: float = 0
    paras: Dict[str, int | float]
    grabbers: Dict[str, DataGrabberBase]
    broker: BrokerBase
    p: Parameter

    def __init__(self):
        self.cash_left = 0
        self.paras = {}
        self.datas: pd.Series[pd.DataFrame] = pd.Series()
        self.grabbers = dict()
        self.broker = None
        self.p = Parameter()

    @abc.abstractmethod
    def next(self) -> List[dict]:
        """
        Return the instrument from strategy.
        Returns a list of orders to feed to the broker.
        """

        raise NotImplementedError

    def setBroker(self, broker: BrokerBase):
        self.broker = broker

    @abc.abstractmethod
    def indicatorProcess(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        An abstract method that processes the given data.

        Args:
            data (pd.DataFrame): The input data in the format of Yahoo Finance dataframe to
                                 be processed.

        Returns:
            pd.DataFrame: The processed data.

        Raises:
            NotImplementedError: If the method is not implemented by the derived class.
        """
        raise NotImplementedError

    def setParameters(self, single_para: Dict[str, int | float]) -> None:
        """
        Update the parameters of the object with the given dictionary.

        Parameters:
            para (Dict[str, int|float]): A dictionary containing the new parameter values.

        Returns:
            None
        """
        self.paras.update(single_para)

    def addGrabber(self, grabber: DataGrabberBase):
        self.grabbers[grabber.name] = grabber

    def addData(self, data: pd.DataFrame, name: str | None = None):
        if name is None:
            self.datas = pd.concat([self.datas, pd.Series({len(self.datas): data})])
        else:
            self.datas[name] = data

    def cross_over(self, data1: pd.Series, data2: pd.Series) -> pd.Series:
        """
        Calculates the crossover points between two data series.

        Args:
            data1 (pd.Series): The first data series.
            data2 (pd.Series): The second data series.

        Returns:
            pd.Series: A series containing the crossover points. A value of 0 indicates
                       no crossover, 1 indicates a upward crossover, and -1 indicates
                       a downward crossover.
        """

        diff = data1 - data2
        crossover = pd.Series(np.zeros(len(diff), dtype=int), index=diff.index)
        crossover.iloc[0] = 0
        crossover[(diff.shift(1) < 0) & (diff > 0)] = 1
        crossover[(diff.shift(1) > 0) & (diff < 0)] = -1
        return crossover
