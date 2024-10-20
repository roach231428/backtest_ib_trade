import abc
import logging
import math
from typing import Dict, List

import backtrader as bt
import numpy as np
import pandas as pd

from src.broker.base import BrokerBase
from src.grabber.base import DataGrabberBase


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
        self.logger = logging.getLogger(__name__)

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


class BtStrategyBase(bt.Strategy):
    # Trading record
    def notify_order(self, order: bt.order.OrderData):
        if order.status == order.Completed:
            time = bt.num2date(order.executed.dt)
            self.trade_record["Time"].append(time)
            if order.isbuy():
                self.trade_record["Type"].append("Buy")
            elif order.issell():
                self.trade_record["Type"].append("Sell")
            self.trade_record["Price"].append(order.executed.price)
            self.trade_record["Amount"].append(order.executed.size)
            self.trade_record["Commission"].append(order.executed.comm)
            self.trade_record["Gross Pnl"].append(order.executed.pnl)

    def notify_trade(self, trade=bt.trade.Trade):
        if trade.isclosed:
            self.trade_record["Net Pnl"].append(trade.pnlcomm)
            self.trade_record["Value"].append(self.broker.get_value())
    
    # Calculate positions
    class Sizer(bt.Sizer):
        def _getsizing(self, comminfo, cash, data, isbuy):
            if isbuy:
                return math.floor(cash / data.close * 0.97)
            else:
                cur_position = self.broker.getposition(data).size
                return (
                    cur_position if cur_position != 0 else math.floor(cash / data.open)
                )

    def next(self):
        raise NotImplementedError

    def stop(self):
        # for k,it in self.trade_record.items():
        #     print(k, len(it))
        # trade_record_df = pd.DataFrame(self.trade_record)
        # trade_record_df.to_csv("result/momentum_williamsR.csv", index=False)
        print("End.")
        return self.close()
