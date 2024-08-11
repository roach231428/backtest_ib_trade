import abc
import datetime
import logging
import time
from typing import Dict, List

import pytz
from pandas import DataFrame

from ..model.models import Position
from .types import OrderType, TimeInForce


class BrokerBase(abc.ABC):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @abc.abstractmethod
    def start(self, **kwargs) -> None:
        """
        Start the broker.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        """
        Stops the execution of the program.
        """
        raise NotImplementedError

    def sleep(self, secs: float) -> None:
        """
        Suspends the execution of the current thread for a specified number of seconds.

        Parameters:
            secs (float): The number of seconds to sleep.

        Returns:
            None
        """

        time.sleep(secs)

    def now(self) -> datetime.datetime:
        """
        Returns the current UTC datetime.

        :return: A datetime object representing the current UTC datetime.
        :rtype: datetime.datetime
        """
        return datetime.datetime.now(pytz.UTC)

    @abc.abstractmethod
    def update(self) -> None:
        """
        Update the broker information.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def getCash(self) -> float:
        """
        Get total current cash.

        Returns:
            float: The cash value.
        """

        raise NotImplementedError

    def getPositions(self, tickers: List[str] = []) -> Dict[str, Position]:
        """
        Retrieves the positions for the given tickers.
        If no tickers are provided, all positions are retrieved.

        Parameters:
            tickers (List[str], optional): A list of tickers. Defaults to [].

        Returns:
            Dict[str, Position]: A dictionary containing the positions for each ticker.
                The Position must contain the following parameters:
                    symbol: str
                    currency: str
                    position: float
                    avg_cost: float
                    unrealized_pnl: float
        """
        all_positions = self.getPositionsDF()
        if all_positions.empty:
            return {t: Position(t) for t in tickers}
        else:
            all_positions.set_index("symbol", inplace=True)
            if tickers == []:
                tickers = all_positions.index
            res = {
                t: Position(
                    symbol=t,
                    currency=all_positions.loc[t, "currency"],
                    position=all_positions.loc[t, "position"],
                    avg_cost=all_positions.loc[t, "avg_cost"],
                    unrealized_pnl=all_positions.loc[t, "unrealized_pnl"],
                )
                if t in all_positions.index
                else Position(t)
                for t in tickers
            }
            return res

        raise NotImplementedError

    @abc.abstractmethod
    def getPositionsDF(self) -> DataFrame:
        """
        Retrieves the positions from the broker and returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the following columns:
                - symbol: str
                - currency: str
                - position: float
                - market_price: float
                - avg_cost: float
                - market_value: float
                - unrealized_pnl: float
                - realized_pnl: float
                - primary_exchange: str
        """
        raise NotImplementedError

    def getHoldings(self, tickers: List[str]) -> Dict[str, float]:
        positions = self.getPositions(tickers)
        return {t: p.position for t, p in positions.items()}

    def getCosts(self, tickers: List[str]) -> Dict[str, float]:
        positions = self.getPositions(tickers)
        return {t: p.avg_cost for t, p in positions.items()}

    @abc.abstractmethod
    def placeStockOrder(
        self,
        instrument: str,
        qty: float,
        order_type: OrderType = OrderType.MARKET,
        tif: TimeInForce = TimeInForce.DAY,
        exchange: str = "",
        price: float = 0,
        stop_price: float = 0,
        **kwargs,
    ) -> str:
        """
        A function to place one stock order and return The order ID.

        Parameters:
            - instrument (str): The instrument to place the stock order for.
                The instrument must be in the format "ticker-symbol-currency".
                Example: "GOOGL-USD-SPOT"
            - qty (float): The quantity of stocks to order.
            - order_type (OrderType): The type of order to place.
            - price (float, optional): The price at which to place the order.
            - stop_price (float, optional): The stop price for the order.
            - **kwargs: Additional keyword arguments.

        Returns:
            str: The ID of the placed order.
        """

        raise NotImplementedError

    def closePosition(self, tickers: str | List[str] = []) -> None:
        """
        Closes a position for a given tickers.
        If no tickers is provided, all positions are closed.

        Parameters:
            tickers (str|List[str], optional): The instrument(s) to close the position for.
              If not specified, all positions will be closed. Defaults to [].

        Returns:
            None: This function does not return anything.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        positions = self.getHoldings(tickers)
        if len(tickers) == 0:
            tickers = list(positions.keys())
        for ticker in tickers:
            if ticker not in positions:
                continue
            self.placeStockOrder(
                instrument=f"{ticker}-USD-SPOT",
                qty=-positions[ticker],
                orderType="MKT",
            )

    @abc.abstractmethod
    def cancelOrders(self, order_ids: List[str] = []) -> None:
        """
        Cancel a list of orders. If no order IDs are provided, all open orders are cancelled.

        Args:
            order_ids (List[str], optional): The list of order IDs to cancel. Defaults to [].
        """

        raise NotImplementedError

    @abc.abstractmethod
    def getOrderStatus(self, order_id: str) -> str:
        """
        Get the status of an order.

        Args:
            order_id (str): The ID of the order.

        Returns:
            str: The status of the order.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def isPending(self, order_id: str) -> bool:
        """
        Check if the order with the given order_id is pending.

        Parameters:
            order_id (str): The ID of the order to check.

        Returns:
            bool: True if the order is pending, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def isFilled(self, order_id: str) -> bool:
        """
        Check if the order with the given order_id is filled.

        Args:
            order_id (str): The ID of the order to check.

        Returns:
            bool: True if the order is filled, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def isCancelled(self, order_id: str) -> bool:
        """
        Check if an order is canceled.

        Args:
            order_id (str): The ID of the order.

        Returns:
            bool: True if the order is canceled, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def getFilledPrice(self, order_id: str) -> float:
        """
        Get the average filled price of an order.

        Parameters:
            order_id (str): The ID of the order.

        Returns:
            float: The average filled price of the order.

        Raises:
            NotImplementedError: This method is abstract and must be implemented by a subclass.
        """
        raise NotImplementedError
