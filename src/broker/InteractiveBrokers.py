import sys

import ib_insync as ib
import pandas as pd

sys.path.append("src")
import time
from typing import Dict, List, Union

from backtrader import CommInfoBase
from ib_insync.order import UNSET_DOUBLE

from model.models import Position
from src.broker.base import BrokerBase


class CommissionSchemeFixed(CommInfoBase):
    params = dict(
        commission=0.5,  # percentage
        stocklike=True,
        commtype=CommInfoBase.COMM_PERC,
        minCommission=1,
        maxCommissionRatio=0.01,
    )

    def _getcommission(self, size, price, pseudoexec):
        if size > 0:  # Buy
            return (
                min(
                    max(size * self.p.commission, self.p.minCommission),
                    size * price * self.p.maxCommissionRatio,
                )
                + 0.000119 * size
            )
        elif size < 0:  # Sell
            size = abs(size)
            return (
                min(
                    max(size * self.p.commission, self.p.minCommission),
                    size * price * self.p.maxCommissionRatio,
                )
                + 0.000119 * size
                + 0.0000221 * size * price
            )
        else:
            return 0  # If size == 0


class CommissionSchemeTiered(CommInfoBase):
    pass


class InteractiveBrokers(BrokerBase):
    _ib: ib.IB
    _account: str

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        clientId: int = 1,
        account: str = "",
        **kwargs,
    ):
        self._ib = ib.IB()
        self._host = host
        self._port = port
        self._clientId = clientId
        self._account = account

        self.orderbyid: Dict[int, ib.Order] = dict()
        self.positions: pd.DataFrame = pd.DataFrame()
        self.cash: float = 0

        self.start(**kwargs)

    def __del__(self):
        self._ib.sleep(0.01)
        self.stop()

    def start(self, **kwargs) -> None:
        self.reconnect(**kwargs)
        if self._account == "":
            self._account = self.getAccounts()[0]
        # self._ib.reqAccountUpdates(self._account)
        self.cash = self.getCash()
        self.positions = self.getPositionsDF()

    def stop(self) -> None:
        self._ib.disconnect()

    def reconnect(self, **kwargs) -> None:
        if self._ib.isConnected():
            self._ib.disconnect()
            self.sleep(0.01)
        self._connect(**kwargs)
        if self._account == "":
            self._account = self._ib.managedAccounts()[0]

    def _connect(self, **kwargs) -> None:
        for i in range(5):
            try:
                self._ib.connect(self._host, self._port, self._clientId, **kwargs)
                return
            except Exception:
                time.sleep(1)
        raise Exception("Failed to connect to Interactive Brokers.")

    # def now(self) -> datetime.datetime:
    #     return self._ib.reqCurrentTime()

    def sleep(self, secs: float) -> None:
        self._ib.sleep(secs)

    def update(self) -> None:
        self.sleep(0)
        self.cash = self.getCash()
        self.positions = self.getPositionsDF()

    def getCash(self) -> float:
        account_info = self.getAccountSummary()
        return float(account_info.loc["TotalCashValue", "value"])

    def getTotalValue(self) -> float:
        account_info = self.getAccountSummary()
        return float(account_info.loc["NetLiquidation", "value"])

    def getAccounts(self) -> List[str]:
        return self._ib.managedAccounts()

    def getAccountSummary(self, account: str = "") -> pd.DataFrame:
        if account == "":
            return pd.DataFrame(self._ib.accountSummary(self._account)).set_index("tag")
        elif account.lower() == "all":
            return pd.DataFrame(self._ib.accountSummary())
        else:
            return pd.DataFrame(self._ib.accountSummary(account)).set_index("tag")

    def makeStockContract(self, symbol: str, exchange: str, currency: str) -> ib.Stock:
        return ib.Stock(symbol, exchange, currency)

    def makeFutureContract(
        self, symbol: str, exchange: str, currency: str
    ) -> ib.Future:
        return ib.Future(symbol, exchange=exchange, currency=currency)

    def makeOrder(
        self,
        action: str = "",
        qty: float = 0,
        orderType: str = "",
        price: float = UNSET_DOUBLE,
        stop_price: float = UNSET_DOUBLE,
        outsideRth: bool = True,
        **kwargs,
    ) -> ib.Order:
        """
        Also see https://interactivebrokers.github.io/tws-api/classIBApi_1_1Order.html
        for more argument details.
        """
        return ib.Order(
            action=action,
            totalQuantity=qty,
            orderType=orderType,
            lmtPrice=price,
            auxPrice=stop_price,
            outsideRth=outsideRth,
            **kwargs,
        )

    def placeStockOrder(
        self,
        instrument: str,
        qty: float,
        orderType: str = "MKT",
        exchange: str = "SMART",
        price: float = UNSET_DOUBLE,
        stop_price: float = UNSET_DOUBLE,
        **kwargs,
    ) -> int:

        symbol, currency, trade_type = instrument.upper().split("-")
        contract: Union[ib.Stock, ib.Future] = None
        if trade_type.lower() == "spot":
            contract = self.makeStockContract(
                symbol=symbol, exchange=exchange, currency=currency
            )
        elif trade_type.lower() == "perp":
            contract = self.makeFutureContract(
                symbol=symbol, exchange=exchange, currency=currency
            )
        else:
            raise ValueError(f"Unknown trade type: {trade_type}")

        if qty > 0:
            action = "BUY"
        else:
            action = "SELL"
            qty = abs(qty)
        order = self.makeOrder(
            action=action,
            qty=qty,
            orderType=orderType,
            price=price,
            stop_price=stop_price,
            **kwargs,
        )
        trade = self._ib.placeOrder(contract, order)
        self._ib.waitOnUpdate()
        order_id = trade.order.orderId
        self.orderbyid[order_id] = order
        order_status = trade.orderStatus
        log = trade.log[-1]
        msg = f"Order {order_id} {order_status.status}"
        if order_status.status == ib.OrderStatus.Filled:
            msg += f" {ib.OrderStatus.Filled} position at price {order_status.avgFillPrice}"
        msg += f". Timestamp: {log.time}"
        self.logger.info(msg)
        if log.errorCode != 0:
            self.logger.error(log.message)
        return order_id

    def closePosition(self, tickers: str | List[str] = []) -> None:
        if isinstance(tickers, str):
            tickers = [tickers]
        positions = self.getHoldings(tickers)
        if len(tickers) == 0:
            tickers = positions.keys()
        for ticker in tickers:
            if ticker not in positions:
                continue
            self.placeStockOrder(
                instrument=f"{ticker}-USD-SPOT",
                qty=-positions[ticker],
                orderType="MKT",
            )

    def getTradesById(self, order_ids: List[int] = []) -> Dict[int, ib.Trade | None]:
        all_trades = self._ib.trades()
        res = dict()
        for id in order_ids:
            res[id] = next((x for x in all_trades if x.order.orderId == id), None)
        return res

    def getOrdersById(self, order_ids: List[int] = []) -> Dict[int, ib.Order | None]:
        trades = self.getTradesById(order_ids)
        return {
            oid: trades[oid].order if trades[oid] is not None else None
            for oid in order_ids
        }

    def cancelOrders(self, order_ids: List[int] = []) -> None:
        open_orders = [x.order for x in self._ib.openTrades()]
        if len(order_ids) == 0:
            order_ids = list(set([x.orderId for x in open_orders]))
        else:
            order_ids = list(set([int(id) for id in order_ids]))
        for order in open_orders:
            if order.orderId in order_ids:
                self._ib.cancelOrder(order)
                self.logger.warning(f"Canceled order {order.orderId}.")
                self.sleep(0.001)

    def isPending(self, order_id: int) -> bool:
        trade = self.getTradesById([order_id])[order_id]
        if trade is None:
            self.logger.error(f"Order {order_id} not found.")
            return False
        res = trade.orderStatus.status in {
            ib.OrderStatus.ApiPending,
            ib.OrderStatus.PendingSubmit,
            ib.OrderStatus.Inactive,
        }
        return res

    def isSumbitted(self, order_id: int) -> bool:
        trade = self.getTradesById([order_id])[order_id]
        if trade is None:
            self.logger.error(f"Order {order_id} not found.")
            return False
        return trade.orderStatus.status in {
            ib.OrderStatus.Submitted,
            ib.OrderStatus.PreSubmitted,
        }

    def isFilled(self, order_id: int) -> bool:
        trade = self.getTradesById([order_id])[order_id]
        if trade is None:
            self.logger.error(f"Order {order_id} not found.")
            return False
        return trade.orderStatus.status == ib.OrderStatus.Filled

    def isCancelled(self, order_id: int) -> bool:
        trade = self.getTradesById([order_id])[order_id]
        if trade is None:
            self.logger.error(f"Order {order_id} not found.")
            return False
        res = trade.orderStatus.status in {
            ib.OrderStatus.PendingCancel,
            ib.OrderStatus.Cancelled,
            ib.OrderStatus.ApiCancelled,
        }
        return res

    def getFilledPrice(self, order_id: int) -> float:
        trade = self.getTradesById([order_id])[order_id]
        if trade is None:
            self.logger.error(f"Order {order_id} not found.")
            return 0.0
        return trade.orderStatus.avgFillPrice

    def getPositions(self, tickers: List[str] = []) -> Dict[str, Position]:
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

    def getExecuteOrdersDF(self) -> pd.DataFrame:
        executed = self._ib.executions()
        return pd.DataFrame(
            list(
                map(
                    lambda ex: pd.Series(
                        {
                            "trade_id": ex.permId,
                            "order_id": ex.orderId,
                            "exchange": ex.exchange,
                            "amount": ex.shares,
                            "price": ex.price,
                            "timestamp": ex.time,
                        }
                    ),
                    executed,
                )
            )
        )

    def getOpenOrdersDF(self) -> pd.DataFrame:
        opens = self._ib.openTrades()
        return pd.DataFrame(
            list(
                map(
                    lambda op: pd.Series(
                        {
                            "id": op.order.orderId,
                            "symbol": op.contract.symbol,
                            "currency": op.contract.currency,
                            "action": op.order.action,
                            "amount": op.order.totalQuantity,
                            "type": op.order.orderType,
                            "lmtPrice": op.order.lmtPrice,
                            "secType": op.contract.secType,
                            "status": op.orderStatus.status,
                        }
                    ),
                    opens,
                )
            )
        )

    def getPositionsDF(self) -> pd.DataFrame:
        portfolio = self._ib.portfolio()
        return pd.DataFrame(
            list(
                map(
                    lambda port: pd.Series(
                        {
                            "symbol": port.contract.symbol,
                            "currency": port.contract.currency,
                            "position": port.position,
                            "market_price": port.marketPrice,
                            "avg_cost": port.averageCost,
                            "market_value": port.marketValue,
                            "unrealized_pnl": port.unrealizedPNL,
                            "realized_pnl": port.realizedPNL,
                            "primary_exchange": port.contract.primaryExchange,
                        }
                    ),
                    portfolio,
                )
            )
        )


if __name__ == "__main__":
    test_ib = InteractiveBrokers(clientId=1)
    order_id1 = test_ib.placeStockOrder(
        instrument="GOOGL-USD-SPOT", qty=1, orderType="MKT"
    )
    order_id2 = test_ib.placeStockOrder(
        instrument="TQQQ-USD-SPOT", qty=-1, orderType="LMT", price=39
    )
    test_ib.isPending(order_id1)
    print(test_ib.getOpenOrdersDF())
    print(test_ib.getExecuteOrdersDF())
    test_ib.cancelOrders([order_id1, order_id2])
    test_ib.isCancelled(order_id1)
    print(test_ib.getOpenOrdersDF())
    test_ib.cancelOrders()
    print(test_ib.getOpenOrdersDF())
    positions = test_ib.getPositionsDF()
    print(positions)

    test_ib._ib.reqHistoricalData(
        test_ib.makeStockContract("GOOGL", "NYSE", "USD"),
        endDateTime="",
        durationStr="1 D",
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=True,
    )
    test_ib._ib.disconnect()
    contract = test_ib.makeStockContract("AAPL", "SMART", "USD")
    ticker = test_ib._ib.reqMktData(contract=contract, snapshot=False)
    ticker.bid

    del test_ib
