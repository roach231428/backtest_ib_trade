import logging
from typing import Dict, List

import numpy as np
import pandas as pd
import schwab
from schwab import orders

from .base import BrokerBase
from .types import OrderType, TimeInForce


class CharlesSchwab(BrokerBase):
    client: schwab.client.Client

    order_type_mapping = {
        OrderType.MARKET: orders.common.OrderType.MARKET,
        OrderType.LIMIT: orders.common.OrderType.LIMIT,
        OrderType.STOP: orders.common.OrderType.STOP,
        OrderType.STOP_LIMIT: orders.common.OrderType.STOP_LIMIT,
        OrderType.TRAILING: orders.common.OrderType.TRAILING_STOP,
        OrderType.TRAILING_LIMIT: orders.common.OrderType.TRAILING_STOP_LIMIT,
        OrderType.MARKET_ON_CLOSE: orders.common.OrderType.MARKET_ON_CLOSE,
    }

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        token_path: str = "credentials/schwab/token.json",
        callback_url: str = "https://127.0.0.1:8182",
        account: str = "",
    ):
        self.__api_key = api_key
        self.__api_secret = api_secret
        self._token_path = token_path
        self._callback_url = callback_url
        self._account = account
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def start(self, **kwargs) -> None:
        self.client = schwab.auth.easy_client(
            api_key=self.__api_key,
            app_secret=self.__api_secret,
            callback_url=self._callback_url,
            token_path=self._token_path,
        )
        acc_res = self.client.get_account_numbers().json()
        account_hash_map = {x["accountNumber"]: x["hashValue"] for x in acc_res}
        if len(self._account) > 0:
            if self._account not in account_hash_map.keys():
                self.logger.error("Account %s not found.", self._account)
                self._account = list(account_hash_map.keys())[0]
        else:
            self._account = list(account_hash_map.keys())[0]
        self.logger.info("Using account %s.", self._account)
        self._account_hash = account_hash_map[self._account]
        self.stream_client = schwab.streaming.StreamClient(
            self.client, account_id=self._account
        )

        self.update()

    def stop(self) -> None:
        pass

    def setTif(self, order: orders.generic.OrderBuilder, tif: TimeInForce):
        if tif == TimeInForce.DAY:
            order.set_duration(orders.common.Duration.DAY)
            order.set_session(orders.common.Session.NORMAL)
        elif tif == TimeInForce.AM:
            order.set_duration(orders.common.Duration.DAY)
            order.set_session(orders.common.Session.AM)
        elif tif == TimeInForce.PM:
            order.set_duration(orders.common.Duration.DAY)
            order.set_session(orders.common.Session.PM)
        elif tif == TimeInForce.EXTENDED:
            order.set_duration(orders.common.Duration.DAY)
            order.set_session(orders.common.Session.SEAMLESS)
        elif tif == TimeInForce.GOOD_TILL_CANCEL:
            order.set_duration(orders.common.Duration.GOOD_TILL_CANCEL)
            order.set_session(orders.common.Session.NORMAL)
        elif tif == TimeInForce.GTC_EXTENDED:
            order.set_duration(orders.common.Duration.GOOD_TILL_CANCEL)
            order.set_session(orders.common.Session.SEAMLESS)
        elif tif == TimeInForce.FILL_OR_KILL:
            order.set_duration(orders.common.Duration.FILL_OR_KILL)
            order.set_session(orders.common.Session.SEAMLESS)
        return order

    def update(self) -> None:
        self.cash = self.getCash()
        self.positions = self.getPositionsDF()

    def getCash(self) -> float:
        res = self.client.get_account(self._account_hash)
        balance: float = res.json()["securitiesAccount"]["currentBalances"][
            "cashBalance"
        ]
        return balance

    def getPositionsDF(self) -> pd.DataFrame:
        res = self.client.get_account(
            self._account_hash,
            fields=schwab.client.Client.Account.Fields.POSITIONS,
        )
        positions_df = pd.DataFrame(res.json()["securitiesAccount"]["positions"])
        position_qty = (
            positions_df["settledLongQuantity"] - positions_df["settledShortQuantity"]
        )
        res_df = pd.DataFrame(
            {
                "symbol": positions_df["instrument"].apply(lambda x: x["symbol"]),
                "currency": ["NA"] * len(positions_df),
                "position": position_qty,
                "market_price": positions_df["marketValue"] / position_qty,
                "avg_cost": positions_df["averagePrice"],
                "market_value": positions_df["marketValue"],
                "unrealized_pnl": positions_df["longOpenProfitLoss"],
                "realized_pnl": [np.nan] * len(positions_df),
                "primary_exchange": ["NA"] * len(positions_df),
            }
        )
        return res_df

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
        symbol, currency, trade_type = instrument.upper().split("-")
        if qty >= 0:
            order = orders.equities.equity_buy_limit(symbol, qty, str(price))
        else:
            order = orders.equities.equity_sell_limit(symbol, abs(qty), str(price))
        order.set_order_type(self.order_type_mapping[order_type])
        self.setTif(order, tif)

        res = self.client.place_order(self._account_hash, order.build())
        order_id: str = schwab.utils.Utils(
            self.client, self._account_hash
        ).extract_order_id(res)
        return order_id

    def cancelOrders(self, order_ids: List[str] = []) -> None:
        if len(order_ids) == 0:
            res = self.client.get_orders_for_account(
                self._account_hash,
                status=self.client.Order.Status.PENDING_ACTIVATION,
            )
            order_ids = [str(x["orderId"]) for x in res.json()]
        for order_id in order_ids:
            res = self.client.cancel_order(order_id, self._account_hash)
            if res.status_code == 200:
                self.logger.info("Order %s has been canceled.", order_id)
            else:
                self.logger.warning("Failed to cancel order %s: %s", order_id, res.text)

    def getOrderStatus(self, order_id: str) -> str:
        res = self.client.get_order(order_id, self._account_hash)
        if res.status_code != 200:
            self.logger.error("Order %s not found: %s", order_id, res.text)
            return ""
        status: str = res.json()["status"]
        return status

    def isPending(self, order_id: str) -> bool:
        order_status = self.getOrderStatus(order_id)
        return order_status in {
            self.client.Order.Status.PENDING_ACTIVATION.value,
            self.client.Order.Status.PENDING_REPLACE.value,
            self.client.Order.Status.PENDING_REPLACE.value,
            self.client.Order.Status.PENDING_CANCEL.value,
            self.client.Order.Status.PENDING_ACKNOWLEDGEMENT.value,
            self.client.Order.Status.AWAITING_RELEASE_TIME.value,
            self.client.Order.Status.AWAITING_STOP_CONDITION.value,
        }

    def isFilled(self, order_id: str):
        order_status = self.getOrderStatus(order_id)
        return order_status == self.client.Order.Status.FILLED

    def isCancelled(self, order_id: str):
        order_status = self.getOrderStatus(order_id)
        return order_status == self.client.Order.Status.CANCELED

    def getFilledPrice(self, order_id: str) -> float:
        res = self.client.get_order(order_id, self._account_hash)
        if res.status_code != 200:
            self.logger.error("Order %s not found: %s", order_id, res.text)
            return 0.0
        if res.json()["status"] != self.client.Order.Status.FILLED.value:
            self.logger.error("Order %s is not filled.", order_id)
            return 0.0
        order = res.json()[0]
        price_list = [
            el["price"]
            for oac in order["orderActivityCollection"]
            for el in oac["executionLegs"]
        ]
        return float(np.mean(price_list))


if __name__ == "__main__":
    import json

    with open("credentials/schwab/api.json") as f:
        credentials = json.load(f)
    sc_broker = CharlesSchwab(
        api_key=credentials["api_key"],
        api_secret=credentials["api_secret"],
    )
    sc_broker.start()
