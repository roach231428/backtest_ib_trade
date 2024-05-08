import sys

sys.path.append("src")
import math
from datetime import datetime
from typing import List

import pandas as pd
import talib
from talib import abstract

from src.strategy.base import StrategyBase

cur_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


class MomentWilliamsR(StrategyBase):
    def __init__(
        self,
        quote_ticker: str,
        hedge_ticker: str,
        stop_loss: float = 0.03,
        cmo_period: int = 10,
        cmo_sma_period: int = 10,
        williams_period: int = 14,
        williams_lower: int = -60,
        williams_upper: int = -40,
        sma_period: int = 25,
        bbands_period: int = 50,
        capital: float = 10000,
    ):
        super().__init__()
        self.paras = {
            "cmo_period": cmo_period,
            "cmo_sma_period": cmo_sma_period,
            "williams_period": williams_period,
            "williams_lower": williams_lower,
            "williams_upper": williams_upper,
            "sma_period": sma_period,
            "bbands_period": bbands_period,
        }
        self.quote_ticker = quote_ticker
        self.hedge_ticker = hedge_ticker
        self.stop_loss = stop_loss
        self.capital = capital

    def indicatorProcess(self, data: pd.DataFrame) -> None:
        data.columns = data.columns.str.lower()
        data["cmo"] = abstract.CMO(data, timeperiod=self.paras["cmo_period"])
        data["cmo_sma"] = talib.SMA(
            data["cmo"], timeperiod=self.paras["cmo_sma_period"]
        )
        data["cmo_cross"] = self.cross_over(data["cmo"], data["cmo_sma"])
        data.eval("cmo_diff = cmo - cmo_sma", inplace=True)
        data["williams_r"] = abstract.WILLR(
            data, timeperiod=self.paras["williams_period"]
        )
        data["sma"] = abstract.SMA(data, timeperiod=self.paras["sma_period"])
        data[["bband_upper", "bband_mid", "bband_lower"]] = abstract.BBANDS(
            data, timeperiod=self.paras["bbands_period"]
        )

    def init(self):
        quote_size = self.broker.getHoldings([self.quote_ticker])[self.quote_ticker]
        hedge_size = self.broker.getHoldings([self.hedge_ticker])[self.hedge_ticker]
        if quote_size > 0:
            position_value = (
                quote_size * self.grabbers[self.quote_ticker].getLatestCloseData()
            )
        elif hedge_size > 0:
            position_value = (
                hedge_size * self.grabbers[self.hedge_ticker].getLatestCloseData()
            )
        else:
            position_value = 0
        self.cash_left = max(self.capital - position_value, 0)

    def move_position(self, src_ticker, dest_ticker, src_size, dest_size) -> List[dict]:
        return [
            {
                "instrument": f"{src_ticker}-USD-SPOT",
                "qty": -src_size,
                "orderType": "MKT",
            },
            {
                "instrument": f"{dest_ticker}-USD-SPOT",
                "qty": dest_size,
                "orderType": "MKT",
            },
        ]

    def next(self) -> List[dict]:
        self.init()
        for data in self.datas:
            self.indicatorProcess(data)
        quote_row = self.datas[self.quote_ticker].iloc[-1]
        hedge_row = self.datas[self.hedge_ticker].iloc[-1]

        # Calculate signal
        broker = self.broker
        holding = broker.getHoldings([self.quote_ticker, self.hedge_ticker])
        quote_size = holding[self.quote_ticker]
        hedge_size = holding[self.hedge_ticker]
        if quote_size > 0:
            cost = broker.getCosts([self.quote_ticker])[self.quote_ticker]
        elif hedge_size > 0:
            cost = broker.getCosts([self.hedge_ticker])[self.hedge_ticker]
        else:
            cost = -1

        if quote_size > 0:
            src_ticker = self.quote_ticker
            dest_ticker = self.hedge_ticker
            size = quote_size
            src_close_price = quote_row["close"]
            dest_close_price = hedge_row["close"]
        elif hedge_size > 0:
            src_ticker = self.hedge_ticker
            dest_ticker = self.quote_ticker
            size = hedge_size
            src_close_price = hedge_row["close"]
            dest_close_price = quote_row["close"]
        else:
            size = 0
            src_close_price = 1
            dest_close_price = 1

        expected_cash = min(
            max(self.cash_left + size * src_close_price, 1), self.capital
        )
        new_size = math.floor(expected_cash / dest_close_price * 0.95)

        if cost > 0 and src_close_price / cost < (1 - self.stop_loss):
            self.logger.info("Stop loss triggered.")
            return self.move_position(
                src_ticker=src_ticker,
                dest_ticker=dest_ticker,
                src_size=size,
                dest_size=new_size,
            )

        if (
            quote_row["cmo_cross"] == 1
            and quote_row["williams_r"] <= self.paras["williams_lower"]
        ):
            self.logger.info("Buy signal.")
            if hedge_size > 0:
                return self.move_position(
                    src_ticker=src_ticker,
                    dest_ticker=dest_ticker,
                    src_size=size,
                    dest_size=new_size,
                )
            elif quote_size == 0:
                new_size = math.floor(expected_cash / quote_row["close"] * 0.95)
                return [
                    {
                        "instrument": f"{self.quote_ticker}-USD-SPOT",
                        "qty": new_size,
                        "orderType": "MKT",
                    }
                ]
        elif (
            quote_row["cmo_cross"] == -1
            and quote_row["williams_r"] >= self.paras["williams_upper"]
        ):
            self.logger.info("Sell signal.")
            if quote_size > 0:
                return self.move_position(
                    src_ticker=src_ticker,
                    dest_ticker=dest_ticker,
                    src_size=size,
                    dest_size=new_size,
                )
            elif hedge_size == 0:
                new_size = math.floor(expected_cash / hedge_row["close"] * 0.95)
                return [
                    {
                        "instrument": f"{self.hedge_ticker}-USD-SPOT",
                        "qty": new_size,
                        "orderType": "MKT",
                    }
                ]
        return []


if __name__ == "__main__":
    pass
