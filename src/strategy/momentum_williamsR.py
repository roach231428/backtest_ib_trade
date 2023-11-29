import math

import backtrader as bt


class MomentumWilliamsStrategy(bt.Strategy):
    # 交易紀錄
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

    params = dict(
        momentum_period=10,
        momentum_ma_period=10,
        williams_period=14,
        williams_upper=-40,
        williams_lower=-60,
        sma_period=5 * 5,
        stop_loss=0.1,
        take_profit=0.3,
        bolling_period=10 * 5,
    )

    def __init__(self):

        # Initialize parameters
        self.quote_data = self.datas[0]
        self.hedge_data = self.datas[1] if len(self.datas) > 1 else None
        self.ini_cash = self.broker.get_cash()

        # Indicators
        self.momentum = bt.ind.MomentumOscillator(
            self.quote_data, period=int(self.p.momentum_period)
        )
        self.momentum_ma = bt.ind.MovingAverageSimple(
            self.momentum, period=int(self.p.momentum_ma_period)
        )
        self.momentum_crossover = bt.ind.CrossOver(self.momentum, self.momentum_ma)
        self.momentum_diff = self.momentum - self.momentum_ma
        self.williams_r = bt.ind.WilliamsR(
            self.quote_data, period=int(self.p.williams_period)
        )
        self.sma = bt.ind.SMA(period=int(self.p.sma_period))
        self.bolling_top = bt.ind.BollingerBands(
            self.quote_data, period=int(self.p.bolling_period)
        ).top
        self.bolling_bottom = bt.ind.BollingerBands(
            self.quote_data, period=int(self.p.bolling_period)
        ).bot
        if self.hedge_data is not None:
            self.sma_hedge = bt.ind.SMA(self.hedge_data, period=int(self.p.sma_period))

        # For trading record output
        self.trade_record = {
            "Time": [],
            "Type": [],
            "Price": [],
            "Amount": [],
            "Commission": [],
            "Gross Pnl": [],
            "Net Pnl": [0],
            "Value": [self.broker.get_cash()],
        }

    def next(self):

        if self.hedge_data is None:
            close_price = self.quote_data.close[0]
            expected_cash = self.broker.get_cash() + self.position.size * close_price
            new_size = math.floor(expected_cash / close_price * 0.95)

            if self.position.size > 0:
                if self.quote_data.close[0] / self.position.price < (
                    1 - self.p.stop_loss
                ):
                    print(f"{self.quote_data.datetime.datetime(0)}\tStop loss.")
                    # self.sell(size=abs(self.position.size) + new_size)
                    return self.close()
                elif self.quote_data.close[0] / self.position.price > (
                    1 + self.p.take_profit
                ):
                    print(f"{self.quote_data.datetime.datetime(0)}\tTake profit.")
                    return self.close()
            elif self.position.size < 0:
                if self.quote_data.close[0] / self.position.price > (
                    1 + self.p.stop_loss
                ):
                    print(f"{self.quote_data.datetime.datetime(0)}\tStop loss.")
                    # self.buy(size=abs(self.position.size) + new_size)
                    return self.close()
                elif self.quote_data.close[0] / self.position.price < (
                    1 - self.p.take_profit
                ):
                    print(f"{self.quote_data.datetime.datetime(0)}\tTake profit.")
                    return self.close()

            if (
                self.momentum_crossover == 1
                and self.williams_r[0] <= self.p.williams_lower
            ):
                if self.position.size < 0:
                    return self.buy(size=abs(self.position.size) + new_size)
                elif self.position.size == 0:
                    return self.buy()
            elif (
                self.momentum_crossover == -1
                and self.williams_r[0] >= self.p.williams_upper
            ):
                if self.position.size > 0:
                    return self.sell(size=abs(self.position.size) + new_size)
                elif self.position.size == 0:
                    return self.sell()
        else:
            quote_price = self.positions[self.quote_data].price
            hedge_price = self.positions[self.hedge_data].price
            quote_size = self.positions[self.quote_data].size
            hedge_size = self.positions[self.hedge_data].size

            if quote_size > 0:
                if self.quote_data.close[0] / quote_price < (1 - self.p.stop_loss):
                    expected_cash = min(
                        self.ini_cash,
                        quote_size * self.quote_data.close[0] + self.broker.get_cash(),
                    )
                    new_size = math.floor(
                        expected_cash / self.hedge_data.close[0] * 0.95
                    )
                    print(f"{self.quote_data.datetime.datetime(0)}\tStop loss.")
                    return self.close(data=self.quote_data), self.buy(data=self.hedge_data, size=new_size)
                elif self.quote_data.close[0] / quote_price > (1 + self.p.take_profit):
                    print(f"{self.quote_data.datetime.datetime(0)}\tTake profit.")
                    return self.close(data=self.quote_data)

            elif hedge_size > 0:
                if self.hedge_data.close[0] / hedge_price < (1 - self.p.stop_loss):
                    expected_cash = min(
                        self.ini_cash,
                        hedge_size * self.hedge_data.close[0] + self.broker.get_cash(),
                    )
                    new_size = math.floor(
                        expected_cash / self.quote_data.close[0] * 0.95
                    )
                    print(f"{self.hedge_data.datetime.datetime(0)}\tStop loss.")
                    return self.close(data=self.hedge_data), self.buy(data=self.quote_data, size=new_size)

                elif self.hedge_data.close[0] / hedge_price > (1 + self.p.take_profit):
                    print(f"{self.hedge_data.datetime.datetime(0)}\tTake profit.")
                    return self.close(data=self.hedge_data)

            if (
                self.momentum_crossover == 1
                and self.williams_r[0] <= self.p.williams_lower
            ):
                expected_cash = min(
                    self.ini_cash,
                    hedge_size * self.hedge_data.close[0] + self.broker.get_cash(),
                )
                new_size = math.floor(expected_cash / self.quote_data.close[0] * 0.95)
                if hedge_size > 0:
                    return self.close(data=self.hedge_data), self.buy(data=self.quote_data, size=new_size)
                elif quote_size == 0 and hedge_size == 0:
                    return self.buy(data=self.quote_data, size=new_size)
            elif (
                self.momentum_crossover == -1
                and self.williams_r[0] >= self.p.williams_upper
            ):
                expected_cash = min(
                    self.ini_cash,
                    quote_size * self.quote_data.close[0] + self.broker.get_cash(),
                )
                new_size = math.floor(expected_cash / self.hedge_data.close[0] * 0.95)
                if quote_size > 0:
                    return self.close(data=self.quote_data), self.buy(data=self.hedge_data, size=new_size)
                elif quote_size == 0 and hedge_size == 0:
                    return self.buy(data=self.hedge_data, size=new_size)

    def stop(self):
        # for k,it in self.trade_record.items():
        #     print(k, len(it))
        # trade_record_df = pd.DataFrame(self.trade_record)
        # trade_record_df.to_csv("result/momentum_williamsR.csv", index=False)
        print("End.")
        return self.close()
