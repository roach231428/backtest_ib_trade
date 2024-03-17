import sys

import backtrader as bt
import yfinance as yf

sys.path.append("src")

from broker.InteractiveBrokers import CommissionSchemeFixed

from strategy.momentum_williamsR import MomentumWilliamsStrategy


def run_backtest(
    stock_data_list: list,
    strategy,
    ini_cash=10000,
    return_value: bool = True,
    **st_kwargs
):
    cerebro = bt.Cerebro()
    # cerebro.broker = bt.brokers.BackBroker(slip_perc=0.005)
    cerebro.addstrategy(strategy, **st_kwargs)
    for data in stock_data_list:
        cerebro.adddata(bt.feeds.PandasData(dataname=data))
    cerebro.broker.addcommissioninfo(CommissionSchemeFixed())
    cerebro.broker.setcash(ini_cash)
    cerebro.addanalyzer(
        bt.analyzers.SharpeRatio, riskfreerate=0.04, _name="SharpeRatio"
    )
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="AnnualReturn")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="DrawDown")
    # cerebro.addanalyzer(bt.analyzers.TimeReturn, _name = 'TimeReturn')
    res = cerebro.run()[0]
    value_end = cerebro.broker.get_value()

    if return_value:
        # return res.analyzers.SharpeRatio.get_analysis()["sharperatio"])
        return value_end
    else:
        print(value_end)
        [print(it.get_analysis(), "\n") for it in res.analyzers]
        cerebro.plot()
        return res.trade_record


if __name__ == "__main__":
    ticker_quote = ["SOXL"]  # ,"SBUX"
    ticker_hedge = ["SQQQ"]
    quote_data = yf.download(ticker_quote, period="2y", interval="1h")
    hedge_data = yf.download(ticker_hedge, period="2y", interval="1h")
    strategy = MomentumWilliamsStrategy

    run_backtest(
        [quote_data, hedge_data],
        strategy,
        return_value=False,
        **{
            "momentum_period": 10,
            "momentum_ma_period": 30,
            "williams_period": 10,
            "williams_upper": -25,
            "williams_lower": -75,
            "stop_loss": 0.0005,
            "take_profit": 0.001,
        },
    )
