from enum import Enum


class Position:
    symbol: str
    currency: str
    position: float
    avg_cost: float
    unrealized_pnl: float

    def __init__(self, symbol, currency="", position=0, avg_cost=0, unrealized_pnl=0):
        self.symbol = symbol
        self.currency = currency
        self.position = position
        self.avg_cost = avg_cost
        self.unrealized_pnl = unrealized_pnl


class Order:
    pass


class Trade:
    pass


class Test(Enum):
    AA = 1
    BB = 2

    def __str__(self):
        return self.value
