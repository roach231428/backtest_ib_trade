import pandas as pd


class TachnicalAnalysis:
    def __init__(self, yf_data: pd.DataFrame) -> None:
        self.data = yf_data
        pass

    def MACD(
        self, fastperiod: int = 12, slowperiod: int = 26, signalperiod: int = 9
    ) -> tuple:
        ema_fast = self.stock_data["Close"].ewm(span=fastperiod, adjust=False).mean()
        ema_slow = self.stock_data["Close"].ewm(span=slowperiod, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal = macd.ewm(span=signalperiod, adjust=False).mean()
        histogram = macd - signal
        return macd, signal, histogram
