import sys

sys.path.append("src")
import logging
from typing import List

from model.trader import TraderBase


class IntradayTrader(TraderBase):
    def __init__(
        self, tickers: List[str], sleep_interval: float = 1, buffer_time: float = 5
    ):
        super().__init__(tickers, sleep_interval, buffer_time)

    def run(self) -> None:
        while True:
            if self.break_flag:
                break

            now = self.broker.now()
            logging.debug(f"Current time: {now}")
            if now.hour == 20 and now.minute == 59:
                self.stop()
                break
            elif 0 < now.second <= self.buffer_time:
                # Check if data is too old
                skip_flag = False
                stale_flag = False
                while True:
                    if skip_flag:
                        break
                    res = self.updateData(now)
                    # Update new data
                    if all([x == 1 for x in res.values()]):
                        break
                    # Data is updated before
                    elif all([x == 0 for x in res.values()]):
                        skip_flag = True
                        break
                    for result in res.values():
                        if result == -1:
                            skip_flag = True
                            stale_flag = True
                        else:
                            skip_flag = True
                            self.broker.sleep(0.3)

                if stale_flag:
                    self.broker.sleep(50)  # Need to be adjusted
                if skip_flag:
                    self.broker.sleep(self.sleep_interval)
                    continue

                self.broker.update()
                try:
                    instructions = self.strategy.next()
                    for instr in instructions:
                        logging.info(f"New order instruction: {instr}")
                        order = self.broker.placeStockOrder(**instr)
                        logging.info(order)
                except Exception as e:
                    logging.error(e)
            else:
                logging.debug(f"Not now.")
            self.broker.sleep(self.sleep_interval)

    def stop(self) -> None:
        self.broker.closePosition(self.tickers)
        self.broker.stop()
