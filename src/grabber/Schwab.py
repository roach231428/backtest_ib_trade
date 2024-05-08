import schwab
from selenium import webdriver
import pandas as pd
from datetime import datetime, timedelta
import time
from .base import DataGrabberBase
from typing import Dict, List

class SchwabGrabber(DataGrabberBase):

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            tickers: List[str],
            interval: str,
            period: str = "max",
            name: str = "",
            token_path: str = None,
            callback_url: str = "https://127.0.0.1",
            client: schwab.client.Client | schwab.client.AsyncClient = None,
        ) -> None:
        """
        Initializes a new instance of the SchwabGrabber class.

        Args:
            api_key (str): The API key for authentication.
            api_secret (str): The API secret for authentication.
            tickers (List[str]): The list of tickers to grab data for.
            interval (str): The interval at which to grab data.
            period (str, optional): The period of data to grab. Defaults to "max".
            name (str, optional): The name of the grabber. Defaults to "".
            token_path (str, optional): The path to the token file. Defaults to None.
            callback_url (str, optional): The callback URL for authentication. Defaults to "https://127.0.0.1".
            client (schwab.client.Client | schwab.client.AsyncClient, optional): The client object. Defaults to None.

        Returns:
            None

        Initializes a new instance of the SchwabGrabber class.
        If client is None, it creates a new client object using the provided API key, API secret, token path, and callback URL.
        If token path is None, it creates a new client object using the provided API key, API secret, callback URL, and token path.
        The account hash value is retrieved from the client object.
        """

        super().__init__(tickers, interval=interval, period=period, name=name)

        if client is None:
            if token_path is not None:
                self.client = schwab.auth.client_from_token_file(
                    token_path=token_path,
                    api_key=api_key,
                    app_secret=api_secret,
                )
            else:
                # self.client = schwab.auth.client_from_login_flow(
                #     webdriver=webdriver.Edge(),
                #     api_key=api_key,
                #     app_secret=api_secret,
                #     callback_url=callback_url,
                #     token_path="./token1",
                # )
                self.client = schwab.auth.client_from_manual_flow(
                    api_key=api_key,
                    app_secret=api_secret,
                    callback_url=callback_url,
                    token_path="./token1",
                )
        else:
            self.client = client

        self.account_hash = self.client.get_account_numbers().json()[0]['hashValue']


# res = client.get_account(account_hash=account_hash)
# res.json()

# while True:
#     res = client.get_price_history_every_minute(
#         'AAPL',
#         start_datetime=datetime.now()-timedelta(minutes=10),
#         end_datetime=datetime.now(),
#         need_extended_hours_data=True,
#     )
#     df = pd.DataFrame.from_dict(res.json()['candles'])
#     df["datetime"] = [datetime.fromtimestamp(x/1000) for x in df["datetime"]]
#     print(datetime.now(), "\t", df["datetime"].iloc[-1])
#     time.sleep(0.5)
