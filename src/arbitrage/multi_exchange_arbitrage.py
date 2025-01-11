import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import itertools
from modules.shared.utils import send_email
from prefect import task, flow, get_run_logger

from modules.shared.exchanges import get_trade_book

def get_min_price_provider(row: np.ndarray):
    arr = row["ask_price"].loc[row["can_deposit_from_currency"] & row["can_withdraw_from_currency"] & row["can_deposit_to_currency"] & row["can_withdraw_to_currency"]]
    return arr.idxmin() if len(arr) > 0 else None

def get_max_price_provider(row: np.ndarray):
    arr = row["ask_price"].loc[row["can_deposit_from_currency"] & row["can_withdraw_from_currency"] & row["can_deposit_to_currency"] & row["can_withdraw_to_currency"]]
    return arr.idxmax() if len(arr) > 0 else None

@flow
def multi_exchange_arbitrage(ignore_from_currencies: list[int] = [], ignore_to_currencies: list[int] = [], threshold = 2, fee: float = 0.4, duration_minutes: int = 30):
    logger = get_run_logger()

    # 1. Main loop.
    start_time = time.time()
    while True:

        # a) Get the trade book.
        df_all = get_trade_book()

        # b) Filter trade book by input from and to currencies.
        df_all = df_all.loc[~((df_all["provider"] == 6) & (df_all["from_currency"].isin(ignore_from_currencies)) & (df_all["to_currency"].isin(ignore_to_currencies)))]
        
        # c) Get the ask and bid prices.
        df_pivot = df_all.pivot(index=["from_currency", "to_currency"],columns=["provider"],values=["ask_price", "bid_price", "can_deposit_from_currency", "can_withdraw_from_currency", "can_deposit_to_currency", "can_withdraw_to_currency" ]).copy(True)
        df_pivot['can_deposit_from_currency'] = df_pivot['can_deposit_from_currency'].replace({ np.nan : False })
        df_pivot['can_withdraw_from_currency'] = df_pivot['can_withdraw_from_currency'].replace({ np.nan : False })
        df_pivot['can_deposit_to_currency'] = df_pivot['can_deposit_to_currency'].replace({ np.nan : False })
        df_pivot['can_withdraw_to_currency'] = df_pivot['can_withdraw_to_currency'].replace({ np.nan : False })
        df_pivot["min_ask_price"] = df_pivot.apply(lambda row: row["ask_price"].loc[row["can_deposit_from_currency"] & row["can_withdraw_from_currency"] & row["can_deposit_to_currency"] & row["can_withdraw_to_currency"]].min(), axis=1).astype("Float64")
        df_pivot["min_ask_price_provider"] = df_pivot.apply(get_min_price_provider, axis=1).astype("Int64")
        df_pivot["max_bid_price"] = df_pivot.apply(lambda row: row["bid_price"].loc[row["can_deposit_from_currency"] & row["can_withdraw_from_currency"] & row["can_deposit_to_currency"] & row["can_withdraw_to_currency"]].max(), axis=1).astype("Float64")
        df_pivot["max_bid_price_provider"] = df_pivot.apply(get_max_price_provider, axis=1).astype("Int64")
        df_pivot["max_profit"] = df_pivot["max_bid_price"] / df_pivot["min_ask_price"]
        df_pivot = df_pivot.loc[df_pivot["max_profit"].notnull()]
        df_pivot = df_pivot.sort_values(by="max_profit", ascending=False)
        df_pivot = df_pivot.reset_index().droplevel(1, axis=1)[["from_currency", "to_currency", "min_ask_price_provider", "max_bid_price_provider", "min_ask_price",  "max_bid_price",  "max_profit"]]
        
        # d) Log the max profit.
        max_profit_df: pd.DataFrame = df_pivot.loc[df_pivot["max_profit"] == df_pivot["max_profit"].max()]
        max_profit: dict = max_profit_df.to_dict("records")[0]
        logger.info(f"Maximum profit: {max_profit.get("max_profit")}")
        if max_profit.get("max_profit") > 1 + (((fee * 2) + threshold) / 100):
            logger.info("Multi-exchange arbitrage detected.")
            logger.info(f"from_currency: {max_profit.get('from_currency')}")
            logger.info(f"to_currency: {max_profit.get('to_currency')}")
            logger.info(f"min_ask_price_provider: {max_profit.get('min_ask_price_provider')}")
            logger.info(f"max_bid_price_provider: {max_profit.get('max_bid_price_provider')}")
            logger.info(f"min_ask_price: {max_profit.get('min_ask_price')}")
            logger.info(f"max_bid_price: {max_profit.get('max_bid_price')}")
            logger.info(f"max_profit: {max_profit.get('max_profit')}")
            body = f"""
            {max_profit_df.to_html(index=False)}
            Profit: {max_profit.get("max_profit")}
            """
            send_email("service@manningcapital.co.uk", ["glynfinck97@gmail.com"], "Multiple Exchange Arbitrage Detected",body)

        # e) Check if time has exceeded.
        elapsed_minutes = (time.time() - start_time) / 60
        if elapsed_minutes > duration_minutes:
            logger.info(f"Elapsed minutes: {round(elapsed_minutes, 4)}. Exiting.")
            break

if __name__ == "__main__":
    multi_exchange_arbitrage(duration_minutes=1, threshold=1, ignore_from_currencies=[382])