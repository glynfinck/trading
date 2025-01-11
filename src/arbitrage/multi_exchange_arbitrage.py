import numpy as np
from prefect import task, flow

def get_min_price_provider(row: np.ndarray):
    arr = row["ask_price"].loc[row["can_deposit_from_currency"] & row["can_withdraw_from_currency"] & row["can_deposit_to_currency"] & row["can_withdraw_to_currency"]]
    return arr.idxmin() if len(arr) > 0 else None

def get_max_price_provider(row: np.ndarray):
    arr = row["ask_price"].loc[row["can_deposit_from_currency"] & row["can_withdraw_from_currency"] & row["can_deposit_to_currency"] & row["can_withdraw_to_currency"]]
    return arr.idxmax() if len(arr) > 0 else None

@flow
def multi_exchange_arbitrage():
    pass