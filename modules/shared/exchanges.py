import os
import sys
import hmac
import okx
import time
import urllib.parse
import hashlib
import okx.Account
import okx.Funding
import requests
import pandas as pd
import numpy as np
from coinbase import jwt_generator
from prefect.variables import Variable
import concurrent.futures

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from modules.shared.utils import match_currencies, match_currency_pairs

def get_binance_trade_book():
    url = f"https://api.binance.com/api/v3/ticker/bookTicker"
    response = requests.get(url)
    json: dict = response.json()
    df_binance = pd.DataFrame([ { "provider" : 2, "pair" : x.get("symbol"), "ask_price" : float(x.get("askPrice")), "bid_price" : float(x.get("bidPrice")) }for x in json ])
    df_binance = match_currency_pairs(df_binance, only_matches=True, include_match_columns=False)
    df_binance = df_binance[["provider", "from_currency", "to_currency", "ask_price", "bid_price"]]
    return df_binance

def get_kraken_wallet_status():
    url = "https://api.kraken.com/0/public/Assets"
    response = requests.get(url)
    json: dict = response.json()
    result = json.get("result")
    df = pd.DataFrame(result.values())
    df["can_deposit"] = (df["status"] == "enabled") | (df["status"] == "deposit_only")
    df["can_withdraw"] = (df["status"] == "enabled") | (df["status"] == "withdraw_only")
    df = match_currencies(df, only_matches=True, include_match_columns=False)
    return df[["currency", "can_deposit", "can_withdraw"]]

def get_kraken_trade_book():
    url = f"https://api.kraken.com/0/public/Ticker"
    response = requests.get(url)
    json: dict = response.json()
    result: dict = json.get("result")
    df_kraken = pd.DataFrame([ { "provider" : 1, "pair" : k, "ask_price" : float(v.get("a")[0]), "bid_price" : float(v.get("b")[0]) } for k,v in result.items() ])
    df_kraken = match_currency_pairs(df_kraken, only_matches=True, include_match_columns=False)
    df_kraken = df_kraken[["provider", "from_currency", "to_currency", "ask_price", "bid_price"]]
    wallet_status = get_kraken_wallet_status()
    df_kraken = df_kraken.merge(wallet_status.rename(columns={ "currency": "from_currency", "can_withdraw" : "can_withdraw_from_currency", "can_deposit" : "can_deposit_from_currency" }), on="from_currency", how="left")
    df_kraken = df_kraken.merge(wallet_status.rename(columns={ "currency": "to_currency", "can_withdraw" : "can_withdraw_to_currency", "can_deposit" : "can_deposit_to_currency" }), on="to_currency", how="left")
    return df_kraken

def get_coinbase_jwt(request_path: str, request_method: str = "GET"):
    coinbase_api: dict = Variable.get("coinbase_api")
    key_name: str = coinbase_api.get("key_name")
    key_secret: str = coinbase_api.get
    jwt_uri = jwt_generator.format_jwt_uri(request_method, request_path)
    jwt_token = jwt_generator.build_rest_jwt(jwt_uri, key_name, key_secret)
    return jwt_token

def get_coinbase_trade_book():
    url = f"https://api.exchange.coinbase.com/products"
    response = requests.get(url)
    json: dict = response.json()
    products: pd.DataFrame = pd.DataFrame(json)
    products = products.loc[products["status"] == "online"]
    products["pair"] = products["base_currency"] + products["quote_currency"]
    products = match_currency_pairs(products, only_matches=True, include_match_columns=False)
    products = products[["id", "pair", "base_currency", "quote_currency", "from_currency", "to_currency"]]
    request_path = "/api/v3/brokerage/best_bid_ask"
    jwt_token = get_coinbase_jwt(request_path)
    headers = dict()
    headers["Authorization"] = f"Bearer {jwt_token}"
    url = f"https://api.coinbase.com{request_path}"
    response = requests.get(url, headers=headers)
    json: dict = response.json()
    product_books = pd.DataFrame([ { "id" : row.get("product_id"), "ask_price" : float(row.get("asks")[0].get("price")), "bid_price" : float(row.get("bids")[0].get("price")) } for row in json.get("pricebooks")])
    product_books = product_books.merge(products, on="id", how="inner")
    product_books["provider"] = 3
    return product_books[["provider", "from_currency", "to_currency", "ask_price", "bid_price"]]

def get_okx_wallet_status():
    return okx.Funding.FundingAPI().get_currencies()

def  get_okx_trade_book():
    flag = "0"  # Production trading:0 , demo trading:1
    marketDataAPI =  okx.MarketData.MarketAPI(flag=flag)
    result = marketDataAPI.get_tickers(instType="SPOT")
    data = pd.DataFrame(result.get("data"))
    data["pair"] = data["instId"].apply(lambda x: x.replace("-",""))
    data = match_currency_pairs(data, only_matches=True, include_match_columns=False)
    data["provider"] = 4
    data.rename(columns={ "askPx" : "ask_price", "bidPx" : "bid_price" }, inplace=True)
    data["ask_price"] = data["ask_price"].astype(float)
    data["bid_price"] = data["bid_price"].astype(float)
    return data[["provider", "from_currency", "to_currency", "ask_price", "bid_price"]]

def get_htx_wallet_status():
    url = "https://api.huobi.pro/v1/settings/common/currencys"
    response = requests.get(url)
    data: list[dict] = response.json().get("data")
    df = pd.DataFrame(data)
    df["name"] = df["name"].apply(lambda x: str.upper(x))
    df["altname"] = df["name"]
    df["bname"] = df["name"]
    df = df.rename(columns={ "de" : "can_deposit", "we" : "can_withdraw" })
    df = df[["name", "altname", "bname", "can_deposit", "can_withdraw"]]
    df = match_currencies(df, only_matches=True, include_match_columns=False)
    return df[["currency", "can_deposit", "can_withdraw"]]

def get_htx_trade_book():
    url = "https://api.huobi.pro/market/tickers"
    response = requests.get(url)
    data: list[dict] = response.json().get("data")
    df = pd.DataFrame(data)
    df.rename(columns={ "symbol" : "pair", "ask" : "ask_price", "bid" : "bid_price" }, inplace=True)
    df["pair"] = df["pair"].apply(lambda x: str.upper(x))
    df["provider"] = 5
    df = match_currency_pairs(df, only_matches=True, include_match_columns=False)
    df = df[["provider", "from_currency", "to_currency", "ask_price", "bid_price"]]
    wallet_status = get_htx_wallet_status()
    df = df.merge(wallet_status.rename(columns={ "currency": "from_currency", "can_withdraw" : "can_withdraw_from_currency", "can_deposit" : "can_deposit_from_currency" }), on="from_currency", how="left")
    df = df.merge(wallet_status.rename(columns={ "currency": "to_currency", "can_withdraw" : "can_withdraw_to_currency", "can_deposit" : "can_deposit_to_currency" }), on="to_currency", how="left")
    return df

def get_mexc_wallet_status():
    mexc_api: dict = Variable.get("mexc_api")
    secret_key: str = mexc_api.get("secret_key")
    access_key: str = mexc_api.get("access_key")
    query_params = {
        "timestamp" : int(time.time() * 1000)
    }
    query_params_uri = urllib.parse.urlencode(query_params)
    message = query_params_uri
    signature = hmac.new(
        secret_key.encode(),
        msg=message.encode(),
        digestmod=hashlib.sha256
    ).hexdigest().upper()
    query_params["signature"] = signature
    headers = {
        "X-MEXC-APIKEY" :	access_key,
        "Content-Type"	: "application/json"
    }
    url = "https://api.mexc.com/api/v3/capital/config/getall"
    response = requests.get(url, headers=headers, params=query_params)
    data: list[dict] = response.json()
    df = pd.DataFrame([ { "currency" : row.get("coin"), "currency_name" : row.get("name"), **network_row } for row in data for network_row in row.get("networkList") ])
    df = df[["currency", "depositEnable", "withdrawEnable"]].groupby(by="currency").any().reset_index().rename(columns={ "currency" : "name", "depositEnable" : "can_deposit", "withdrawEnable" : "can_withdraw" })
    df["altname"] = df["name"]
    df["bname"] = df["name"]
    df = match_currencies(df, only_matches=True, include_match_columns=False)
    return df[["currency", "can_deposit", "can_withdraw"]]

def get_mexc_trade_book():
    url = "https://api.mexc.com/api/v3/ticker/bookTicker"
    response = requests.get(url)
    data: list[dict] = response.json()
    df = pd.DataFrame(data)
    df.rename(columns={ "symbol" : "pair", "askPrice" : "ask_price", "bidPrice" : "bid_price" }, inplace=True)
    df["ask_price"] = df["ask_price"].astype(float)
    df["bid_price"] = df["bid_price"].astype(float)
    df["pair"] = df["pair"].apply(lambda x: str.upper(x))
    df["provider"] = 6
    df = match_currency_pairs(df, only_matches=True, include_match_columns=False)
    df = df[["provider", "from_currency", "to_currency", "ask_price", "bid_price"]]
    wallet_status = get_mexc_wallet_status()
    df = df.merge(wallet_status.rename(columns={ "currency": "from_currency", "can_withdraw" : "can_withdraw_from_currency", "can_deposit" : "can_deposit_from_currency" }), on="from_currency", how="left")
    df = df.merge(wallet_status.rename(columns={ "currency": "to_currency", "can_withdraw" : "can_withdraw_to_currency", "can_deposit" : "can_deposit_to_currency" }), on="to_currency", how="left")
    return df

def get_max_profit(row, provider_combinations):
    lst = [ row["bid_price"][c[0]] / row["ask_price"][c[1]] for c in provider_combinations if not pd.isnull(row["bid_price"][c[0]]) and not pd.isnull(row["ask_price"][c[1]]) ]
    return None if len(lst) == 0 else max(lst)

def get_trade_book():
    all_data = []
    functions = [get_kraken_trade_book, get_coinbase_trade_book, get_htx_trade_book, get_mexc_trade_book]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(functions)) as executor:
        future_to_url = {executor.submit(function): function for function in functions}
        for future in concurrent.futures.as_completed(future_to_url):
            function = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (str(function.__name__), exc))
            else:
                all_data.append(data)     
    df_all = pd.concat(all_data)
    df_all.replace({ 0.0 : None }, inplace=True)
    df_all.replace({ np.nan : None }, inplace=True)  
    return df_all