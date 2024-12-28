import time
import urllib.parse
import hashlib
import hmac
import base64
import requests
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from database import utils
import itertools
from prefect.variables import Variable
from shared.utils import send_email

class CurrencyData:
    
    symbol: str
    ask_price: float
    ask_quantity: float
    bid_price: float
    bid_quantity: float

    def __init__(self, symbol: str, ask_price: float, ask_quantity: float, bid_price: float, bid_quantity: float):
        self.symbol = symbol
        self.ask_price = ask_price
        self.ask_quantity = ask_quantity
        self.bid_price = bid_price
        self.bid_quantity = bid_quantity

class TriangularArbitrage:

    def __init__(self):
        pass

    def __get_currency_data(tickers: list[str]) -> list[CurrencyData]:
        pass

    def run(self, tickers: list[str]):
        
        # 1. 
        currency = self.__get_currency_data(tickers)

class BinanaceTriangularArbitrage(TriangularArbitrage):

    def __get_currency_data(tickers: list[str]) -> list[CurrencyData]:
        response = requests.get("https://api.binance.com/api/v3/ticker/bookTicker")
        json: list[dict] = response.json()
        return [ CurrencyData(item.get("symbol"), item.get("bidPrice"), item.get("bidQty"), item.get("askPrice"), item.get("askQty")) for item in json ]

class KrakenTriangularArbitrage(TriangularArbitrage):

    def __get_currency_data(tickers: list[str]) -> list[CurrencyData]:
        pass
    

@task
def get_assets():
    data = requests.get("https://api.kraken.com/0/public/Assets").json()
    return pd.DataFrame([ { "name": k, "altname": v.get("altname") }  for k,v in data.get("result").items() ])

@task
def get_asset_pairs():
    data = requests.get("https://api.kraken.com/0/public/AssetPairs").json()
    return pd.DataFrame([ { "name": k, "altname": v.get("altname") }  for k,v in data.get("result").items() ])

def get_kraken_api_headers(uri_path: str, data: dict) -> dict:

    # 1. Setup variables.
    kraken_api_credentials: dict = Variable.get("kraken_api_credentials")

    # 2. Encode the data for the request
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()

    # 3. Create a message to be signed
    message = uri_path.encode() + hashlib.sha256(encoded).digest()

    # 4. Create the HMAC signature
    mac = hmac.new(base64.b64decode(kraken_api_credentials.get("private_key")), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())

    # 5. Create headers for the request
    headers = {}
    headers['API-Key'] = kraken_api_credentials.get("api_key")
    headers['API-Sign'] = sigdigest.decode()

    return headers

@task
def cancel_all_orders():
    nonce = str(int(1000 * time.time()))
    api_url = 'https://api.kraken.com'
    uri_path = '/0/private/CancelAll'
    data = {
        'nonce': nonce
    }
    headers = get_kraken_api_headers(uri_path, data)
    response = requests.request("POST", api_url + uri_path, headers=headers, data=data)
    return response.json()

@task
def get_all_currency_tickers_data() -> pd.DataFrame:
    url = f"https://api.kraken.com/0/public/Ticker"
    response = requests.get(url)
    result: dict = response.json().get('result')
    asset_pairs = get_asset_pairs()
    asset_pair_map = { x["name"]: x["altname"] for x in asset_pairs.to_dict("records") }
    return pd.DataFrame([ { 
            "name" : k,
            "altname" : asset_pair_map.get(k),
            "ask" : float(v.get("a")[0]), 
            "ask_wlv" : float(v.get("a")[1]),
            "ask_lv" : float(v.get("a")[2]),
            "bid" : float(v.get("b")[0]), 
            "bid_wlv" : float(v.get("b")[1]),
            "bid_lv" : float(v.get("b")[2]),
            "close": float(v.get("c")[0]),
            "close_lv": float(v.get("c")[1]),
        }
        for k,v in result.items()
    ], columns=["name","altname","ask","ask_wlv","ask_lv","bid","bid_wlv","bid_lv","close","close_lv"])

@task
def get_current_balance() -> pd.DataFrame:

    nonce = str(int(1000 * time.time()))
    api_url = 'https://api.kraken.com'
    uri_path = '/0/private/Balance'
    data = {
        'nonce': nonce
    }
    headers = get_kraken_api_headers(uri_path, data)
    response = requests.request("POST", api_url + uri_path, headers=headers, data=data)
    result: dict = response.json().get("result")

    assets = get_assets()
    position_data = pd.DataFrame([ { "name": k, "position": float(v) } for k,v in result.items() ])
    position_data = position_data.merge(assets, how="left", on=["name"])[["name","altname","position"]]
    
    exchange_rate_data_1 = pd.DataFrame({ "from_iso" : position_data["altname"] })
    exchange_rate_data_1["to_iso"] = "USD"
    exchange_rate_data_2 = pd.DataFrame({ "from_iso" : exchange_rate_data_1["to_iso"], "to_iso" : exchange_rate_data_1["from_iso"] })
    exchange_rate_data = pd.concat([exchange_rate_data_1, exchange_rate_data_2])
    exchange_rate_data["pair"] = exchange_rate_data["from_iso"] + exchange_rate_data["to_iso"]

    close_data = get_all_currency_tickers_data()[["pair","close"]]
    exchange_rate_data = exchange_rate_data.merge(close_data, how="inner", on=["pair"])
    exchange_rate_data_reverse = exchange_rate_data.copy(True)
    temp = exchange_rate_data_reverse["from_iso"]
    exchange_rate_data_reverse["from_iso"] = exchange_rate_data_reverse["to_iso"]
    exchange_rate_data_reverse["to_iso"] = temp
    exchange_rate_data_reverse["close"] = 1 / exchange_rate_data_reverse["close"]
    exchange_rate_data_same = pd.DataFrame({ "from_iso" : position_data["altname"], "to_iso" : position_data["altname"], "pair": position_data["altname"] + position_data["altname"] })
    exchange_rate_data_same["close"] = 1.0
    exchange_rate_data = pd.concat([exchange_rate_data, exchange_rate_data_reverse, exchange_rate_data_same])
    exchange_rate_data = exchange_rate_data.loc[exchange_rate_data["to_iso"] == "USD"]

    position_data = position_data.merge(exchange_rate_data.rename(columns={ "from_iso" : "altname" }), how="left", on=["altname"])
    position_data["market_value_usd"] = position_data["position"] * position_data["close"]

    return position_data[["name","altname","position","market_value_usd"]]

@task
def add_order(pair: str, action_type: str, volume: float, price: float, validate: bool = False):
    nonce = str(int(1000 * time.time()))
    api_url = 'https://api.kraken.com'
    uri_path = '/0/private/AddOrder'
    data = {
        'nonce': nonce,
        'ordertype': 'limit',
        'type': action_type,
        'volume': volume,
        'pair': pair,
        'price': price,
        'validate': validate
    }
    headers = get_kraken_api_headers(uri_path, data)
    response = requests.request("POST", api_url + uri_path, headers=headers, data=data)
    return response.json()


@task
def get_currency_data() -> pd.DataFrame:
    return utils.query_table("SELECT currency,iso FROM \"Currency\"")

@flow(task_runner=ThreadPoolTaskRunner(max_workers=20))
def triangular_arbitrage(ignore_currency_isos: list[str] = [], threshold = 2, fee = 0.4, wait_seconds: float = 2.0, n_iterations: int = 600):
    logger = get_run_logger()

    # 1. Get unique pairs.
    currency = get_currency_data()
    if len(ignore_currency_isos) != 0:
        currency = currency.loc[~currency["iso"].isin(ignore_currency_isos)]
    pairs = utils.query_table("""
    SELECT
        FROM_CURRENCY,
        TO_CURRENCY,
        CLOSE,
        VOLUME
    FROM
        "DailyProviderCurrencyMarket"
    WHERE
        DATE IN (
            SELECT
                MAX(DATE)
            FROM
                "DailyProviderCurrencyMarket"
	)""")
    pairs = pairs.merge(currency[["iso","currency"]].rename(columns={ "iso": "from_iso", "currency": "from_currency" }), how="left", on=["from_currency"])
    pairs = pairs.merge(currency[["iso","currency"]].rename(columns={ "iso": "to_iso", "currency": "to_currency" }), how="left", on=["to_currency"])
    pairs["pair"] = pairs["from_iso"] + pairs["to_iso"]

    # 2. Get all combinations from current currencies.
    tris = list(itertools.combinations(currency["iso"].to_list(),3))
    pairs_set = set([ tuple([x["from_iso"],x["to_iso"]]) for x in pairs.to_dict("records") ])
    triangular_groups = []
    for x in tris:
        if (tuple([str(x[0]),str(x[1])]) in pairs_set) and (tuple([str(x[2]),str(x[1])]) in pairs_set)and (tuple([str(x[2]),str(x[0])]) in pairs_set):
            triangular_groups.append(x)
    
    # 3. Filter triangular groups based on current positions held.
    groups = []
    # balance = get_current_balance()
    # balance = balance.loc[balance["market_value_usd"] > min_position_size]
    for group in triangular_groups:

        example_pairs = pairs.loc[pairs["from_iso"].isin(group) & pairs["to_iso"].isin(group)].copy(deep=True)
        
        pairs_grouping_1 = pairs.loc[pairs["from_iso"].isin(group) & pairs["to_iso"].isin(group)].groupby(by=["from_iso"]).size().reset_index(name="count")
        left = pairs_grouping_1.loc[pairs_grouping_1["count"] == pairs_grouping_1["count"].max()]["from_iso"].to_list()[0]
        pairs_grouping_2 = pairs.loc[pairs["from_iso"].isin(group) & pairs["to_iso"].isin(group)].groupby(by=["to_iso"]).size().reset_index(name="count")
        right = pairs_grouping_2.loc[pairs_grouping_1["count"] == pairs_grouping_2["count"].max()]["to_iso"].to_list()[0]
        middle = pd.Series(group).loc[~pd.Series(group).isin([left,right])][0]

        first = example_pairs.loc[(example_pairs["from_iso"] == left) & (example_pairs["to_iso"] == middle)][["from_iso","to_iso","pair"]].to_dict("records")[0]
        second =  example_pairs.loc[(example_pairs["to_iso"] == right) & (example_pairs["to_iso"] != middle)][["from_iso","to_iso","pair"]].to_dict("records")[0]
        third = example_pairs.loc[(example_pairs["from_iso"] == left) & (example_pairs["to_iso"] != middle)][["from_iso","to_iso","pair"]].to_dict("records")[0]

        # if (not first.get("from_iso") in balance["altname"].to_list()) or (not second.get("from_iso") in balance["altname"].to_list()) or (not third.get("to_iso") in balance["altname"].to_list()):
        #     continue

        groups.append([first.get("from_iso") + first.get("to_iso"),second.get("from_iso") + second.get("to_iso"),third.get("from_iso") + third.get("to_iso")])
    logger.info(f"Number of groups: {len(groups)}")

    # 4. Filter groups where all pairs are tradeable.
    valid_groups = []
    asset_pairs = get_asset_pairs()
    for group in groups:
        valid = True
        for item in group:
            if not item in asset_pairs["altname"].to_list():
                valid = False
                break
        if valid:
            valid_groups.append(group)
    logger.info(f"Number of valid groups: {len(valid_groups)}")
    valid_currencies = list(set([ x for y in valid_groups for x in y ]))

    # 5. Throw error if there are no groups left.
    if len(valid_groups) == 0:
        logger.warning("No tradeable groups.")
        return

    # 6. Start loop to check multiple times.
    for _ in range(n_iterations):
        
        # a) Get data concurrently.
        valid_groups_df = get_all_currency_tickers_data()
        valid_groups_df = valid_groups_df.loc[valid_groups_df["altname"].isin(valid_currencies) | valid_groups_df["name"].isin(valid_currencies)]

        # b) Query current close from all market status.
        groups_df = pd.DataFrame([ { "group" : i + 1, "order" : j + 1, "altname" : altname } for i,y in enumerate(valid_groups) for j,altname in enumerate(y) ])
        groups_pivot_df = groups_df.merge(valid_groups_df,how="left",on=["altname"]).pivot(index=["group"],columns=["order"],values=["altname", "ask","bid"])
        groups_pivot_df["bid_1"] = groups_pivot_df["bid"][1]
        groups_pivot_df["bid_2"] = groups_pivot_df["bid"][2]
        groups_pivot_df["ask_3"] = groups_pivot_df["ask"][3]
        groups_pivot_df["current_profit"] = groups_pivot_df["bid"][1] * groups_pivot_df["bid"][2] * (1 / groups_pivot_df["ask"][3])
        groups_pivot_df["pairs"] = groups_pivot_df["altname"].apply(lambda x: [x[1],x[2],x[3]], axis=1)
        groups_pivot_df = pd.DataFrame({ "pairs" : pd.Series(groups_pivot_df["pairs"].to_list()), "bid_1": pd.Series(groups_pivot_df["bid_1"].to_list()), "bid_2":pd.Series(groups_pivot_df["bid_2"].to_list()), "ask_3": pd.Series(groups_pivot_df["ask_3"].to_list()), "current_profit" : pd.Series(groups_pivot_df["current_profit"].to_list()) })
        
        # c) Take the greatest profit.
        max_profit = groups_pivot_df.loc[groups_pivot_df["current_profit"] == groups_pivot_df["current_profit"].max()].to_dict("records")[0]

        # d) Execute trade if the profit exceeds threshold.
        logger.info(f"Max profit: {max_profit.get("current_profit")}")
        if max_profit.get("current_profit") > 1 + (((fee * 3) + threshold) / 100):
            logger.info(f"First: pair = {max_profit.get("pairs")[0]}, bid = {max_profit.get("bid_1")}")
            logger.info(f"Second: pair = {max_profit.get("pairs")[1]}, bid = {max_profit.get("bid_2")}")
            logger.info(f"Third: pair = {max_profit.get("pairs")[2]}, ask = {max_profit.get("ask_3")}")
            pairs = pd.DataFrame({
                "pair": pd.Series(max_profit.get("pairs")), 
                "type": pd.Series(["bid","bid","ask"]), 
                "price": pd.Series([max_profit.get("bid_1"), max_profit.get("bid_2"), max_profit.get("ask_3")]) 
                })
            body = f"""
            {pairs.to_html(index=False)}
            Profit: {max_profit.get("current_profit")}
            """
            send_email("service@manningcapital.co.uk", ["glynfinck97@gmail.com"], "Triangular Arbitrage Detected",body)

        # e) Sleep for wait seconds.
        time.sleep(wait_seconds * 1.0)

if __name__ == "__main__":
    triangular_arbitrage()