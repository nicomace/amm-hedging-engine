import requests
import pandas as pd
import re
from datetime import datetime

BASE_URL = "https://api.lyra.finance"


# retrieve all options instruments
def fetch_instruments(curr):
    url = f"{BASE_URL}/public/get_all_instruments"

    payload = {
        "currency": curr,
        "expired": False,
        "instrument_type": "option",
        "page": 1,
        "page_size": 1000
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


# keep only instrument_name
def data_clean_inst(data):
    rows = []
    instruments = data["result"]["instruments"]

    active_instruments = [inst for inst in instruments if inst.get("is_active") == True]
    # print(active_instruments)

    for inst in active_instruments:
        rows.append({
            "instrument_name": inst.get("instrument_name")
        })
    return rows


# retrieve details (expiry, fees, ...)
def fetch_tickers(instrument_name):
    url = f"{BASE_URL}/public/get_instrument"

    payload = {
        "instrument_name": instrument_name,
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


# retrieve live market data (bid, ask, ...)
def fetch_details(currency, expiry_date):
    url = f"{BASE_URL}/public/get_tickers"

    payload = {
        # instrument_name": inst_name,
        "currency": currency,
        "instrument_type": "option",
        "expiry_date": expiry_date
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


# enrichment with live market data (bid, ask, ...)
def clean_details(live_data, instru):
    tickers = (live_data or {}).get("tickers") or {}
    rows = []
    missing = 0

    if not isinstance(tickers, dict):
        raise TypeError(f"tickers devrait être un dict, reçu: {type(tickers)}")

    for inst in instru:
        datat = tickers.get(inst)

        if datat is None:
            missing += 1
            # on garde quand même une ligne vide pour merge (optionnel)
            continue

        op = datat.get("option_pricing") or {}

        if not isinstance(datat, dict):
            raise TypeError(f"ticker[{inst}] devrait être un dict, reçu: {type(datat)}")

        rows.append({
            "instrument_name": inst,
            "ask_price": float(datat.get("a", 0) or 0),
            "ask_size": float(datat.get("A", 0) or 0),
            "bid_price": float(datat.get("b", 0) or 0),
            "bid_size": float(datat.get("B", 0) or 0),
            "market_price": float(datat.get("I", 0) or 0),
            "delta": float(op.get("d", 0) or 0),
            "gamma": float(op.get("g", 0) or 0),
            "vega": float(op.get("v", 0) or 0),
            "theta": float(op.get("t", 0) or 0),
            "iv": float(op.get("i", 0) or 0),
            "mark": float(op.get("m", 0) or 0),
            "ts": int(datat.get("t", 0) or 0),
        })

    new_df = pd.DataFrame(rows)

    return new_df

# TODO - adapt clean_details on for with new df. then merge outside on main function


# BTC-20260925-95000-C
def unitary_test(inst_name):
    # url = f"{BASE_URL}/public/get_instrument"
    url = f"{BASE_URL}/public/get_tickers"

    payload = {
        # instrument_name": inst_name,
        "currency": "BTC",
        "instrument_type": "option",
        "expiry_date": "20260925"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


if __name__ == "__main__":

    # test = unitary_test("BTC-20260925-95000-C")

    currency = "BTC"
    expiry = "20260925"  # YYYYMMDD

    data = fetch_instruments(currency)
    result = data_clean_inst(data)
    options_details = []

    for res in result:
        instrument_name = res["instrument_name"]

        ticker_json = fetch_tickers(instrument_name)["result"]
        opt = ticker_json.get("option_details")

        options_details.append({
            "index": opt["index"],
            "instrument_name": ticker_json.get("instrument_name"),
            "instrument_type": ticker_json.get("instrument_type"),
            "is_active": ticker_json.get("is_active"),
            "tick_size": ticker_json.get("tick_size"),
            "option_type": opt["option_type"],
            "strike": float(opt["strike"]),
            "expiry": opt["expiry"],
            "minimum_amount": float(ticker_json.get("minimum_amount", 0) or 0),
            "amount_step": float(ticker_json.get("amount_step", 0) or 0),
            "maker_fee_rate": float(ticker_json.get("maker_fee_rate", 0) or 0),
            "taker_fee_rate": float(ticker_json.get("taker_fee_rate", 0) or 0),
        })

    df = pd.DataFrame(options_details)
    df["expiry"] = pd.to_datetime(df["expiry"], unit="s", utc=True).dt.strftime("%Y%m%d")
    list_expiry = df["expiry"].dropna().unique().tolist()

    list_instrument = df["instrument_name"].astype(str).str.strip().tolist()
    # live_data = fetch_details(str(df["instrument_name"].iloc[0][:3]), int(df["expiry"].iloc[0]))["result"]
    all_details = []

    for exp in list_expiry:
        live_data = fetch_details(currency, exp)["result"]
        details_df = clean_details(live_data, list_instrument)
        details_df["expiry"] = exp
        all_details.append(details_df)

    details_all = pd.concat(all_details, ignore_index=True)
    df2 = df.copy()
    df_merged = df2.merge(details_all, on=["instrument_name", "expiry"], how="left")

    print("live_data keys:", live_data.keys())
    print("tickers type:", type(live_data.get("tickers")))
    print("tickers len:", len(live_data.get("tickers") or {}))

    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"options_snapshot_{now}.csv"
    df_merged.to_csv(filename, index=False)

    df_merged = df_merged[df_merged["ask_price"] != 0]

    print(df_merged.head(5))



