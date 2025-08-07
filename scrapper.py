from dune_client.client import DuneClient
from datetime import datetime, timezone
from tqdm import tqdm
import re
import time
import os
import requests
import json


CALLS_COUNTER = 0
def call_get_request(endpoint):
    # Makes the request to the API and counts the calls.
    # Waits 0.25s between calls and at 30 calls waits for 65s.
    # If the status_code is not 200OK, retries up to 30 times.  
    global CALLS_COUNTER
    max_retries = 30
    retries = 0
    while retries < max_retries:
        if CALLS_COUNTER >= 30:
            print("Limit of 30 calls/min reached, waiting for 65s")
            for i in range(65, 0, -1):
                print(f"{i}s", end="\r")
                time.sleep(1)
            CALLS_COUNTER = 0
        response = requests.get(endpoint)
        CALLS_COUNTER += 1
        time.sleep(0.25)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            print("Data not available for input timestamp")
            return None
        retries += 1
        print(f"\nRetry {retries}/{max_retries}: Error {response.status_code}, retrying...")

####################################################################################################
#########################################  GET - FUNCTIONS  ########################################
####################################################################################################

def get_top_pools_info(network, dex, top_pools_info=None, sort: str="default"):
    # Get top 200 pools on dex from geckoterminal, sorted by default (trend), volume_usd or tx_count
    pools_info = []
    print(f"Getting top 200 pools on {network}/{dex}")
    for n in tqdm(range(10)):
        endpoint = f"https://api.geckoterminal.com/api/v2/networks/{network}/dexes/{dex}/pools?page={n+1}"
        if sort in ("h24_volume_usd_desc","h24_tx_count_desc"):
            endpoint += f"&sort={sort}"
        response_data = call_get_request(endpoint)
        repeated_pools = 0
        for entry in response_data["data"]:
            entry_dict = {
                "name": entry["attributes"]["name"],
                "network": network,
                "dex": dex,
                "address": entry["attributes"]["address"],
                "pool_created_at": entry["attributes"]["pool_created_at"]
                }
            if len(pools_info)>0:            
                if not entry_dict in pools_info:
                    pools_info.append(entry_dict)
                else:
                    repeated_pools+=1
                    print("REPEATED POOL:", entry_dict)
            else:
                pools_info.append(entry_dict)
    print("Total Repeated Pools:",repeated_pools)
    
    # Writes or updates the total_top_pools.json file with the new pools
    if top_pools_info == None:
        top_pools_info = pools_info
    else:
        new_top_pools_info = [pool for pool in pools_info if pool not in top_pools_info]
        top_pools_info.extend(new_top_pools_info)
        top_pools_info = sorted(top_pools_info, key=lambda x: x["name"])
    with open("./metadata/pools/top_pools_info.json", "w", encoding="utf-8") as f:
        for item in top_pools_info:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    return top_pools_info

def get_dune_query_data(dune_api_key, query_id, top_pools_info):
    # Create SQL Query for top_pools and write on a .sql
    pool_addresses_1 = ""
    pool_addresses_2 = ""
    for i,item in enumerate(top_pools_info):
        pool_addresses_1 = f"{pool_addresses_1}\t\t\t'{item['address']}',\n"
        pool_addresses_2 = f"{pool_addresses_2}\tMAX(CASE WHEN whirlpool_id = '{item['address']}' THEN tvl END) AS p{i+1},\n"
        top_pools_info[i]["id"] = f"p{i+1}"
        
    pool_addresses_1 = pool_addresses_1[:-2]
    pool_addresses_2 = pool_addresses_2[:-2]
    with open("./metadata/queries/dune_query_sheet.sql",'r',encoding='utf-8') as fi:
        with open("./metadata/queries/dune_query.sql",'w',encoding='utf-8') as fo:
            dune_query = fi.read()
            dune_query = re.sub(r"pool_addresses_1", pool_addresses_1, dune_query)
            dune_query = re.sub(r"pool_addresses_2", pool_addresses_2, dune_query)
            fo.write(dune_query)
    
    # Get Query data from Dune and write on a .json
    input(
        f"""Manually Execute the Query:
    - Copy the Query from: './metadata/queries/dune_query.sql'
    - Paste and Run at: https://dune.com/queries/{query_id}
    --- Press 'ENTER' when the Query is finished ---"""
    )
    dune = DuneClient(dune_api_key)
    query_result = dune.get_latest_result(query_id)
    query_data = query_result.result.rows
    query_data_dict = {item["time"]: item for item in query_data} # An intermediate dict for easier and faster search
    with open("./metadata/queries/dune_query_result.json", "w", encoding="utf-8") as json_file:
        json.dump(query_data_dict, json_file, ensure_ascii=False)
    
    # Checks if the pools have data on dune and marks them on a new .json
    available_pools=0
    for i,item in enumerate(top_pools_info):
        if query_data[0][item["id"]] == None:
            top_pools_info[i]["tvl_history_available"] = False
        else:
            available_pools+=1
            top_pools_info[i]["tvl_history_available"] = True
    print(f"TVL data available for {available_pools}/{len(top_pools_info)} pools")
    with open("./metadata/pools/top_pools_info.json", "w", encoding="utf-8") as f:
        for item in top_pools_info:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    return query_data_dict, top_pools_info

def get_ohlcv_info(pool_info, before_timestamp, limit: int=1000, timeframe: str="day", currency: str="usd"):
    # Get the ohlcv data for a given pool
    # timeframe = day, hour
    # currency = usd, token
    endpoint = f"https://api.geckoterminal.com/api/v2/networks/{pool_info['network']}/pools/{pool_info['address']}/ohlcv/{timeframe}?aggregate=1&limit={limit}&currency={currency}&before_timestamp={before_timestamp}"
    response_data = call_get_request(endpoint)
    return response_data

def get_tvl_info(top_pools_info):
    # Get TVL data for available pools, groups them in calls of 30 pools to the geckoterminal API
    available_pools_info = [item for item in top_pools_info if item["tvl_history_available"]==True]
    pool_batch_list = [available_pools_info[i:i+30] for i in range(0, len(available_pools_info), 30)]
    network = available_pools_info[0]["network"]
    pools_tvl_info = {}
    for pool_batch in pool_batch_list:
        endpoint = f"https://api.geckoterminal.com/api/v2/networks/{network}/pools/multi/"
        for pool in pool_batch:
            endpoint = endpoint + f"{pool['address']}%2C"
        endpoint = endpoint[:-3]
        response_data = call_get_request(endpoint)
        for tvl_info in response_data["data"]:
            pools_tvl_info[tvl_info["attributes"]["address"]] = float(tvl_info["attributes"]["reserve_in_usd"])
    return pools_tvl_info

####################################################################################################
##################################  CREATE / UPDATE - FUNCTIONS  ###################################
####################################################################################################

def create_pool_metadata(pool_info, query_data_dict, utc_now):
    # Creates the pool metadata on "pool_dayhour_metadata.json" format
    # If the .json already exists, updates the missing data
    # Get pool meta and data for day format before the actual day
    utc_midnight = datetime(utc_now.year, utc_now.month, utc_now.day, 0, 0, 0, tzinfo=timezone.utc)
    timestamp = int(utc_midnight.timestamp())
    json_filepath = f"./metadata/pools/pools_metadata/{pool_info['address']}.json"

    if os.path.exists(json_filepath):
        print(f"Updating metadata for pool {pool_info['name']} ({pool_info['address']})")
        with open(json_filepath, "r", encoding="utf-8") as json_infile:
            metadata = json.load(json_infile)
        utc_metadata_last_update = datetime.fromtimestamp(metadata["meta"]["metadata_last_update"][0], tz=timezone.utc)
        if utc_metadata_last_update >= utc_midnight:
            print(f"Pool {pool_info['name']} ({pool_info['address']}) metadata is already up to date.")
            return
        timediff = utc_now - utc_metadata_last_update
        limit_days = timediff.days - 1
    else:
        print(f"Creating metadata for pool {pool_info['name']} ({pool_info['address']})")
        limit_days = 1000

    # Calls for getting the days data
    response_data_usd = get_ohlcv_info(pool_info, limit=limit_days, before_timestamp=timestamp, currency="usd")
    response_data_token = get_ohlcv_info(pool_info, limit=limit_days, before_timestamp=timestamp, currency="token")
    meta = {
        "pool_address": pool_info["address"],      # the pool's address
        "name": pool_info["name"],                 # the pool's name
        "fee": "<EMPTY>",                          # the pool's fee in percentage (must be searched manually)
        "network": pool_info["network"],           # the network where the pool is (solana, ethereum, arbitrum ...)
        "dex": pool_info["dex"],                   # the dex where the pool is (orca, meteora, raydium ...)
        "base": {
            "address": response_data_usd["meta"]["base"]["address"],     # the base token's address
            "name": response_data_usd["meta"]["base"]["name"],           # the base token's name
            "symbol": response_data_usd["meta"]["base"]["symbol"]        # the base token's symbol
        },
        "quote": {
            "address": response_data_usd["meta"]["quote"]["address"],    # the quote token's address
            "name": response_data_usd["meta"]["quote"]["name"],          # the quote token's name
            "symbol": response_data_usd["meta"]["quote"]["symbol"]       # the quote token's symbol
        },
        "pool_created_at": [int(datetime.fromisoformat(pool_info["pool_created_at"]).timestamp()), pool_info["pool_created_at"]],  # the pool creation time in timestamp unix and iso8601.UTC format
        "metadata_last_update": [int(utc_now.timestamp()), utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')]      # the last update in timestamp unix and iso8601.UTC format
    }
    data = []
    for i, ohlcv_usd_entry in enumerate(response_data_usd["data"]["attributes"]["ohlcv_list"]):
        ohlcv_token_entry = response_data_token["data"]["attributes"]["ohlcv_list"][i]
        epoch = datetime.fromtimestamp(ohlcv_usd_entry[0], tz=timezone.utc)
        tvl_data = query_data_dict.get(epoch.strftime('%Y-%m-%dT%H:%M:%SZ'), {}).get(pool_info["id"], None)
        day_item = {
            "epoch": [int(epoch.timestamp()), epoch.strftime('%Y-%m-%dT%H:%M:%SZ')],
            "tvl": tvl_data,
            "open": [ohlcv_usd_entry[1], ohlcv_token_entry[1]],
            "high": [ohlcv_usd_entry[2], ohlcv_token_entry[2]],
            "low": [ohlcv_usd_entry[3], ohlcv_token_entry[3]],
            "close": [ohlcv_usd_entry[4], ohlcv_token_entry[4]],
            "volume": ohlcv_usd_entry[5],
            "hour_data": []
        }
        data.append(day_item)

    # Calls for getting the hours data
    hours = len(data)*24
    limit_batch = [1000] * (hours // 1000) + ([hours % 1000] if hours % 1000 else [])
    data_dict = {item["epoch"][1][:10]: item for item in data} # An intermediate dict sorted by epoch str for faster search
    for limit in limit_batch:
        response_data_usd = get_ohlcv_info(pool_info, limit=limit, before_timestamp=timestamp, timeframe="hour", currency="usd")
        response_data_token = get_ohlcv_info(pool_info, limit=limit, before_timestamp=timestamp, timeframe="hour", currency="token")
        if not response_data_usd == None:
            for i, ohlcv_usd_entry in enumerate(response_data_usd["data"]["attributes"]["ohlcv_list"]):
                ohlcv_token_entry = response_data_token["data"]["attributes"]["ohlcv_list"][i]
                epoch = datetime.fromtimestamp(ohlcv_usd_entry[0], tz=timezone.utc)
                hour_item = {
                    "epoch": [int(epoch.timestamp()), epoch.strftime('%Y-%m-%dT%H:%M:%SZ')],
                    "open": [ohlcv_usd_entry[1], ohlcv_token_entry[1]],
                    "high": [ohlcv_usd_entry[2], ohlcv_token_entry[2]],
                    "low": [ohlcv_usd_entry[3], ohlcv_token_entry[3]],
                    "close": [ohlcv_usd_entry[4], ohlcv_token_entry[4]],
                    "volume": ohlcv_usd_entry[5]
                }
                if day_item := data_dict.get(epoch.strftime("%Y-%m-%d")):
                    day_item["hour_data"].append(hour_item) # day_data_dict elements point to the actual day_data elements, so the day_items modify the actual value of the original list
            timestamp = hour_item["epoch"][0]

    if os.path.exists(json_filepath):
        data.extend(metadata["data"])
        metadata["data"] = data
        metadata["meta"]["metadata_last_update"] = [int(utc_now.timestamp()), utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')]
    else:
        metadata = {"meta": meta, "data": data}

    with open(json_filepath, "w", encoding="utf-8") as json_file:
        json.dump(metadata, json_file, ensure_ascii=False)
  
def daily_update_pool_metadata(pool_info, utc_now, pools_tvl_info):
    # Updates the ohlcv and tvl data for existing and available pools, for the past day
    utc_midnight = datetime(utc_now.year, utc_now.month, utc_now.day, 0, 0, 0, tzinfo=timezone.utc)
    timestamp = int(utc_midnight.timestamp())
    json_filepath = f"./metadata/pools/pools_metadata/{pool_info['address']}.json"
    print(f"Updating daily metadata for pool {pool_info['name']} ({pool_info['address']})")
    with open(json_filepath, "r", encoding="utf-8") as json_infile:
        metadata = json.load(json_infile)

    # Calls for getting the day data
    response_data_usd = get_ohlcv_info(pool_info, limit=1, before_timestamp=timestamp, currency="usd")
    response_data_token = get_ohlcv_info(pool_info, limit=1, before_timestamp=timestamp, currency="token")
    ohlcv_usd_entry = response_data_usd["data"]["attributes"]["ohlcv_list"][0]
    ohlcv_token_entry = response_data_token["data"]["attributes"]["ohlcv_list"][0]
    epoch = datetime.fromtimestamp(ohlcv_usd_entry[0], tz=timezone.utc)
    day_item = {
        "epoch": [int(epoch.timestamp()), epoch.strftime('%Y-%m-%dT%H:%M:%SZ')],
        "tvl": pools_tvl_info[pool_info["address"]],
        "open": [ohlcv_usd_entry[1], ohlcv_token_entry[1]],
        "high": [ohlcv_usd_entry[2], ohlcv_token_entry[2]],
        "low": [ohlcv_usd_entry[3], ohlcv_token_entry[3]],
        "close": [ohlcv_usd_entry[4], ohlcv_token_entry[4]],
        "volume": ohlcv_usd_entry[5],
        "hour_data": []
    }

    # Calls for getting the hours data
    response_data_usd = get_ohlcv_info(pool_info, limit=24, before_timestamp=timestamp, timeframe="hour", currency="usd")
    response_data_token = get_ohlcv_info(pool_info, limit=24, before_timestamp=timestamp, timeframe="hour", currency="token")
    for i, ohlcv_usd_entry in enumerate(response_data_usd["data"]["attributes"]["ohlcv_list"]):
        ohlcv_token_entry = response_data_token["data"]["attributes"]["ohlcv_list"][i]
        epoch = datetime.fromtimestamp(ohlcv_usd_entry[0], tz=timezone.utc)
        hour_item = {
            "epoch": [int(epoch.timestamp()), epoch.strftime('%Y-%m-%dT%H:%M:%SZ')],
            "open": [ohlcv_usd_entry[1], ohlcv_token_entry[1]],
            "high": [ohlcv_usd_entry[2], ohlcv_token_entry[2]],
            "low": [ohlcv_usd_entry[3], ohlcv_token_entry[3]],
            "close": [ohlcv_usd_entry[4], ohlcv_token_entry[4]],
            "volume": ohlcv_usd_entry[5]
        }
        day_item["hour_data"].append(hour_item)
    
    metadata["meta"]["metadata_last_update"] = [int(utc_now.timestamp()), utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')]
    metadata["data"].insert(0,day_item)
    with open(json_filepath, "w", encoding="utf-8") as json_outfile:
        json.dump(metadata, json_outfile, ensure_ascii=False)

####################################################################################################
##############################################  MAIN  ##############################################
####################################################################################################

def pools_creation(network, dex):
    with open("./metadata/keys/dune_api_key", "r", encoding="utf-8") as f:
        dune_api_key = f.read().strip()
    utc_now = datetime.now(timezone.utc)
    query_id = 4516255
    if not os.path.exists("./metadata/pools/top_pools_info.json"):
        top_pools_info = None
    else:
        with open("./metadata/pools/top_pools_info.json", "r", encoding="utf-8") as f:
            top_pools_info = [json.loads(line) for line in f]

    top_pools_info = get_top_pools_info(network, dex, top_pools_info=top_pools_info)
    top_pools_info = get_top_pools_info(network, dex, top_pools_info=top_pools_info, sort="h24_volume_usd_desc")
    top_pools_info = get_top_pools_info(network, dex, top_pools_info=top_pools_info, sort="h24_tx_count_desc")
    query_data_dict, top_pools_info = get_dune_query_data(dune_api_key, query_id, top_pools_info)
    for pool_info in tqdm(top_pools_info):
        if pool_info["tvl_history_available"]:
            create_pool_metadata(pool_info, query_data_dict, utc_now)
        else:
            print("TVL history not available for pool:", pool_info["name"], pool_info["address"])

def pools_daily_update():
    utc_now = datetime.now(timezone.utc)
    with open("./metadata/pools/top_pools_info.json", "r", encoding="utf-8") as f:
        top_pools_info = [json.loads(line) for line in f]
    pools_tvl_info = get_tvl_info(top_pools_info)
    for pool_info in tqdm(top_pools_info):
        if pool_info["tvl_history_available"]:
            daily_update_pool_metadata(pool_info, utc_now, pools_tvl_info)

if __name__ == "__main__":
    pools_creation(network="solana", dex="orca")