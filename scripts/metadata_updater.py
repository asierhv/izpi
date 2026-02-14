from datetime import datetime, timezone
from tqdm import tqdm
import json
import sys
import metadata_scrapper as ms

####################################################################################################
#########################################  GET - FUNCTIONS  ########################################
####################################################################################################

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
        response_data = ms.call_get_request(endpoint)
        for tvl_info in response_data["data"]:
            pools_tvl_info[tvl_info["attributes"]["address"]] = float(tvl_info["attributes"]["reserve_in_usd"])
    return pools_tvl_info

####################################################################################################
##################################  CREATE / UPDATE - FUNCTIONS  ###################################
####################################################################################################

def check_metadata_last_update(metadata, utc_midnight):
    # Checks if the metadata for the pool is up to date, returns True if it is, False otherwise
    utc_metadata_last_update = datetime.fromtimestamp(metadata["meta"]["metadata_last_update"][0], tz=timezone.utc)
    if utc_metadata_last_update >= utc_midnight:
        tqdm.write(f"Metadata for pool {metadata['meta']['name']} ({metadata['meta']['pool_address']}) is UP to date.")
        return True
    else:
        tqdm.write(f"Metadata for pool {metadata['meta']['name']} ({metadata['meta']['pool_address']}) is NOT UP to date.")
        tqdm.write(f"Last update: {utc_metadata_last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        return False

def daily_update_pool_metadata(pool_info, utc_now, pools_tvl_info):
    # Updates the ohlcv and tvl data for existing and available pools, for the past day
    utc_midnight = datetime(utc_now.year, utc_now.month, utc_now.day, 0, 0, 0, tzinfo=timezone.utc)
    timestamp = int(utc_midnight.timestamp())
    json_filepath = f"./metadata/pools/pools_metadata/{pool_info['address']}.json"
    
    with open(json_filepath, "r", encoding="utf-8") as json_infile:
        metadata = json.load(json_infile)
    if check_metadata_last_update(metadata, utc_midnight):
        return
    
    # Calls for getting the days data
    tqdm.write(f"Updating daily metadata for pool {pool_info['name']} ({pool_info['address']})")
    day_limit = 3 # 2 days + current day (just in case, later only the missing days will be added)
    response_data_usd = ms.get_ohlcv_info(pool_info, limit=day_limit, before_timestamp=timestamp, currency="usd")
    response_data_token = ms.get_ohlcv_info(pool_info, limit=day_limit, before_timestamp=timestamp, currency="token")
    data = []
    for i, ohlcv_usd_entry in enumerate(response_data_usd["data"]["attributes"]["ohlcv_list"]):
        ohlcv_token_entry = response_data_token["data"]["attributes"]["ohlcv_list"][i]
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
        data.append(day_item)

    # Calls for getting the hours data
    hours = len(data)*24
    limit_batch = [1000] * (hours // 1000) + ([hours % 1000] if hours % 1000 else [])
    data_dict = {item["epoch"][1][:10]: item for item in data} # An intermediate dict sorted by epoch str for faster search
    for limit in limit_batch:
        response_data_usd = ms.get_ohlcv_info(pool_info, limit=limit, before_timestamp=timestamp, timeframe="hour", currency="usd")
        response_data_token = ms.get_ohlcv_info(pool_info, limit=limit, before_timestamp=timestamp, timeframe="hour", currency="token")
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
                    day_item["hour_data"].append(hour_item)
            timestamp = hour_item["epoch"][0]
    
    existing_epoch_list = [item["epoch"][0] for item in metadata["data"]]
    new_data = [item for item in data if item["epoch"][0] not in existing_epoch_list and not int(utc_midnight.timestamp()) == item["epoch"][0]]
    if len(new_data) > 1:
        tqdm.write(f"Warning: More than one day of data found for pool {pool_info['name']} ({pool_info['address']})")
        for item in new_data[1:]:
            item["tvl"] = None
    metadata["data"] = new_data + metadata["data"]
    metadata["meta"]["metadata_last_update"] = [int(utc_now.timestamp()), utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')]
    
    with open(json_filepath, "w", encoding="utf-8") as json_outfile:
        json.dump(metadata, json_outfile, ensure_ascii=False)

####################################################################################################
##############################################  MAIN  ##############################################
####################################################################################################

def pools_daily_update():
    # Updates the metadata for all available pools in the top_pools_info.json file 
    utc_now = datetime.now(timezone.utc)
    tqdm.write(f"\n------ POOLS DAILY UPDATE - {utc_now.strftime('%Y-%m-%d %H:%M:%S')} ------\n")

    # Step 1: Get the TVL info for available pools
    with open("./metadata/pools/top_pools_info.json", "r", encoding="utf-8") as f:
        top_pools_info = [json.loads(line) for line in f]
    tqdm.write("\nStep 1: Getting TVL info for available pools...")
    pools_tvl_info = get_tvl_info(top_pools_info)

    #Step 2: Update the metadata for each pool
    tqdm.write("\nStep 2: Updating daily metadata for each pool...")
    available_top_pools_info = [pool_info for pool_info in top_pools_info if pool_info["tvl_history_available"]]
    for pool_info in tqdm(available_top_pools_info, disable=not sys.stdout.isatty()):
        daily_update_pool_metadata(pool_info, utc_now, pools_tvl_info)
    
    #Step 3: Check if all the pools have been updated
    tqdm.write("\nStep 3: Updating daily metadata for each pool...")
    for pool_info in tqdm(available_top_pools_info, disable=not sys.stdout.isatty()):
        daily_update_pool_metadata(pool_info, utc_now, pools_tvl_info)

    tqdm.write(f"\n------ POOLS DAILY UPDATE COMPLETED - {utc_now.strftime('%Y-%m-%d %H:%M:%S')} ------\n")

if __name__ == "__main__":
    pools_daily_update()