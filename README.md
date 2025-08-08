# Project Start:
**Terminal 1**:
```bash
cd ~/izpi
uvicorn api.main:app --reload --port 9000
```
**Terminal 2**  
```bash
cd ~/izpi/web
python -m http.server 8000
```

**Site Entry:**  
[http://localhost:8000](http://localhost:8000)

---

# API Info

**Base URL:**  
```
https://api.geckoterminal.com/api/v2
```
- `BASE_URL`: https://api.geckoterminal.com/api/v2
- `network`: solana, eth, ...
- `dex`: orca, uniswap, raydium, ...

**Get top pools in a network's DEX**
```python
endpoint = f"{BASE_URL}/networks/{network}/dexes/{dex}/pools?page=10&sort={sort}"
# sort: h24_volume_usd_desc, h24_tx_count_desc
```
    
**Get TVL value for up to 30 given pool ids**
```python
endpoint = f"{BASE_URL}/networks/{network}/pools/multi/{pool_id_0}%2C{pool_id_1}%2C{pool_id_2}"
# Result:
# tvl = data["data"][n]["attributes"]["reserve_in_usd"]
```

**Get ohlcv values for a given pool**
```python
endpoint = f"{BASE_URL}/networks/{network}/pools/{pool_id}/ohlcv/{timeframe}?aggregate={aggregate}&limit={limit}&before_timestamp={before_timestamp}&currency={currency}"
# timeframe: day, hour, minute
# aggregate: day: 1 | hour: 1, 4, 12 | minute: 1, 5, 15
# limit: max 1000
# before_timestamp: seconds in unix format (e.g. 1679414400)
# currency: token, usd
```

---

## Pool Metadata Structure:  (`<pool_address>.json`)
```json
{
    "meta": {
        "pool_address": "str",
        "name": "str",
        "fee": "float",
        "network": "str",
        "dex": "str",
        "base": {
            "address": "str",
            "name": "str",
            "symbol": "str"
        },
        "quote": {
            "address": "str",
            "name": "str",
            "symbol": "str"
        },
        "pool_created_at": ["int", "str"],
        "metadata_last_update": ["int", "str"]
    },
    "data": [
        {
            "epoch": ["int", "str"],
            "tvl": "float",
            "open": ["float", "float"],
            "high": ["float", "float"],
            "low": ["float", "float"],
            "close": ["float", "float"],
            "volume": "float",
            "hour_data": [
                {
                    "epoch": ["int", "str"],
                    "open": ["float", "float"],
                    "high": ["float", "float"],
                    "low": ["float", "float"],
                    "close": ["float", "float"],
                    "volume": "float",
                },
                {
                    "..."
                }
            ]
        },
        {
            "..."
        }
    ]
}
```
