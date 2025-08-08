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
        "pool_address": "str",    // the pool's address
        "name": "str",            // the name of the pool
        "fee": "float",           // the pool's fee in percentage
        "network": "str",         // the network where the pool is (solana, ethereum, arbitrum ...)
        "dex": "str",             // the dex where the pool is (orca, meteora, raydium ...)
        "base": {
            "address": "str",     // the base token's address
            "name": "str",        // the base token's name
            "symbol": "str"       // the base token's symbol
        },
        "quote": {
            "address": "str",     // the quote token's symbol
            "name": "str",        // the quote token's symbol
            "symbol": "str"       // the quote token's symbol
        },
        "pool_created_at": ["int", "str"],
        "metadata_last_update": ["int", "str"] // the last update date in timestamp unix and iso8601.UTC format
    },
    "data": [
        {
            "epoch": ["int", "str"],                                    // timestamp in unix and iso8601.UTC
            "tvl": "float",                                             // the total value locked or liquidity for the pool in $USD
            "open": ["float", "float"],             // the open value in $USD and token value
            "high": ["float", "float"],             // the high value in $USD and token value
            "low": ["float", "float"],              // the low value in $USD and token value
            "close": ["float", "float"],            // the close value in $USD and token value
            "volume": "float",                                          // the volume value in $USD
            "hour_data": [                                          // the ohlcv data per hour, descendant -> [{23:00}, {22:00}, ..., {00:00}]
                {
                    "epoch": ["int", "str"],                            // timestamp in unix and iso8601.UTC
                    "open": ["float", "float"],     // the open value in $USD and token value
                    "high": ["float", "float"],     // the high value in $USD and token value
                    "low": ["float", "float"],      // the low value in $USD and token value
                    "close": ["float", "float"],    // the close value in $USD and token value
                    "volume": "float",                                  // the volume value in $USD
                },
                //...
            ]
        },
        //...
    ] 
}
```
