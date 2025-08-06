# START:
T1:
~/izpi:
uvicorn api.main:app --reload --port 9000

T2:
~/izpi/web:
python -m http.server 8000

SE:
http://localhost:8000

# Info
Important public API endpoints:

BASE_URL = https://api.geckoterminal.com/api/v2
network = solana, eth, ...
dex = orca, uniswap, raydium, ...

- Get top pools in a network's DEX:
    endpoint = f"{BASE_URL}/networks/{network}/dexes/{dex}/pools?page=10&sort={sort}"
    sort = h24_volume_usd_desc, h24_tx_count_desc
    
- Get TVL value for up to 30 given pool ids:
    endpoint = f"{BASE_URL}/networks/{network}/pools/multi/{pool_id_0}%2C{pool_id_1}%2C{pool_id_2}"
    - Result:
        tvl = data["data"][n]["attributes"]["reserve_in_usd"]

- Get ohlcv values for a given pool:
    endpoint = f"{BASE_URL}/networks/{network}/pools/{pool_id}/ohlcv/{timeframe}?aggregate={aggregate}&limit={limit}&before_timestamp={before_timestamp}&currency={currency}"
    timeframe = day, hour, minute
    aggregate = day: 1 - hour: 1, 4, 12 - minute: 1, 5, 15
    limit: (max 1000)
    before_timestamp: (seconds in unix format, example: 1679414400)
    currency: token, usd

- Data structure for pools:  "<pool_address>.json"
{
    "meta": {
        "pool_address": str,    # the pool's address
        "name": str,            # the name of the pool
        "fee": float,           # the pool's fee in percentage
        "network": str,         # the network where the pool is (solana, ethereum, arbitrum ...)
        "dex": str,             # the dex where the pool is (orca, meteora, raydium ...)
        "base": {
            "address": str,     # the base token's address
            "name": str,        # the base token's name
            "symbol": str       # the base token's symbol
        },
        "quote": {
            "address": str,     # the quote token's symbol
            "name": str,        # the quote token's symbol
            "symbol": str       # the quote token's symbol
        },
        "pool_created_at": [int, str],
        "metadata_last_update": [int, str] # the last update date in timestamp unix and iso8601.UTC format
    },
    "data": [
        {
            "epoch": [int, str],                                    # timestamp in unix and iso8601.UTC
            "tvl": int,                                             # the total value locked or liquidity for the pool in $USD
            "open": [decimal.Decimal, decimal.Decimal],             # the open value in $USD and token value
            "high": [decimal.Decimal, decimal.Decimal],             # the high value in $USD and token value
            "low": [decimal.Decimal, decimal.Decimal],              # the low value in $USD and token value
            "close": [decimal.Decimal, decimal.Decimal],            # the close value in $USD and token value
            "volume": int,                                          # the volume value in $USD
            "hour_data": [                                          # the ohlcv data per hour, descendant -> [{23:00}, {22:00}, ..., {00:00}]
                {
                    "epoch": [int, str],                            # timestamp in unix and iso8601.UTC
                    "open": [decimal.Decimal, decimal.Decimal],     # the open value in $USD and token value
                    "high": [decimal.Decimal, decimal.Decimal],     # the high value in $USD and token value
                    "low": [decimal.Decimal, decimal.Decimal],      # the low value in $USD and token value
                    "close": [decimal.Decimal, decimal.Decimal],    # the close value in $USD and token value
                    "volume": int,                                  # the volume value in $USD
                },
                ...
            ]
        },
        ...
    ] 
}
