WITH
    times AS (
        SELECT 
            q.time,
            ip.account_whirlpool AS whirlpool_id
        FROM query_2254698 q
        JOIN whirlpool_solana.whirlpool_call_initializePool ip
        ON TRUE
        WHERE q.time >= (SELECT MIN(call_block_time) FROM whirlpool_solana.whirlpool_call_initializePool)
        AND q.time >= CURRENT_DATE - INTERVAL '200' DAY
        AND ip.account_whirlpool IN (
pool_addresses_1
        )

    ),
    
    whirlpool_liquidity_a AS (
        SELECT 
            account_whirlpool AS whirlpool_id,
            account_tokenVaultA AS tokenVaultA,
            db.token_mint_address AS tokenA,
            db.token_balance AS tokenA_balance,
            db.day,
            ROW_NUMBER() OVER (PARTITION BY account_tokenVaultA ORDER BY db.day DESC) AS latest
        FROM whirlpool_solana.whirlpool_call_initializePool ip
        LEFT JOIN solana_utils.daily_balances db 
            ON db.address = ip.account_tokenVaultA
            AND db.token_mint_address IS NOT NULL
            AND db.day >= CURRENT_DATE - INTERVAL '200' DAY
    ),
    
    whirlpool_liquidity_b AS (
        SELECT 
            account_whirlpool AS whirlpool_id,
            account_tokenVaultB AS tokenVaultB,
            db.token_mint_address AS tokenB,
            db.token_balance AS tokenB_balance,
            db.day,
            ROW_NUMBER() OVER (PARTITION BY account_tokenVaultB ORDER BY db.day DESC) AS latest
        FROM whirlpool_solana.whirlpool_call_initializePool ip
        LEFT JOIN solana_utils.daily_balances db 
            ON db.address = ip.account_tokenVaultB
            AND db.token_mint_address IS NOT NULL
            AND db.day >= CURRENT_DATE - INTERVAL '200' DAY
    ),
    
    rolling_liquidity AS (
        SELECT 
            time,
            whirlpool_id,
            COALESCE(tokenA, LAG(tokenA, 1) IGNORE NULLS OVER (PARTITION BY whirlpool_id ORDER BY time ASC), LEAD(tokenA, 1) IGNORE NULLS OVER (PARTITION BY whirlpool_id ORDER BY time ASC)) AS tokenA,
            COALESCE(tokenB, LAG(tokenB, 1) IGNORE NULLS OVER (PARTITION BY whirlpool_id ORDER BY time ASC), LEAD(tokenB, 1) IGNORE NULLS OVER (PARTITION BY whirlpool_id ORDER BY time ASC)) AS tokenB,
            COALESCE(tokenA_balance, LAG(tokenA_balance, 1) IGNORE NULLS OVER (PARTITION BY whirlpool_id ORDER BY time ASC)) AS tokenA_balance,
            COALESCE(tokenB_balance, LAG(tokenB_balance, 1) IGNORE NULLS OVER (PARTITION BY whirlpool_id ORDER BY time ASC)) AS tokenB_balance
        FROM (
            SELECT 
                t.time,
                t.whirlpool_id,
                tokenA,
                tokenB,
                tokenA_balance,
                tokenB_balance
            FROM times t 
            LEFT JOIN whirlpool_liquidity_a a 
                ON a.whirlpool_id = t.whirlpool_id AND a.day = t.time
            LEFT JOIN whirlpool_liquidity_b b 
                ON b.whirlpool_id = t.whirlpool_id AND b.day = t.time
        ) base
    ),
    
    tvl_all AS (
        SELECT
            rl.*,
            COALESCE(tokenA_balance * COALESCE(p_a.price, dp_a.median_price), 0) 
            + COALESCE(tokenB_balance * COALESCE(p_b.price, dp_b.median_price), 0) AS tvl
        FROM rolling_liquidity rl
        LEFT JOIN prices.usd p_a 
            ON p_a.blockchain = 'solana' 
            AND toBase58(p_a.contract_address) = rl.tokenA
            AND p_a.minute = rl.time
        LEFT JOIN dune.dune.result_dex_prices_solana dp_a 
            ON dp_a.token_mint_address = rl.tokenA 
            AND rl.time = dp_a.day
        LEFT JOIN prices.usd p_b 
            ON p_b.blockchain = 'solana' 
            AND toBase58(p_b.contract_address) = rl.tokenB
            AND p_b.minute = rl.time
        LEFT JOIN dune.dune.result_dex_prices_solana dp_b 
            ON dp_b.token_mint_address = rl.tokenB 
            AND rl.time = dp_b.day
    )
    
SELECT 
    time,
pool_addresses_2
FROM tvl_all
WHERE time >= CURRENT_DATE - INTERVAL '200' DAY
AND tvl < 1e8
GROUP BY time
ORDER BY time;
