-- This query shows total liquidity being deposited and withdrawn from all Uniswap V3 Pools each hour
WITH liquidity_pool AS (
    SELECT
        tx_id,
        date_trunc('hour', block_timestamp) AS block_hour,
        action,
        pool_address,
        pool_name,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        amount0_adjusted,
        amount1_adjusted,
        amount0_usd,
        amount1_usd,
        (amount0_usd + amount1_usd) AS add_amount_total
    FROM uniswapv3.lp_actions
),
add_liquidity AS (
    SELECT
        block_hour,
        SUM(add_amount_total) AS total_deposit
    FROM liquidity_pool
    WHERE action = 'INCREASE_LIQUIDITY'
    GROUP BY 1
),
remove_liquidity AS (
    SELECT
        block_hour,
        -SUM(add_amount_total) AS total_withdrawl
    FROM liquidity_pool
    WHERE action = 'DECREASE_LIQUIDITY'
    GROUP BY 1
)

SELECT
    coalesce(add_liquidity.block_hour, remove_liquidity.block_hour) AS block_hour,
    total_deposit AS total_deposits_usd,
    total_withdrawl AS total_withdrawls_usd
FROM add_liquidity

LEFT JOIN remove_liquidity
  ON add_liquidity.block_hour = remove_liquidity.block_hour

WHERE (total_deposit + total_withdrawl) IS NOT NULL
ORDER BY 1 DESC
