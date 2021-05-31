with prices as (
    SELECT * FROM ethereum.token_prices_hourly
    where hour > '2021-05-04'
    and token_address in (
        select distinct token1_address as address from uniswapv3.pools
        union
        select distinct token0_address as address from uniswapv3.pools
    )
),
liquidity_pool AS (
    SELECT
        tx_id,
        date_trunc('hour', block_timestamp) AS hour,
        action,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        amount0_adjusted,
        amount1_adjusted,
        p0.price AS token0_price,
        p1.price AS token1_price,
        amount0_adjusted * p0.price AS liquidity_usd_amount_0,
        amount1_adjusted * p1.price AS liquidity_usd_amount_1,
        (liquidity_usd_amount_0 + liquidity_usd_amount_1) AS add_amount_total
    FROM uniswapv3.lp_actions lp_actions

    LEFT JOIN prices p0
      ON p0.hour = date_trunc('hour', lp_actions.block_timestamp)
      AND p0.token_address = lp_actions.token0_address

    LEFT JOIN prices p1
      ON p1.hour = date_trunc('hour', lp_actions.block_timestamp)
      AND p1.token_address = lp_actions.token1_address

    WHERE block_timestamp > '2021-05-05 12:00:00'
),
add_liquidity AS (
    SELECT
        hour,
        SUM(add_amount_total) AS total_deposit
    FROM liquidity_pool
    WHERE action = 'INCREASE_LIQUIDITY'
    GROUP BY 1
),
remove_liquidity AS (
    SELECT
        hour,
        -SUM(add_amount_total) AS total_withdrawl
    FROM liquidity_pool
    WHERE action = 'DECREASE_LIQUIDITY'
    GROUP BY 1
)
SELECT
    coalesce(add_liquidity.hour,remove_liquidity.hour) as hour,
    total_deposit as deposits,
    total_withdrawl as withdrawals
FROM add_liquidity

FULL JOIN remove_liquidity
ON add_liquidity.hour = remove_liquidity.hour

WHERE total_deposit IS NOT NULL
ORDER BY 1 DESC
