WITH hourly as (
  SELECT
    date_trunc('hour',pstat.block_timestamp) as block_hour,
    pstat.pool_address,
    pool_name,
    avg(tph0.price) as price0,
    avg(tph1.price) as price1,
    avg(VIRTUAL_RESERVES_TOKEN0_ADJUSTED * pow(VIRTUAL_RESERVES_TOKEN1_ADJUSTED,-1)) as native_price0,
    avg(VIRTUAL_RESERVES_TOKEN1_ADJUSTED * pow(VIRTUAL_RESERVES_TOKEN0_ADJUSTED,-1)) as native_price1,
    avg(virtual_liquidity_adjusted) as virtual_liquidity_adjusted,
    avg(virtual_reserves_token1_adjusted) as virtual_reserves_token1_adj,
    avg(virtual_reserves_token0_adjusted) as virtual_reserves_token0_adj,
    avg(token1_balance_adjusted) as gross_reserves_token1_adjusted,
    avg(token0_balance_adjusted) as gross_reserves_token0_adjusted,
    price0 * gross_reserves_token0_adjusted as token0_gross_usd,
    price1 * gross_reserves_token1_adjusted as token1_gross_usd,
    price0 * virtual_reserves_token0_adj as token0_virtual_usd,
    price1 * virtual_reserves_token1_adj as token1_virtual_usd,
    CASE
      WHEN price0 IS NULL and price1 IS NULL THEN 'no prices'
      WHEN price0 IS NULL and price1 IS NOT NULL THEN 'price1'
      WHEN price1 IS NULL and price0 IS NOT NULL THEN 'price0' else 'both prices'
    END AS price_status
  FROM uniswapv3.pool_stats pstat

  JOIN ethereum.token_prices_hourly tph0
    ON tph0.hour = date_trunc('hour',pstat.block_timestamp)
    AND pstat.token0_address = tph0.token_address

  JOIN ethereum.token_prices_hourly tph1
    ON tph1.hour = date_trunc('hour',pstat.block_timestamp)
    AND pstat.token1_address = tph1.token_address

  WHERE pstat.block_timestamp >= current_timestamp - interval '5 hours'
  GROUP BY 1,2,3
--   order by block_hour desc, pstat.pool_address
),

gussied as (
  SELECT
  block_hour, pool_address,pool_name,
  CASE
    WHEN price_status = 'both prices' THEN token0_gross_usd + token1_gross_usd
    WHEN price_status = 'price1' THEN token1_gross_usd + ((gross_reserves_token0_adjusted * native_price1) * price1)
    WHEN price_status = 'price0' THEN token0_gross_usd + ((gross_reserves_token1_adjusted * native_price0) * price0)
    ELSE NULL
  END AS gross_liquidity_usd,
  CASE
    WHEN price_status = 'both prices' THEN sqrt(token0_virtual_usd * token1_virtual_usd)
    WHEN price_status = 'price1' THEN sqrt(token1_virtual_usd * ((virtual_reserves_token0_adj * native_price1) * price1))
    WHEN price_status = 'price0' THEN sqrt(token0_virtual_usd * ((virtual_reserves_token1_adj * native_price0) * price0))
    ELSE NULL
  END AS end as virtual_liquidity_usd
  FROM hourly
)

SELECT *
FROM (
SELECT
  distinct pool_address,
  pool_name,
  last_value(virtual_liquidity_usd) OVER (PARTITION BY pool_address, pool_name ORDER BY block_hour ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as virtual_liquidity_usd,
  last_value(gross_liquidity_usd) OVER (PARTITION BY pool_address, pool_name ORDER BY block_hour ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as real_liquidity_usd
FROM gussied)gus

WHERE gus.real_liquidity_usd > 100000
ORDER BY real_liquidity_usd DESC;
