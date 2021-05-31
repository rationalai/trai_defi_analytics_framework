WITH hourly as (
SELECT
  DISTINCT date_trunc('hour',pstat.block_timestamp) as block_hour,
  pstat.pool_address,
  pool_name,

  last_value(price_0_1) OVER (partition by block_hour, pool_address, pool_name order by block_timestamp) as price01,
  last_value(price_1_0) OVER (partition by block_hour, pool_address, pool_name order by block_timestamp) as price10,

  last_value(tph0.price) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as price0,
  last_value(tph1.price) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as  price1,

  last_value(token0_balance_adjusted) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as gross_reserves_token0_adjusted,
  last_value(token1_balance_adjusted) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as gross_reserves_token1_adjusted,

  price0 * gross_reserves_token0_adjusted as token0_gross_usd,
  price1 * gross_reserves_token1_adjusted as token1_gross_usd,

  CASE WHEN price0 IS NULL and price1 IS NULL THEN 'no prices'
       WHEN price0 IS NULL and price1 IS NOT NULL THEN 'price1'
       WHEN price1 IS NULL and price0 IS NOT NULL THEN 'price0'
       ELSE 'both prices'
  END AS price_status
  FROM uniswapv3.pool_stats pstat

  LEFT JOIN ethereum.token_prices_hourly tph0
    ON tph0.hour = date_trunc('hour',pstat.block_timestamp)
    AND pstat.token0_address = tph0.token_address

  LEFT JOIN ethereum.token_prices_hourly tph1
    ON tph1.hour = date_trunc('hour',pstat.block_timestamp)
    AND pstat.token1_address = tph1.token_address

WHERE pstat.block_timestamp >= getdate() - interval '7 days'
ORDER BY block_hour DESC, pstat.pool_address
),

gussied as (
  SELECT
    block_hour,
    pool_address,
    pool_name,
    price_status,
    CASE
      WHEN price_status = 'both prices' THEN token0_gross_usd + token1_gross_usd
      when price_status = 'price1' then token1_gross_usd + ((gross_reserves_token0_adjusted * price10) * price1)
      when price_status = 'price0' then token0_gross_usd + ((gross_reserves_token1_adjusted * price01) * price0)
      ELSE NULL
     END AS tvl_usd
  FROM hourly
  WHERE price_status <> 'no prices'
)
SELECT
  block_hour,
  count(1) as n_pools,
  sum(tvl_usd) as total_liquidity_usd
FROM gussied
WHERE tvl_usd <> 'NaN'
  AND tvl_usd < 250000000
GROUP BY 1
ORDER BY 1 DESC;
