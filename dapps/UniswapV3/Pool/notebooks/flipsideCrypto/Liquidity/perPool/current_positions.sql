WITH pools as (
  SELECT * FROM uniswapv3.pools
 )

SELECT * FROM (
  SELECT DISTINCT
      liquidity_provider,
      nf_token_id,
      pool_address,
      pool_name,
      last_value(liquidity_adjusted) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as liquidity_adj,

      last_value(tick_upper) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as tick_upper,
      last_value(tick_lower) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as tick_lower,

      last_value(price_lower_0_1_usd) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_lower_0_1_usd,
      last_value(price_upper_0_1_usd) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_upper_0_1_usd,

      last_value(price_lower_1_0_usd) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_lower_1_0_usd,
      last_value(price_upper_1_0_usd) OVER(PARTITION BY liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_upper_1_0_usd
  FROM uniswapv3.positions pos
  WHERE pos.pool_address in (SELECT pool_address
                             FROM pools)
  AND pos.block_timestamp > '2021-05-05 12:00:00'
) allpos
WHERE allpos.liquidity_adj > 0
