WITH swap_raw AS (
    SELECT pool_address,
        pool_name,
        CASE
            WHEN amount0_usd < 0 THEN amount0_usd
            ELSE amount1_usd
        END AS swap_usd_amount_out,
        CASE
            WHEN amount0_usd >= 0 THEN amount0_usd
            ELSE amount1_usd
        END AS swap_usd_amount_in
    FROM uniswapv3.swaps
    WHERE amount0_usd IS NOT NULL
        AND amount1_usd IS NOT NULL
) -- // Only amount in
SELECT pool_address,
    pool_name,
    SUM(ABS(swap_usd_amount_in)) AS swap_volume_usd
FROM swap_raw
GROUP BY 1,
    2
ORDER BY 3 DESC
LIMIT 10000