WITH inflows as (
    SELECT
        date_trunc('day',block_timestamp) as date,
        to_label as exchange,
        sum(amount_usd) as deposits_usd
    FROM ethereum.udm_events
    WHERE
        block_timestamp >= getdate() - interval '9 days'
        and
        ((
            to_label_type = 'cex'
            and (from_label_type <> 'cex' OR from_label_type IS NULL)
        ) OR (
            to_label_type = 'dex'
            and (from_label_type <> 'dex' OR from_label_type IS NULL)
        ))
        and contract_address = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
    GROUP BY 1,2
),

outflows as (
    SELECT
        date_trunc('day',block_timestamp) as date,
        from_label as exchange,
        sum(amount_usd) as withdrawals_usd
    FROM ethereum.udm_events
    WHERE
        block_timestamp >= getdate() - interval '9 days'
        and
        ((
            from_label_type = 'cex'
            and (to_label_type <> 'cex' OR to_label_type IS NULL)
        ) OR (
            from_label_type = 'dex'
            and (to_label_type <> 'dex' OR to_label_type IS NULL)
        ))
        and contract_address = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
    GROUP BY 1,2
),

-- subset to exchanges with at least some meaningful activity
legits as (
    select legs.exchange
    from (
        select
          exchange,
          sum(deposits_usd) as deps
        from inflows
        group by 1)legs
    where deps > 5000000
)

SELECT
    coalesce(inflows.date, outflows.date) as date,
    coalesce(inflows.exchange, outflows.exchange) as exchange,
    inflows.deposits_usd as deposits_to_exchanges,
    outflows.withdrawals_usd as withdrawals_from_exchanges,
    withdrawals_from_exchanges - deposits_to_exchanges as net_flow_to_ethereum
FROM inflows

FULL JOIN outflows
  ON inflows.date = outflows.date
  AND inflows.exchange = outflows.exchange

WHERE inflows.exchange IN (select exchange from legits)
  OR outflows.exchange IN (select exchange from legits)
ORDER BY 1 DESC, 3 DESC;
