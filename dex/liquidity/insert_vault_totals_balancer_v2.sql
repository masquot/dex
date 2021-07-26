CREATE OR REPLACE FUNCTION dex.insert_liquidity_balancer_v2(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH days as ( 
    SELECT day FROM generate_series(start_ts, (SELECT end_ts - interval '1 day'), '1 day') g(day)
),
balancer_v2_pools as ( -- https://github.com/duneanalytics/abstractions/blob/master/labels/ethereum/balancer_v2_pools.sql
    SELECT
        pool_id,
        SUBSTRING(pool_id FOR 20) as pool_address,
        token_address,
        normalized_weight,
        symbol as pool_name_symbolic
    FROM
    (
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP' as pool_type
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."WeightedPoolFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
    union all
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP2T' as pool_type
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."WeightedPool2TokensFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
    union all
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, 0 as normalized_weight, cc.symbol, 'SP' as pool_type
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."StablePoolFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
    ) all_pools
),
dex_wallet_balances AS (
    SELECT
        balances.wallet_address,
        balances.token_address,
        balances.amount_raw,
        balances.timestamp,
        'token_x' AS token_index
    FROM erc20.token_balances balances
    WHERE
        balances.wallet_address = '\xba12222222228d8ba445958a75a0704d566bf2c8' -- https://docs.balancer.fi/getting-started/faqs/the-vault
        AND
        balances.timestamp >= start_ts AND balances.timestamp < end_ts
        AND
        EXISTS (SELECT * FROM balancer_v2_pools p WHERE balances.token_address = p.token_address) 
    UNION ALL
    SELECT
        pool_address,
        token_address,
        token_amount_raw,
        liq.day,
        token_index
    FROM dex.liquidity liq
    WHERE project = 'Balancer' AND version = '2' AND liq.day >= start_ts - interval '3 days'
),
balances AS (
    SELECT
        wallet_address,
        token_address,
        token_index,
        amount_raw,
        date_trunc('day', timestamp) as day,
        lead(date_trunc('day', timestamp), 1, now()) OVER (PARTITION BY token_address, wallet_address, token_index ORDER BY timestamp) AS next_day
        FROM dex_wallet_balances
),
rows AS (
    INSERT INTO dex.liquidity (
        day,
        token_symbol,
        token_amount,
        pool_name,
        project,
        version,
        category,
        token_amount_raw,
        token_usd_amount,
        token_address,
        pool_address,
        token_index,
        token_pool_percentage
    )
    SELECT
        day,
        erc20.symbol AS token_symbol,
        token_amount_raw / 10 ^ erc20.decimals AS token_amount,
        labels.get(pool_address, 'lp_pool_name'),
        project,
        version,
        category,
        token_amount_raw,
        token_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
        token_address,
        pool_address,
        token_index,
        token_pool_percentage
    FROM (
        -- Balancer v2
        SELECT
            d.day,
            CONCAT('Balancer v2 LP ', pools.pool_name_symbolic),
            'Balancer' AS project,
            '2' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            balances.wallet_address AS pool_address,
            balances.token_index,
            pools.normalized_weight AS token_pool_percentage
        FROM balances
        INNER JOIN days d ON balances.day <= d.day AND d.day < balances.next_day
        LEFT JOIN balancer_v2_pools pools ON balances.wallet_address = pools.pool_address AND balances.token_address = pools.token_address
    ) dexs
    LEFT JOIN erc20.tokens erc20 on erc20.contract_address = dexs.token_address
    LEFT JOIN prices.usd p on p.contract_address = dexs.token_address and p.minute = dexs.day

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Balancer v2 contract deployed on '2018-11-02'
-- fill 2018 Q4 + 2019 Q1
SELECT dex.insert_liquidity_balancer_v2(
    '2018-11-02',
    '2019-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2018-11-02'
    AND day < '2019-04-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2019 Q2
SELECT dex.insert_liquidity_balancer_v2(
    '2019-04-01',
    '2019-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2019-04-01'
    AND day < '2019-07-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2019 Q3
SELECT dex.insert_liquidity_balancer_v2(
    '2019-07-01',
    '2019-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2019-07-01'
    AND day < '2019-10-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2019 - Q4
SELECT dex.insert_liquidity_balancer_v2(
    '2019-10-01',
    '2020-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2019-10-01'
    AND day < '2020-01-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2020 - Q1
SELECT dex.insert_liquidity_balancer_v2(
    '2020-01-01',
    '2020-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-01-01'
    AND day < '2020-04-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2020 - Q2
SELECT dex.insert_liquidity_balancer_v2(
    '2020-04-01',
    '2020-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-04-01'
    AND day < '2020-07-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2020 - Q3
SELECT dex.insert_liquidity_balancer_v2(
    '2020-07-01',
    '2020-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-07-01'
    AND day < '2020-10-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2020 - Q4
SELECT dex.insert_liquidity_balancer_v2(
    '2020-10-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-10-01'
    AND day < '2021-01-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2021 - Q1
SELECT dex.insert_liquidity_balancer_v2(
    '2021-01-01',
    '2021-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-01-01'
    AND day < '2021-04-01'
    AND project = 'Balancer'
    AND version = '2'
);

-- fill 2021 Q2 + Q3
SELECT dex.insert_liquidity_balancer_v2(
    '2021-04-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-04-01'
    AND day < now() - interval '20 minutes'
    AND project = 'Balancer'
    AND version = '2'
);

INSERT INTO cron.job (schedule, command)
VALUES ('41 3 * * *', $$
    SELECT dex.insert_liquidity_balancer_v2(
        (SELECT max(day) - interval '3 days' FROM dex.liquidity WHERE project = 'Balancer' and version = '2'),
        (SELECT now() - interval '20 minutes');
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
