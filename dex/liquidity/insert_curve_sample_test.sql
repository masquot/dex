CREATE OR REPLACE FUNCTION dex.insert_liquidity_curve(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH days AS ( 
    SELECT day FROM generate_series(start_ts, (SELECT end_ts - interval '1 day'), '1 day') g(day)
),
curve_pool_tokens AS (
    SELECT
        *
    FROM
        (
            VALUES
                ('\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'::bytea, '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea),
                ('\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
                ('\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea)
        ) AS t (pool, token)
),
dex_wallet_balances AS (
    SELECT
        balances.wallet_address,
        balances.token_address,
        balances.amount_raw,
        balances.timestamp,
        'token_0' AS token_index
    FROM erc20.token_balances balances
    INNER JOIN curve_pool_tokens c ON balances.token_address = c.token AND balances.wallet_address = c.pool
    -- :todo: INNER JOIN FROM curvefi."mainreg_evt_PoolAdded" dex ON balances.token_address = dex.token AND dex.exchange = balances.wallet_address
    WHERE balances.timestamp >= start_ts AND balances.timestamp < end_ts
    UNION ALL
    SELECT
        pool_address,
        token_address,
        token_amount_raw,
        liq.day,
        token_index
    FROM dex.liquidity liq
    WHERE project = 'Curve' AND version = '1' AND liq.day >= start_ts - interval '3 days'
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
        version, -- :todo:
        category,
        token_amount_raw,
        token_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
        token_address,
        pool_address,
        token_index,
        token_pool_percentage
    FROM (
        -- Curve v1
        SELECT
            d.day,
            'Curve' AS project,
            '1' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            balances.wallet_address AS pool_address,
            balances.token_index,
            0.5 AS token_pool_percentage
        FROM balances
        INNER JOIN days d ON balances.day <= d.day AND d.day < balances.next_day
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

-- :todo: adapt fills
-- Curve v1 contract deployed on '2018-11-02'
-- fill 2018 Q4 + 2019 Q1
SELECT dex.insert_liquidity_curve(
    '2018-11-02',
    '2019-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2018-11-02'
    AND day < '2019-04-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2019 Q2
SELECT dex.insert_liquidity_curve(
    '2019-04-01',
    '2019-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2019-04-01'
    AND day < '2019-07-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2019 Q3
SELECT dex.insert_liquidity_curve(
    '2019-07-01',
    '2019-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2019-07-01'
    AND day < '2019-10-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2019 - Q4
SELECT dex.insert_liquidity_curve(
    '2019-10-01',
    '2020-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2019-10-01'
    AND day < '2020-01-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2020 - Q1
SELECT dex.insert_liquidity_curve(
    '2020-01-01',
    '2020-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-01-01'
    AND day < '2020-04-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2020 - Q2
SELECT dex.insert_liquidity_curve(
    '2020-04-01',
    '2020-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-04-01'
    AND day < '2020-07-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2020 - Q3
SELECT dex.insert_liquidity_curve(
    '2020-07-01',
    '2020-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-07-01'
    AND day < '2020-10-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2020 - Q4
SELECT dex.insert_liquidity_curve(
    '2020-10-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-10-01'
    AND day < '2021-01-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2021 - Q1
SELECT dex.insert_liquidity_curve(
    '2021-01-01',
    '2021-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-01-01'
    AND day < '2021-04-01'
    AND project = 'Curve'
    AND version = '1'
);

-- fill 2021 Q2 + Q3
SELECT dex.insert_liquidity_curve(
    '2021-04-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-04-01'
    AND day < now() - interval '20 minutes'
    AND project = 'Curve'
    AND version = '1'
);

INSERT INTO cron.job (schedule, command)
VALUES ('41 3 * * *', $$
    SELECT dex.insert_liquidity_curve(
        (SELECT max(day) - interval '3 days' FROM dex.liquidity WHERE project = 'Curve' and version = '1'),
        (SELECT now() - interval '20 minutes');
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;