CREATE OR REPLACE FUNCTION dex.insert_liquidity_uniswap_v2(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH days as ( 
    SELECT day FROM generate_series(start_ts, (SELECT end_ts - interval '1 day'), '1 day') g(day)
),
dex_wallet_balances AS (
    SELECT
        balances.wallet_address,
        balances.token_address,
        balances.amount_raw,
        balances.timestamp,
        CASE WHEN balances.token_address = dex.token0 THEN 'token_0'
             WHEN balances.token_address = dex.token1 THEN 'token_1'
             ELSE 'token_x'
        END AS token_index
    FROM erc20.token_balances balances
    INNER JOIN uniswap_v2."Factory_evt_PairCreated" dex ON (balances.token_address = dex.token0 OR balances.token_address = dex.token1) AND dex.pair = balances.wallet_address
    WHERE balances.timestamp >= start_ts AND balances.timestamp < end_ts
    UNION ALL
    -- get the latest entries from `dex.liquidity` as a starting point to avoid expensive recalculations
    -- going back 3 extra days in time to be on the safe side if previous day was not correctly updated
    SELECT
        pool_address,
        token_address,
        token_amount_raw,
        liq.day,
        token_index
    FROM dex.liquidity liq
    WHERE project = 'Uniswap' AND version = '2' AND liq.day >= start_ts - interval '3 days'
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
        -- :todo: get pool name using `labels` functionality
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
        -- Uniswap v2
        SELECT
            d.day,
            'Uniswap' AS project,
            '2' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            balances.wallet_address AS pool_address,
            balances.token_index,
            0.5 AS token_pool_percentage -- :todo: :research: is this always 0.5 for uniswap_v2 ?
        FROM balances
        INNER JOIN days d ON balances.day <= d.day AND d.day < balances.next_day
        WHERE balances.wallet_address NOT IN (
            '\xed9c854cb02de75ce4c9bba992828d6cb7fd5c71', -- remove WETH-UBOMB wash trading pair
            '\x854373387e41371ac6e307a1f29603c6fa10d872' ) -- remove FEG/ETH token pair
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

-- Uniswap V2 contract deployed on '2020-05-04'
-- fill 2020 - Q2 + Q3
SELECT dex.insert_liquidity_uniswap_v2(
    '2020-05-04',
    '2020-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-05-04'
    AND day < '2020-10-01'
    AND project = 'Uniswap'
    AND version = '2'
);

-- fill 2020 - Q4
SELECT dex.insert_liquidity_uniswap_v2(
    '2020-10-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-10-01'
    AND day < '2021-01-01'
    AND project = 'Uniswap'
    AND version = '2'
);

-- fill 2021 - Q1
SELECT dex.insert_liquidity_uniswap_v2(
    '2021-01-01',
    '2021-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-01-01'
    AND day < '2021-04-01'
    AND project = 'Uniswap'
    AND version = '2'
);

-- fill 2021 Q2 + Q3
SELECT dex.insert_liquidity_uniswap_v2(
    '2021-04-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-04-01'
    AND day < now() - interval '20 minutes'
    AND project = 'Uniswap'
    AND version = '2'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$ -- :todo:
    SELECT dex.insert_liquidity_uniswap_v2(
        (SELECT max(day) - interval '3 days' FROM dex.liquidity WHERE project = 'Uniswap' and version = '2'),
        (SELECT now() - interval '20 minutes');
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
