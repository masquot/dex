CREATE OR REPLACE FUNCTION dex.insert_liquidity_sushi(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH days as ( 
    SELECT day FROM generate_series(timestamp with time zone start_ts, end_ts, '1 day') g(day) -- see https://duneanalytics.com/queries/81692
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
    INNER JOIN sushi."Factory_evt_PairCreated" dex ON (balances.token_address = dex.token0 OR balances.token_address = dex.token1) AND dex.pair = balances.wallet_address
    WHERE EXISTS (SELECT * FROM sushi."Pair_evt_Swap" swap WHERE swap.evt_block_time > now() - interval '1week' AND balances.wallet_address = swap.contract_address)
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
        -- Sushi v1
        SELECT
            d.day,
            'Sushiswap' AS project,
            '1' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            balances.wallet_address AS pool_address,
            balances.token_index,
            0.5 AS token_pool_percentage -- :todo: :research: is this always 0.5 for sushi ?
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

-- fill 2020
SELECT dex.insert_liquidity_sushi(
    '2020-01-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day > '2020-01-01'
    AND day <= '2021-01-01'
    AND project = 'Sushiswap'
);

-- fill 2021
SELECT dex.insert_liquidity_sushi(
    '2021-01-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day > '2021-01-01'
    AND day <= now() - interval '20 minutes'
    AND project = 'Sushiswap'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_liquidity_sushi(
        (SELECT max(day) - interval '1 days' FROM dex.liquidity WHERE project='Sushiswap'),
        (SELECT now() - interval '20 minutes');
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
