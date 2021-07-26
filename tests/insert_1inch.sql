CREATE OR REPLACE FUNCTION dex.insert_liquidity_1inch(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH days as ( 
    SELECT day FROM generate_series(timestamp with time zone start_ts, end_ts, '1 day') g(day) -- see https://duneanalytics.com/queries/81692
),
dex_all_versions AS (
    SELECT
        token1,
        token2,
        mooniswap
    FROM onelp."MooniswapFactory_evt_Deployed"
    UNION ALL
    SELECT
        token1,
        token2,
        mooniswap
    FROM onelp."MooniswapFactory_v2_evt_Deployed"
),
dex_wallet_balances AS (
    SELECT
        balances.wallet_address,
        balances.token_address,
        balances.amount_raw,
        balances.timestamp,
        CASE WHEN balances.token_address = dex.token1 THEN 'token_0'
             WHEN balances.token_address = dex.token2 THEN 'token_1'
             ELSE 'token_x'
        END AS token_index
    FROM erc20.token_balances balances
    INNER JOIN dex_all_versions dex ON (balances.token_address = dex.token1 OR balances.token_address = dex.token2) AND dex.mooniswap = balances.wallet_address
-- :todo: needs work   WHERE EXISTS (SELECT * FROM onelp."Mooniswap_evt_Swapped" swap WHERE swap.evt_block_time > now() - interval '1week' AND balances.wallet_address = swap.contract_address)
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
        -- 1inch v1
        SELECT
            d.day,
            '1inch' AS project,
            '1' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            balances.wallet_address AS pool_address,
            balances.token_index,
            0.5 AS token_pool_percentage -- :todo: :research: is this always 0.5 for 1inch ?
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
SELECT dex.insert_liquidity_1inch(
    '2020-01-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day > '2020-01-01'
    AND day <= '2021-01-01'
    AND project = '1inch'
);

-- fill 2021
SELECT dex.insert_liquidity_1inch(
    '2021-01-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day > '2021-01-01'
    AND day <= now() - interval '20 minutes'
    AND project = '1inch'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_liquidity_1inch(
        (SELECT max(day) - interval '1 days' FROM dex.liquidity WHERE project='1inch'),
        (SELECT now() - interval '20 minutes');
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
