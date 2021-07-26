-- generate table with results to be used as starting point for update query
WITH interval_1 as ( 
    SELECT day FROM generate_series(timestamp with time zone '2021-07-01', '2021-07-15', '1 day') g(day) -- see https://duneanalytics.com/queries/81692
),
-- :todo: next iteration -> start_date should depend on result returned
interval_2 as ( 
    SELECT day FROM generate_series(timestamp with time zone '2021-07-16', now(), '1 day') g(day) -- see https://duneanalytics.com/queries/81692
),
test_pairs AS (
    SELECT
        *
    FROM
        (
            VALUES
                ('\x111111111117dc0aa78b770fa6a738034120c302'::bytea,'\x4d5f08eccd3d9281632aa3fc6937e98441564544'::bytea), -- 1inch LP 1inch-ETH
                ('\x18aaa7115705e8be94bffebde57af9bfc265b998'::bytea,'\xdc1534a2817bfccd9b6637fb184588e7842805f0'::bytea), -- 1inch LP Audio
                ('\x888888888889c00c67689029d7856aac1065ec11'::bytea,'\x4d5f08eccd3d9281632aa3fc6937e98441564544'::bytea)  -- 1inch LP Audio
        ) AS t (token_address, pool_address)
),
dex_wallet_balances_1 AS (
    SELECT
        balances.wallet_address,
        balances.token_address,
        balances.amount_raw,
        balances.timestamp,
        -- :todo:
        'token_1' AS token_index
    FROM erc20.token_balances balances
    INNER JOIN test_pairs pairs ON balances.token_address = pairs.token_address AND balances.wallet_address = pairs.pool_address
--  INNER JOIN dex_all_versions dex ON (balances.token_address = dex.token1 OR balances.token_address = dex.token2) AND dex.mooniswap = balances.wallet_address
--  WHERE EXISTS (SELECT * FROM onelp."Mooniswap_evt_Swapped" swap WHERE swap.evt_block_time > now() - interval '1week' AND balances.wallet_address = swap.contract_address)
),
balances_1 AS (
    SELECT
        wallet_address,
        token_address,
        token_index,
        amount_raw,
        timestamp, -- :todo: remove
        date_trunc('day', timestamp) as day,
        lead(date_trunc('day', timestamp), 1, now()) OVER (PARTITION BY token_address, wallet_address, token_index ORDER BY timestamp) AS next_day
        FROM dex_wallet_balances_1
),
-- initial results
result_1 AS (
    SELECT
        day,
        erc20.symbol AS token_symbol,
        token_amount_raw / 10 ^ erc20.decimals AS token_amount,
        -- :todo: get pool name using `labels` functionality
        project,
        version,
        category,
        token_amount_raw,
  --      token_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
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
        FROM balances_1 balances
        INNER JOIN interval_1 d ON balances.day <= d.day AND d.day < balances.next_day
    ) dexs
    LEFT JOIN erc20.tokens erc20 on erc20.contract_address = dexs.token_address
),
result_1_latest AS (
    SELECT pool_address, token_address, token_amount_raw, r.day, token_index
    FROM result_1 r
    INNER JOIN (SELECT MAX(day) AS day FROM result_1) r_max ON r.day = r_max.day
),
dex_wallet_balances_2 AS (
    SELECT 
        pool_address as wallet_address,
        token_address,
        token_amount_raw AS amount_raw,
        r.day as timestamp,
        token_index
    FROM result_1 r
    INNER JOIN (SELECT MAX(day) AS day FROM result_1) r_max ON r.day = r_max.day
UNION ALL
    SELECT
        balances.wallet_address,
        balances.token_address,
        balances.amount_raw,
        balances.timestamp,
        -- :todo:
        'token_1' AS token_index
    FROM erc20.token_balances balances
    INNER JOIN test_pairs pairs ON balances.token_address = pairs.token_address AND balances.wallet_address = pairs.pool_address
    WHERE balances.timestamp > (SELECT MAX(day) AS day FROM result_1)
--  INNER JOIN dex_all_versions dex ON (balances.token_address = dex.token1 OR balances.token_address = dex.token2) AND dex.mooniswap = balances.wallet_address
--  WHERE EXISTS (SELECT * FROM onelp."Mooniswap_evt_Swapped" swap WHERE swap.evt_block_time > now() - interval '1week' AND balances.wallet_address = swap.contract_address)
),
balances_2 AS (
    SELECT
        wallet_address,
        token_address,
        token_index,
        amount_raw,
        timestamp, -- :todo: remove
        date_trunc('day', timestamp) as day,
        lead(date_trunc('day', timestamp), 1, now()) OVER (PARTITION BY token_address, wallet_address, token_index ORDER BY timestamp) AS next_day
        FROM dex_wallet_balances_2
),
-- final results
result_2 AS (
    SELECT
        day,
        erc20.symbol AS token_symbol,
        token_amount_raw / 10 ^ erc20.decimals AS token_amount,
        -- :todo: get pool name using `labels` functionality
        project,
        version,
        category,
        token_amount_raw,
  --      token_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
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
        FROM balances_2 balances
        INNER JOIN interval_2 d ON balances.day <= d.day AND d.day < balances.next_day
    ) dexs
    LEFT JOIN erc20.tokens erc20 on erc20.contract_address = dexs.token_address
)

SELECT * FROM result_2 



/*
    WHERE dex.mooniswap IN (
    '\x4d5f08eccd3d9281632aa3fc6937e98441564544',
    '\x09fd2ee3660805b57442f9767e49f5392a95987c',
    '\xf13eef1c6485348b9c9fa0d5df2d89accc5b0147',
    '\x35a0d9579b1e886702375364fe9c540f97e4517b',
    '\x94b0a3d511b6ecdb17ebf877278ab030acb0a878',
    '\x8878df9e1a7c87dcbf6d3999d997f262c05d8c70',
    '\xDc1534A2817BfcCD9b6637FB184588E7842805F0'
    )

===

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
*/
