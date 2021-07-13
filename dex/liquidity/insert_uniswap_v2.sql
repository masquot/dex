-- Pickle test
-- Pickle token contract: '\x429881672b9ae42b8eba0e26cd9c73711b891ca5'

-- Notes:
-- In this setup, a 2-token liquidity pool will consist of two table entries
-- This setup accomodates multi-asset pools of more than 2 tokens
-- e.g. Balancer pools with up to 8 tokens
SELECT
    balances.day,
    erc20.symbol AS token_symbol,
    balances.amount AS token_amount,
    -- :todo: get pool name using `labels` functionality
    'Uniswap' AS project,
    '2' AS version,
    'DEX' AS category,
    balances.amount_raw AS token_amount_raw,
    balances.amount_usd AS token_usd_amount,
    erc20.contract_address AS token_contract_address,
    dex.pair AS pool_address,
    CASE WHEN erc20.contract_address = dex.token0 THEN 'token_0'
         WHEN erc20.contract_address = dex.token1 THEN 'token_1'
         ELSE 'token_x' 
    END AS token_index
    -- :include?: token_pool_percentage :todo: :research: is this always 0.5 for uniswap_v2 ?
FROM erc20.tokens erc20
LEFT JOIN uniswap_v2."Factory_evt_PairCreated" dex ON (erc20.contract_address = dex.token0 OR erc20.contract_address = dex.token1) -- INNER JOIN
LEFT JOIN erc20."view_token_balances_daily" balances ON erc20.contract_address = balances.token_address AND dex.pair = balances.wallet_address
WHERE balances.day > now() - interval '1week'
-- filter out pools without token swaps in the last week
AND EXISTS (SELECT evt_tx_hash FROM uniswap_v2."Pair_evt_Swap" swap WHERE swap.evt_block_time > now() - interval '1week' AND dex.pair = swap.contract_address) 
-- PICKLE token for testing
AND erc20.contract_address = '\x429881672b9ae42b8eba0e26cd9c73711b891ca5'

-- TESTS
-- WHERE erc20.contract_address = '\x429881672b9ae42b8eba0e26cd9c73711b891ca5'  -- PICKLE TOKEN runs for 17 seconds
-- WHERE erc20.contract_address = '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984'  -- UNI TOKEN runs for 1 minute
-- WHERE erc20.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'  -- WETH TOKEN times out; 2 minutes 282K rows
