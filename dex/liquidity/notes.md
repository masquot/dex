-- Notes insert_uniswap-v2.sql
-- 1. In this setup, a 2-token liquidity pool will consist of two table entries
-- This setup accomodates multi-asset pools of more than 2 tokens
-- e.g. Balancer pools with up to 8 tokens
--
-- 2. logic from https://github.com/duneanalytics/abstractions/pull/398
--
-- 3. generation of `dex_wallet_balances` and `balances` is costly and almost 'static'
--    except for filter to get active pairs only -> create table and then delete again ?

-- Pickle test
-- Pickle token contract: '\x429881672b9ae42b8eba0e26cd9c73711b891ca5'

