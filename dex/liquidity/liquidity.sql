CREATE TABLE dex.liquidity (
    day timestamptz NOT NULL,
    token_symbol text,
    token_amount numeric,
    pool_name text,
    project text NOT NULL,
    version text,
    category text,
    token_amount_raw numeric,
    token_usd_amount numeric,
    token_address bytea NOT NULL,
    pool_address bytea NOT NULL,
    token_index text,
    token_pool_percentage numeric
);

-- :todo: indexes
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dex_liquidity_platform_tx_hash_evt_index_uniq_idx ON dex.liquidity (platform, tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS dex_liquidity_block_time_idx ON dex.liquidity USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS dex_liquidity_seller_idx ON dex.liquidity (seller);
CREATE INDEX IF NOT EXISTS dex_liquidity_buyer_idx ON dex.liquidity (buyer);
CREATE INDEX IF NOT EXISTS dex_liquidity_nft_project_name_nft_token_id_block_time_idx ON dex.liquidity (nft_project_name, nft_token_id, block_time)
-- overkill ?
CREATE INDEX IF NOT EXISTS dex_liquidity_block_time_platform_seller_buyer_nft_project_name_nft_token_id_idx ON dex.liquidity (block_time, platform, seller, buyer, nft_project_name, nft_token_id)
