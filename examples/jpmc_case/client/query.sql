SELECT COUNT_UNIQUE(asset_name) FROM t_transaction WHERE time < 1677465658
SELECT AVG(price_for_buy) FROM t_transaction WHERE is_buy = TRUE