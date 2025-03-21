# Solana Indexer 

# Instruction

## Brief description:
- There are no duplicates in the table; all data is unique  
- Uniqueness can be verified using the `transaction_id` key (must be unique across the entire table)  
- The table is partitioned by the `block_date_utc` field  
- Data starts from **2025-02-14**  

## Usage Example:
- Check out `MEVResearch.ipynb` to see how simple you could use our data

## Field descriptions:

| Field                         | Description                                                                                   |
|------------------------------|-----------------------------------------------------------------------------------------------|
| `transaction_id`             | Unique transaction key                                                                        |
| `block_date_utc`             | Transaction date in UTC                                                                       |
| `token_first_trade_dt`       | Date of the first trade in the Raydium pool for the token in UTC                              |
| `block_time_utc`             | Time when the transaction occurred in UTC                                                     |
| `token_first_trade_time_utc` | Time of the first trade in the Raydium pool for the token in UTC                              |
| `block_time_gtm3`            | Time when the transaction occurred in GMT+3                                                   |
| `signature`                  | Transaction signature; if the transaction included multiple interactions with RaydiumV4, this field may not be unique |
| `signing_wallet`             | Wallet that executed the transaction                                                          |
| `coin`                       | Token mint                                                                                    |
| `raydium_market_id`          | ID of the Coin-Solana pair, used in the software                                              |
| `slot`                       | Block number                                                                                 |
| `tx_idx`                     | Transaction index within the block                                                            |
| `direction`                  | `s` for selling the token; `b` for buying the token                                           |
| `coin_amount`                | Amount of token bought/sold, already converted from lamports to a human-readable format       |
| `sol_amount`                 | Amount of SOL bought/sold, already converted from lamports to a human-readable format         |
| `provided_gas_fee`           |                                                                                               |
| `provided_gas_limit`         |                                                                                               |
| `fee`                        | Gas paid to the block leader (validator) for the transaction                                  |
| `consumed_gas`               |                                                                                               |
| `quote_pool_balance_before`  | Token balance in the liquidity pool before the transaction                                    |
| `quote_pool_balance_after`   | Token balance in the liquidity pool after the transaction                                     |
| `base_pool_balance_before`   | SOL balance in the liquidity pool before the transaction                                      |
| `base_pool_balance_after`    | SOL balance in the liquidity pool after the transaction                                       |
| `price_before_deal`          | Token price in SOL before the transaction                                                     |
| `price_after_deal`           | Token price in SOL after the transaction                                                      |
| `slippage_perc`              | How much the price slipped after the trade                                                    |
| `sol_price`                  | SOL price in USDT at the time of the trade                                                    |
| `usdt_amount`                | Trade value in USDT                                                                            |
| `liquidity_before`           | Liquidity before the trade in USDT                                                            |
| `liquidity_after`            | Liquidity after the trade in USDT                                                             |
| `mev_transactio_flg`         | MEV transaction flag                                                                          |
| `mev_victim_flg`             | Flag for transactions that were victims of MEV transactions                                   |
| `mev_type`                   | `b` for MEV scheme buy-buy-sell; `s` for MEV scheme sell-sell-buy; empty if not an MEV trade  |
