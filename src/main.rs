use ethers::prelude::*;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ---- اتصال به دیتابیس ----
    let db_url = "postgres://mehran:mehran.crypto9@127.0.0.1:5432/pajohesh";
    let pool = sqlx::postgres::PgPool::connect(db_url).await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    println!("Migrations applied successfully!");

    let provider = Arc::new(Provider::<Http>::try_from("https://rpc.ankr.com/eth/a4ce905377a7aa94ded62bf6efb50b20acde76159d163f8de77a16ec6237137b")?);

    // get 300 transactions
    let start_block: u64 = 19000000;
    let mut tx_count = 0;
    let total_txs = 300;

    for block_number in start_block..start_block + 20 {
        if tx_count >= total_txs { break; }

        if let Some(block) = provider.get_block_with_txs(block_number).await? {
            for tx in block.transactions {
                if tx_count >= total_txs { break; }
                process_tx(&provider, &pool, tx, block_number as i64).await?;
                tx_count += 1;
            }
        }
    }

    println!("Done: {} txs fetched", tx_count);
    Ok(())
}

async fn process_tx(provider: &Arc<Provider<Http>>, pool: &Pool<Postgres>, tx: Transaction, block_number: i64) -> anyhow::Result<()> {
    let from = Some(tx.from);
    let to = tx.to;

    if let Some(addr) = from {
        save_wallet(provider, pool, addr, block_number).await?;
    }
    if let Some(addr) = to {
        save_wallet(provider, pool, addr, block_number).await?;
    }

    sqlx::query(
    r#"
    INSERT INTO transactions (hash, block_number, from_addr, to_addr, value, gas, input)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (hash) DO NOTHING
    "#)
    .bind(format!("{:?}", tx.hash))
    .bind(block_number as i64)
    .bind(from.map(|a| format!("{:?}", a)))
    .bind(to.map(|a| format!("{:?}", a)))
    .bind(tx.value.to_string())
    .bind(tx.gas.as_u64() as i64)
    .bind(format!("0x{}", hex::encode(tx.input.as_ref())))
    .execute(pool)
    .await?;

    Ok(())
}

async fn save_wallet(provider: &Arc<Provider<Http>>, pool: &Pool<Postgres>, addr: Address, block_number: i64) -> anyhow::Result<()> {
    let balance = provider.get_balance(addr, None).await?;
    let nonce = provider.get_transaction_count(addr, None).await?;


    sqlx::query(
        r#"
        INSERT INTO wallets (address, balance, nonce, last_seen_block)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (address)
        DO UPDATE SET balance = EXCLUDED.balance, nonce = EXCLUDED.nonce, last_seen_block = EXCLUDED.last_seen_block
        "#)
        .bind(format!("{:?}", addr))
        .bind(balance.to_string())
        .bind(nonce.as_u64() as i64)
        .bind(block_number)
    .execute(pool)
    .await?;

    Ok(())
}
