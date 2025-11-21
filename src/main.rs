use ethers::prelude::*;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio;
use serde::Deserialize; // آپدیت
use reqwest::Client;
use ethers::types::Address;


#[derive(Deserialize)]
struct EtherscanAbiResult {
    status: String,
    message: String,
    result: String,
}

pub async fn detect_wallet_type_from_etherscan(
    address: Address,
    api_key: &str,
) -> anyhow::Result<String> {

    let client = Client::new();

    let url = format!(
        "https://api.etherscan.io/v2/api?chain=eth&chainid=1&module=contract&action=getabi&address={:?}&apikey={}",
        address, api_key
    );

    let resp = client.get(&url).send().await?;
    let body: EtherscanAbiResult  = resp.json().await?;

    if body.status == "1" && body.message == "OK" {
        return Ok("smart_contract".to_string());
    }

    if body.status == "0" {
        return Ok("wallet".to_string());
    }
    Ok("wallet".to_string())
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = "postgres://mehran:mehran.crypto9@127.0.0.1:5432/pajohesh";
    let pool = sqlx::postgres::PgPool::connect(db_url).await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    println!("Migrations applied successfully!");

    let provider = Arc::new(
        Provider::<Http>::try_from(
            "https://rpc.ankr.com/eth/a4ce905377a7aa94ded62bf6efb50b20acde76159d163f8de77a16ec6237137b",
        )?
    );

    let start_block: u64 = 19000000;
    let mut tx_count = 0;
    let total_txs = 300;

    for block_number in start_block..start_block + 20 {
        if tx_count >= total_txs {
            break;
        }

        if let Some(block) = provider.get_block_with_txs(block_number).await? {
            for tx in block.transactions {
                if tx_count >= total_txs {
                    break;
                }

                process_tx(&provider, &pool, tx, block_number as i64).await?;
                tx_count += 1;
            }
        }
    }

    println!("Done: {} txs fetched", tx_count);
    Ok(())
}


async fn process_tx(
    provider: &Arc<Provider<Http>>,
    pool: &Pool<Postgres>,
    tx: Transaction,
    block_number: i64,
) -> anyhow::Result<()> {
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
        "#,
    )
    .bind(format!("{:?}", tx.hash))
    .bind(block_number)
    .bind(from.map(|a| format!("{:?}", a)))
    .bind(to.map(|a| format!("{:?}", a)))
    .bind(tx.value.to_string())
    .bind(tx.gas.as_u64() as i64)
    .bind(format!("0x{}", hex::encode(tx.input.as_ref())))
    .execute(pool)
    .await?;

    Ok(())
}

async fn save_wallet(
    provider: &Arc<Provider<Http>>,
    pool: &Pool<Postgres>,
    addr: Address,
    block_number: i64,
) -> anyhow::Result<()> {

    let balance = provider.get_balance(addr, None).await?;
    let nonce = provider.get_transaction_count(addr, None).await?;
    
    let api_key = "DWYGKM65G8A7HHE4J497BWF9TK3R4H9NGC";
    let wallet_type = detect_wallet_type_from_etherscan(addr, api_key).await?;

    sqlx::query(
        r#"
        INSERT INTO wallets (address, balance, nonce, last_seen_block, type, defi, sensitive)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (address)
        DO UPDATE SET 
            balance = EXCLUDED.balance,
            nonce = EXCLUDED.nonce,
            last_seen_block = EXCLUDED.last_seen_block,
            type = EXCLUDED.type
        "#,
    )
    .bind(format!("{:#x}", addr))
    .bind(balance.to_string())
    .bind(nonce.as_u64() as i64)
    .bind(block_number)
    .bind(wallet_type)          
    .bind("")                
    .bind("")                
    .execute(pool)
    .await?;

    Ok(())
}
