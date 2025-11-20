CREATE TABLE wallets (
    address TEXT PRIMARY KEY,
    balance TEXT NOT NULL,
    nonce BIGINT NOT NULL,
    last_seen_block BIGINT
);

CREATE TABLE transactions (
    hash TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    from_addr TEXT NOT NULL,
    to_addr TEXT,
    value TEXT NOT NULL,
    gas BIGINT NOT NULL,
    input TEXT
);
