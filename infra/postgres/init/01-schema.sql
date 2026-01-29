-- Transactions table for PaySim fraud detection data
-- This script runs automatically when PostgreSQL container starts

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    step INTEGER NOT NULL,
    type VARCHAR(20) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    name_orig VARCHAR(50) NOT NULL,
    old_balance_orig DECIMAL(15,2) NOT NULL,
    new_balance_orig DECIMAL(15,2) NOT NULL,
    name_dest VARCHAR(50) NOT NULL,
    old_balance_dest DECIMAL(15,2) NOT NULL,
    new_balance_dest DECIMAL(15,2) NOT NULL,
    is_fraud INTEGER NOT NULL DEFAULT 0,
    is_flagged_fraud INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fraud detection queries
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);

-- Publication for Debezium CDC
ALTER TABLE transactions REPLICA IDENTITY FULL;
