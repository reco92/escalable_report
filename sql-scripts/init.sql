DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'transaction_data'
    ) THEN
        CREATE TABLE transaction_data (
            transaction_id TEXT PRIMARY KEY,
            customer_id TEXT,
            card_number TEXT,
            merchant_category TEXT,
            merchant_type TEXT,
            merchant TEXT,
            amount FLOAT,
            currency TEXT,
            country TEXT,
            city TEXT,
            city_size TEXT,
            card_type TEXT,
            card_present BOOLEAN,
            device TEXT,
            channel TEXT,
            device_fingerprint TEXT,
            ip_address TEXT,
            distance_from_home INT,
            high_risk_merchant BOOLEAN,
            transaction_hour INT,
            weekend_transaction BOOLEAN,
            is_fraud BOOLEAN,
            year INT,
            month INT,
            day INT,
            hour INT,
            minute INT,
            rate_usd FLOAT
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_indexes 
        WHERE tablename = 'transaction_data' 
          AND indexname = 'idx_transaction_date'
    ) THEN
        CREATE INDEX idx_transaction_date 
        ON transaction_data (transaction_id, year, month, day);
    END IF;
END $$;