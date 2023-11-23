-- Создание таблиц transactions и currencies
DROP TABLE IF EXISTS YASHMAOBLYANDEXRU__STAGING.transactions;
CREATE TABLE YASHMAOBLYANDEXRU__STAGING.transactions (
    operation_id VARCHAR(60),
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(30),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP
);

DROP TABLE IF EXISTS YASHMAOBLYANDEXRU__STAGING.currencies;
CREATE TABLE YASHMAOBLYANDEXRU__STAGING.currencies (
    date_update DATE,
    currency_code VARCHAR(3),
    currency_code_with VARCHAR(3),
    currency_with_div DECIMAL(10, 4)
);

-- Создание проекций по датам для таблиц transactions и currencies
DROP PROJECTION IF EXISTS YASHMAOBLYANDEXRU__STAGING.transactions_date_projection;
CREATE PROJECTION YASHMAOBLYANDEXRU__STAGING.transactions_date_projection (
    transaction_dt,
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount
) AS
SELECT
    transaction_dt,
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount
FROM YASHMAOBLYANDEXRU__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;

DROP PROJECTION IF EXISTS YASHMAOBLYANDEXRU__STAGING.currencies_date_projection;
CREATE PROJECTION YASHMAOBLYANDEXRU__STAGING.currencies_date_projection (
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
) AS
SELECT
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
FROM YASHMAOBLYANDEXRU__STAGING.currencies
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES;
