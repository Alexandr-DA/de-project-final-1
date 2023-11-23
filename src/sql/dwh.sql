
-- Создание таблицы global_metrics в витрине
CREATE TABLE YASHMAOBLYANDEXRU__DWH.global_metrics (
    date_update DATE,
    currency_from VARCHAR(3),
    amount_total DECIMAL(12, 2),
    cnt_transactions INT,
    avg_transactions_per_account DECIMAL(12, 2),
    cnt_accounts_make_transactions INT
);