MERGE INTO STV202504294__dwh.transactions t
USING (
    select operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt::date 
    from (
        select * , row_number() over ( partition by operation_id, transaction_dt ) as rn from STV202504294__staging.transactions
    ) a where a.rn = 1 
            AND transaction_dt::date = '{{ ds }}'
            AND operation_id IS NOT NULL
            AND account_number_from IS NOT NULL
            AND account_number_to IS NOT NULL
            AND currency_code IS NOT NULL
            AND country IS NOT NULL
            AND status IS NOT NULL
            AND transaction_type IS NOT NULL
            AND amount IS NOT NULL
            AND transaction_dt IS NOT NULL
) src
on src.operation_id = t.operation_id 
 and src.transaction_dt = t.transaction_dt
 and t.transaction_dt = '{{ ds }}'
WHEN MATCHED THEN UPDATE
SET account_number_from = src.account_number_from,
	account_number_to = src.account_number_to,
	currency_code = src.currency_code,
	country = src.country,
	status = src.status,
	transaction_type = src.transaction_type,
	amount = src.amount
	
WHEN NOT MATCHED THEN
INSERT (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
VALUES (src.operation_id, src.account_number_from, src.account_number_to, src.currency_code, src.country, src.status, src.transaction_type, src.amount, src.transaction_dt);