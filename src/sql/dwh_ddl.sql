CREATE TABLE IF NOT EXISTS STV202504294__DWH.global_metrics (
	date_update date NOT NULL,
	currency_from int NOT NULL,
	cnt_transactions int NOT NULL ,
	amount_total numeric(20, 2) NOT NULL,
	avg_transactions_per_account numeric(10, 3) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL
)
order by date_update
SEGMENTED BY hash(date_update) all nodes
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2);

CREATE TABLE IF NOT EXISTS STV202504294__DWH.currencies (
	date_update date not null, 
    currency_code int not null,
    currency_code_with int not null,
    currency_with_div numeric(5,3) not null
)
ORDER BY date_update
SEGMENTED BY hash(currency_code,date_update) ALL NODES
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2)
;

CREATE TABLE IF NOT EXISTS STV202504294__DWH.transactions (
	operation_id varchar(60) not null,
	account_number_from int not null,
	account_number_to int not null,
	currency_code int not null,
	country varchar(30) not null,
	status varchar(30) not null,
	transaction_type varchar(30) not null,
	amount int not null,
	transaction_dt date not null
)
ORDER BY transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt
GROUP BY calendar_hierarchy_day(transaction_dt, 3, 2)
;