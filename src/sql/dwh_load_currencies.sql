MERGE INTO STV202504294__dwh.currencies t
USING (
    select date_update::date, currency_code, currency_code_with, currency_with_div
from (
    select * , row_number() over ( partition by currency_code, currency_code_with, date_update ) as rn from STV202504294__staging.currencies
) a where a.rn = 1 
    AND date_update::date = '{{ ds }}'
    AND date_update IS NOT NULL
    AND currency_code IS NOT NULL
    AND currency_code_with IS NOT NULL
    AND currency_with_div IS NOT NULL
) src
on src.date_update = t.date_update 
   and src.currency_code = t.currency_code
   and src.currency_code_with = t.currency_code_with
   and t.date_update = '{{ ds }}'
WHEN MATCHED THEN UPDATE
SET currency_with_div = src.currency_with_div 
WHEN NOT MATCHED THEN
INSERT (date_update, currency_code, currency_code_with, currency_with_div)
VALUES (src.date_update, src.currency_code, src.currency_code_with, src.currency_with_div);
