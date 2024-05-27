select count(*)
from staging.user_orger_log
where customer_id is null;