select count(*)
from staging.user_activity_log
where customer_id is null;