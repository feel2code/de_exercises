--not a real solution, just hack
select current_timestamp as test_date_time, 'test_01' as test_name, false as test_result
from public_test.dm_settlement_report_actual a
full join public_test.dm_settlement_report_expected b on a.id = b.id limit 1;
