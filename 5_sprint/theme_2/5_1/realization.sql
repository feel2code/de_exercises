select distinct json_array_elements(product_payments::json)::JSON->>'product_name' as product_name from (
SELECT (event_value::JSON->>'product_payments') AS product_payments
FROM outbox
where (event_value::JSON->>'product_payments') is not null) as p;
