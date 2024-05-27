SELECT utm_campaign AS campaign, AVG(oa.payment_amount) AS avg_payment
FROM user_attributes ua JOIN (SELECT client_id, payment_amount FROM orders_attributes WHERE payment_amount < 1000
) AS oa ON ua.client_id = oa.client_id
GROUP BY utm_campaign;

SELECT ua.utm_campaign AS campaign, AVG(do2.payment) as avg_payment
FROM d_products dp
JOIN d_buckets db ON db.product_id = dp.product_id
JOIN ( SELECT * FROM d_orders WHERE payment < 1000
) AS do2 ON do2.order_id = db.order_id
JOIN user_attributes ua ON ua.client_id = do2.client_id
GROUP BY ua.utm_campaign;


-- Исправьте ошибки в этом скрипте

-- Создание таблиц
CREATE TABLE public.d_categories(
   category_id  SERIAL ,
   name_category TEXT ,
   description  TEXT ,
   PRIMARY KEY  (category_id)
);
CREATE TABLE public.d_products(
   product_id   SERIAL,
   category_id  BIGINT ,
   name_product  TEXT ,
   stock        BOOLEAN,
   PRIMARY KEY (product_id),
   FOREIGN KEY (category_id) REFERENCES d_categories(category_id) ON UPDATE CASCADE
);
CREATE TABLE public.d_buckets(
   bucket_id    SERIAL ,
   order_id     BIGINT ,
   product_id   BIGINT ,
   num          NUMERIC(14,0),
   PRIMARY KEY (bucket_id),
   FOREIGN KEY (product_id) REFERENCES d_products(product_id) ON UPDATE CASCADE
);

CREATE TABLE public.d_orders(
   order_id     BIGINT ,
   client_id    BIGINT,
   payment      NUMERIC(14,2),
   hit_date_time  TIMESTAMP,
   PRIMARY KEY (order_id),
   FOREIGN KEY (client_id) REFERENCES user_attributes(client_id) ON UPDATE CASCADE
);

-- Миграция данных

INSERT INTO public.d_categories
(name_category, description)
SELECT DISTINCT category[1] AS name_category ,
                            category[2] AS description
FROM (
      SELECT (regexp_split_to_array(category , E'\\:+')) AS category
      FROM orders_attributes oa) AS parse_category;

INSERT INTO public.d_products
(product_id, category_id, name_product, stock)
SELECT DISTINCT oa.itemcode as product_id,
       dc.category_id AS category_id,
       (regexp_split_to_array(oa.description, E'\\:+'))[1] AS name_product,
       oa.stock AS stock
FROM (
      SELECT *, (regexp_split_to_array(category, E'\\:+'))[1] AS name_category
      FROM orders_attributes
    ) as oa
RIGHT JOIN d_categories dc ON oa.name_category = dc.name_category;

INSERT INTO public.d_buckets
(product_id, order_id, num)
SELECT
    itemcode as product_id,
    order_id as order_id,
    num
FROM orders_attributes;

INSERT INTO public.d_orders
(order_id, client_id, payment, hit_date_time)
SELECT distinct
  oa.order_id AS order_id,
  oa.client_id AS client_id,
  oa.payment_amount,
  datetime AS hit_date_time
FROM orders_attributes oa;