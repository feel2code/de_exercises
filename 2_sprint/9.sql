-- Исправьте ошибку в этом скрипте

CREATE TABLE public.d_categories(
   category_id  SERIAL ,
   namecategory TEXT ,
   description  TEXT ,
   PRIMARY KEY  (category_id)
);
CREATE TABLE public.d_products(
   product_id   SERIAL,
   category_id  BIGINT ,
   nameproduct  TEXT ,
   stock        BOOLEAN,
   PRIMARY KEY (product_id),
   FOREIGN KEY (category_id) REFERENCES d_categories(category_id) ON UPDATE CASCADE
);
INSERT INTO public.d_categories
(namecategory, description)
SELECT DISTINCT category[1] AS namecategory ,
                            category[2] AS description
FROM (
      SELECT (regexp_split_to_array(category , E'\\:+')) AS category
      FROM orders_attributes oa) AS parse_category;

INSERT INTO public.d_products
(category_id, nameproduct, stock)
SELECT dc.category_id AS category_id,
        (regexp_split_to_array(oa.description, E'\\:+'))[1] AS nameproduct,
        oa.stock AS stock
FROM (
      SELECT DISTINCT itemcode, description, stock, (regexp_split_to_array(category, E'\\:+'))[1] AS namecategory
      FROM orders_attributes)
AS oa
join d_categories dc on dc.namecategory=oa.namecategory;