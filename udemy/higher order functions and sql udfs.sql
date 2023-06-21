-- Databricks notebook source
-- MAGIC %run  ../Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select 
 order_id,multiple_copies
 from
 (select order_id, filter(books, i -> i.quantity >=2) as multiple_copies
from orders)

where size(multiple_copies) >0;
 

-- COMMAND ----------

select 
  order_id,
  books,
  transform(
    books,
    b -> cast(b.subtotal * 0.8 as INT)
  ) as subtotal_after_discount
from orders;

-- COMMAND ----------

create or replace function get_url(email string)
returns string

return concat("https://www.",split(email,"@")[1])

-- COMMAND ----------

select email, get_url(email) domain
from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email string)
returns string
return case
          when email like "%.com" then "commercial business"
          when email like "%.org" then "non-pofit org"
          when email like "%.edu" then "educational institution"
          else concat("unknown extention for domain: ",split(email,"@")[1])
       end;
        

-- COMMAND ----------

select email, site_type(email) as domain_category
from customers

-- COMMAND ----------

DESCRIBE FUNCTION extended site_type

-- COMMAND ----------

