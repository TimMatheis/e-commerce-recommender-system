rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directiory wherever you save this R script
setwd(wd)

# load packages

# if necessary: install.packages("RSQLite")
library(RSQLite)
library(data.table)

# initiate the database
ecom_db <- dbConnect(SQLite(), "C:/Users/timma/OneDrive - Handelshögskolan i Stockholm/HSG/Big Data Analytics/project/ecom_db.sqlite")

# We already have the tables inside the database; otherwise we could have used the following code:
#   import data into current R sesssion
#   ecom <- fread("C:\Users\timma\OneDrive - HandelshÃ¶gskolan i Stockholm\HSG\Big Data Analytics\project\Oct19")
#   add tables to database
#   dbWriteTable(ecom_db, "ecom", ecom)

# counting the number of event types
event_query <-
  "
SELECT DISTINCT event_type, COUNT(*) AS counter
FROM ecom
WHERE event_type IN ('view','purchase')
GROUP BY event_type;
"

# counting the number of product types
product_query <-
  "
SELECT DISTINCT product_id, COUNT(*) AS counter
FROM ecom
GROUP BY product_id
HAVING COUNT(product_id) > 1000
ORDER BY COUNT(product_id) DESC;
"

# counting the number of unique products
unique_product_query <-
  "
SELECT COUNT(DISTINCT product_id) AS products
FROM ecom;
"

# information for each product
interaction_product_query <-
  "
SELECT DISTINCT product_id, event_type, COUNT(event_type)
FROM ecom
WHERE event_type IN ('view','purchase')
GROUP BY event_type, product_id
ORDER BY product_id ASC;
"

# information number of users
user_count_query <-
  "
SELECT COUNT(DISTINCT user_id) AS users
FROM ecom;
"

# purchases per user of all products
cosine_user_product_query <-
  "
SELECT user_id, product_id, COUNT(event_type) AS number_purchases
FROM ecom
WHERE event_type IN ('purchase')
GROUP BY product_id, user_id
ORDER BY product_id ASC;
"

cosine_user_product_clean <-
  "
SELECT user_id, product_id, COUNT(event_type) AS number_purchases
FROM ecom
WHERE event_type IN ('purchase')
GROUP BY product_id, user_id
ORDER BY product_id ASC;" 


# issue event query
event_df <- dbGetQuery(ecom_db, event_query)
event_df

# issue product query
product_df <- dbGetQuery(ecom_db, product_query)
product_df
# OUTPUT: 166,794

# issue unique product query
unique_product_df <- dbGetQuery(ecom_db, unique_product_query)
unique_product_df

# issue unique product query
interaction_product_df <- dbGetQuery(ecom_db, interaction_product_query)
interaction_product_df

# issue unique user query
user_count_df <- dbGetQuery(ecom_db, user_count_query)
user_count_df
# OUTPUT: 3,022,290