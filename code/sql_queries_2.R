wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directiory wherever you save this R script
setwd(wd)

# load packages

# if necessary: install.packages("RSQLite")
library(RSQLite)
library(data.table)
library(ggplot2)
library(tidyverse)
library(scales)

# initiate the database
ecom_db <- dbConnect(SQLite(), "../data/ecom_2.sqlite")


### DEFINE QUERIES ###

# information about how many "views" and how many "purchases" there are
event_query <-
  "
  SELECT DISTINCT event_type, COUNT(*) AS counter
  FROM customer
  WHERE event_type IN ('view','purchase')
  GROUP BY event_type;
"

# information about number of users
total_user_count_query <-
  "
  SELECT COUNT(DISTINCT user_id) AS users
  FROM customer;
"

# information about number of users that purchased something
customer_count_query <-
  "
  SELECT COUNT(DISTINCT user_id) AS users
  FROM customer
  WHERE event_type IN ('purchase');
  "

# information about number of unique products purchased for each user
unique_products_per_customer <-
  "
  SELECT user_id, COUNT(DISTINCT product_id) AS number_purchases
  FROM customer
  WHERE event_type IN ('purchase')
  GROUP BY user_id
  ORDER BY COUNT(DISTINCT product_id) DESC;
  "

# information about number of products purchased for each user
products_per_customer <-
  "
  SELECT user_id, COUNT(product_id) AS number_purchases
  FROM customer
  WHERE event_type IN ('purchase')
  GROUP BY user_id
  ORDER BY COUNT(product_id) DESC;
  "

# information about number of categories of the products that have been bought
purchased_categories_query <-
  "
  SELECT COUNT(customer.product_id) AS number_purchases, product.category_code
  FROM customer
  JOIN product ON product.product_id = customer.product_id
  WHERE event_type IN ('purchase')
  GROUP BY category_code
  ORDER BY number_purchases DESC;
  "

# information about number of rows
n_rows <- 
  "
  SELECT COUNT(user_id)
  FROM customer  
  "

# information about total number of purchases
total_purchases_query <-
  "
  SELECT COUNT(product_id) AS mean_purchases
  FROM customer
  WHERE event_type IN ('purchase')
  "


### ISSUE QUERIES ###

# issue event query
event_df <- dbGetQuery(ecom_db, event_query)
event_df
# OUTPUT: purchase: 1,659,788, view: 104,335,509

# issue unique user query
total_user_count_df <- dbGetQuery(ecom_db, total_user_count_query)
total_user_count_df
# OUTPUT: 5,316,649 (October data only: 3,022,290)

# issue unique users who purchased query
customer_count_df <- dbGetQuery(ecom_db, customer_count_query)
customer_count_df
# OUTPUT: 697,470

# issue unique products per customer
unique_products_customer_df <- dbGetQuery(ecom_db, unique_products_per_customer)
unique_products_customer_df

# plot histogram for unique purchases per user
ggp_purchasesUser <- ggplot(unique_products_customer_df, aes(x = number_purchases)) + scale_y_continuous(trans = 'log2') +
    geom_histogram(binwidth = 1, color="darkblue", fill="lightblue") +
    theme_light() +
    ggtitle("Purchases per customer that bought at least one product") +
    xlab("Unique product purchases per user") +
    ylab("Count (log)") 

ggp_purchasesUser + 
    geom_vline(aes(xintercept = median(number_purchases), color='median'), size=1) + 
    geom_vline(aes(xintercept = mean(number_purchases), color='mean'), size=1) + 
    scale_color_manual(name = "statistics", values = c(median = "blue", mean = "red"))

# issue number of categories of the products that have been bought
purchased_categories_df <- dbGetQuery(ecom_db, purchased_categories_query)
purchased_categories_df
purchased_categories_df[c(1:5),]

purchased_categories_df[2, "category_code"] = "Others"
purchased_categories_df[c(1:5),]

# plot bar chart for product categories bought
top_n_categories = 10
ggp_productCategories <- ggplot(purchased_categories_df[c(1:top_n_categories),], aes(x = reorder(category_code, -number_purchases), y=number_purchases)) +
  theme_light() +
  scale_y_continuous(labels = scales::comma) +
  geom_bar(stat="identity", color="darkblue", fill="lightblue") + theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust=0.5)) +
  ggtitle("Purchases per category") +
  xlab("Product category") +
  ylab("Number of purchases")

# issue n_rows query
number_of_rows <- dbGetQuery(ecom_db, n_rows)
number_of_rows
# OUTPUT: 109,950,743

# issue total purchases query
total_purchases_df <- dbGetQuery(ecom_db, total_purchases_query)
total_purchases_df
# OUTPUT: 1,659,788

# find mean number of purchases per user who actually made purchase
total_purchases_df/customer_count_df
# OUTPUT: 2.379727

total_purchases_df

# find mean number of purchases for all users
total_purchases_df/total_user_count_df
# OUTPUT: 0.3121869

# products (not unique) per customer
products_customer_df <- dbGetQuery(ecom_db, products_per_customer)
products_customer_df
sd(products_customer_df$number_purchases)
# OUTPUT: 4.409599

# Disconnect from database
dbDisconnect(ecom_db)
