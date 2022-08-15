# you first have to download the two csv files from https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
# save them in the 'data' folder and name them 'Oct19.csv' and 'Nov19.csv'

# reset the environment
rm(list=ls())

# set the working directory wherever you save this R script
wd <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(wd)

# load packages
library("tidyverse")
library("dplyr")
library("readr")
library("data.table")


# specify the needed attributes for each of the db tables
customer_columns = c("event_time", "event_type", "product_id", "user_id", "user_session")
product_columns = c("product_id", "category_id", "category_code", "brand", "price")

# load October customer data
data_customer_Oct = fread("../data/Oct19.csv", 
                          header=T, 
                          select = customer_columns)

# save data for the Oct customer table
write.csv(data_customer_Oct, "../data/data_customer_Oct.csv", row.names = FALSE)
rm(data_customer_Oct)

# load November customer data
data_customer_Nov = fread("../data/Nov19.csv", 
                          header=T, 
                          select = customer_columns)

# save data for the Nov customer table
write.csv(data_customer_Nov, "../data/data_customer_Nov.csv", row.names = FALSE)
rm(data_customer_Nov)

# load data for the Oct product table
data_product_Oct = fread("../data/Oct19.csv", 
                          header=T,
                          data.table = FALSE,
                          select = product_columns)

# group and summarize Oct product data
data_product_Oct = data_product_Oct %>%
  group_by(product_id) %>%
  summarise( category_id = first(category_id), category_code = first(category_code), brand = first(brand),  price = first(price) )

# load data for the Nov product table
data_product_Nov = fread("../data/Nov19.csv", 
                         header=T,
                         data.table = FALSE,
                         select = product_columns)

# group and summarize Nov product data
data_product_Nov = data_product_Nov %>%
  group_by(product_id) %>%
  summarise( category_id = first(category_id), category_code = first(category_code), brand = first(brand),  price = first(price) )

# append Oct product data with Nov data and remove redundant entries
data_product_OctNov <- rbind(data_product_Oct, data_product_Nov) %>%
                          group_by(product_id) %>%
                          summarise( category_id = first(category_id), category_code = first(category_code), brand = first(brand),  price = first(price) )

# save data for OctNov product table
write.csv(data_product_OctNov, "../data/data_product_OctNov.csv", row.names = FALSE)
rm(data_product_Oct)
rm(data_product_Nov)
rm(data_product_OctNov)