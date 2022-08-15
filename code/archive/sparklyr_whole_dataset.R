rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directiory wherever you save this R script
setwd(wd)

# load packages

# if necessary: install.packages("RSQLite")
library(RSQLite)
library(data.table)
library(tidyverse)
library(recommenderlab)

# initiate the database
ecom_db <- dbConnect(SQLite(), "C:/Users/timma/OneDrive - Handelshögskolan i Stockholm/HSG/Big Data Analytics/project/ecom_db.sqlite")

# purchases per user of all products
cosine_user_product_query <-
  "
SELECT user_id, product_id, COUNT(event_type) AS number_purchases
FROM ecom
WHERE event_type IN ('purchase')
GROUP BY product_id, user_id
ORDER BY product_id ASC;
"

# use query to extract needed data
cosine_user_product_df <- dbGetQuery(ecom_db, cosine_user_product_query)
cosine_user_product_df

# filter for 1000 most popular products
list_product_id <- list(cosine_user_product_df$product_id)
which(table(list_product_id)>20) # viewing all product that are bought at least by 20 customers
list_product_id_popular <- tail(names(sort(table(cosine_user_product_df$product_id))), 1000)
'1005115' %in% list_product_id_popular # check if it worked

# create df with only most popular products
cosine_user_product_clean_df <- filter(cosine_user_product_df, product_id %in% list_product_id_popular)

# spread df, set non-existent values to 0 to use binary encoding
matrix_data <- cosine_user_product_clean_df %>% select(user_id, product_id, number_purchases) %>% spread(product_id, number_purchases, fill=0)
matrix_data

# create binary matrix
ratings_matrix <- matrix_data %>%
                  as.matrix() %>%
                  as("binaryRatingMatrix")

# evaluation method with 80% of data for train and 20% for test
set.seed(1)

evalu <- evaluationScheme(ratings_matrix, method="split", train=0.8, given=1, goodRating=4, k=5)

# prep data
train <- getData(evalu, 'train')# Training Dataset 
dev_test <- getData(evalu, 'known') # Test data from evaluationScheme of type KNOWN
test <- getData(evalu, 'unknown') # Unknow datset used for RMSE / model evaluation

library(sparklyr)
# spark_install(version = 3.1)

# configure spark connection
config <- spark_config()
config$spark.executor.memory <- "8G"
config$spark.executor.cores <- 2
config$spark.executor.instances <- 3
config$spark.dynamicAllocation.enabled <- "false"

# initiate connection
sc <- spark_connect(master = "local", config=config, version = "3.1")

# unhash to verify version: 
# spark_version(sc)

# select data for spark and create spark table 
spark_train <- as(train,"data.frame")
spark_test<- as(test,"data.frame") 

spark_train <- sdf_copy_to(sc, spark_train, "spark_train", overwrite = TRUE) 
spark_test <- sdf_copy_to(sc, spark_test, "spark_test", overwrite = TRUE) 

# Transform features
spark_train <- spark_train %>%
  ft_string_indexer(input_col = "user", output_col = "user_index") %>%
  ft_string_indexer(input_col = "item", output_col = "item_index") %>%
  sdf_register("spark_train")

spark_test <- spark_test %>%
  ft_string_indexer(input_col = "user", output_col = "user_index") %>%
  ft_string_indexer(input_col = "item", output_col = "item_index") %>%
  sdf_register("spark_test")

# build model using user/business/ratings
als_fit <- ml_als(spark_train, 
                  max_iter = 5, 
                  implicit_prefs = TRUE,
                  nonnegative = TRUE, 
                  rank = 2,
                  rating_col = "rating", 
                  user_col = "user_index", 
                  item_col = "item_index")

# predict from the model for the training data
als_predict_train <- ml_predict(als_fit, spark_train) %>% collect()
als_predict_test <- ml_predict(als_fit, spark_test) %>% collect()

# Remove NaN (result of test/train splits - not data)
als_predict_train <- als_predict_train[!is.na(als_predict_train$prediction), ] 
als_predict_test <- als_predict_test[!is.na(als_predict_test$prediction), ]



# Create test set for loop -> exclude item_index 0 because of previous error; only take 1000 most frequent users
spark_test_loop <- filter(spark_test, item_index != 0)
users_spark_test <- spark_test_loop %>% pull(user)
most_frequent_users_test <- tail(names(sort(table(users_spark_test))), 1000)
most_frequent_users_test
spark_test_loop <- filter(spark_test_loop, user %in% most_frequent_users_test)
spark_test_loop

# get all unique user ids and shuffle them
# could also directly pull user_index! but then maybe loss of user informaton (would have to search for it afterwards...)
unique_users_test <- unique(spark_test_loop %>% pull(user))
unique_users_test <- unique_users_test[sample(1:length(unique_users_test))]

# specify top_n highest predictions; number of users to check
top_n = 10
number_users_test = 10
true_positive_list = c()

# loop for number of shuffled users
for (i in 1:number_users_test){
  
  unique_users_test_i <- unique_users_test[i]
  
  # Receive test data for one specific user
  # ERROR: 'user_id' is one element in item...
  # one_user <- filter(spark_test, user == unique_users_test_i)
  one_user <- filter(spark_test_loop, user == unique_users_test_i)[c("rating", "user_index", "item_index")]
  one_user <- copy_to(sc, one_user, overwrite = TRUE)
  one_user
  one_user_index <- one_user %>% pull(user_index)
  one_user_index <- one_user_index[1]
  
  # Get product id for the products which were not bought; leave 1 out!
  list_product_id_popular_missing <- setdiff(1:1000, one_user %>% pull(item_index))
  
  # Create the missing data for not purchased items
  add_data <- tibble(
    "rating" = rep(0, length(list_product_id_popular_missing)),
    "user_index" = rep(one_user_index, length(list_product_id_popular_missing)),
    "item_index" = list_product_id_popular_missing
    )
  add_data <- copy_to(sc, add_data, overwrite = TRUE)
  
  # append the existing test data for the user with data for not purchased items
  one_data_predict <- union_all(one_user, add_data)
  
  # predict for the complete test data for this one user
  one_predict_test <- ml_predict(als_fit, one_data_predict) %>% collect()
  
  # Sort predictions
  # ERROR: item_index 0 is wrong (column name?)
  one_predict_sorted <- one_predict_test %>% 
                          arrange(desc(prediction))
  one_predict_sorted
  
  true_positive_i <- sum((one_predict_sorted %>% pull(rating))[1:top_n]) / top_n
  true_positive_list <- append(true_positive_list, true_positive_i)
}

# vector containing the percentage of true positives for a number of customers
true_positive_list
# average percentage of true positive for a number of customers
mean(true_positive_list)
