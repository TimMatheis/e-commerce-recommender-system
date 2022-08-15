rm(list=ls())

library(recommenderlab)
library(tidyverse)
library(sparklyr)
library(kableExtra)

df <- read_csv("~/Library/Mobile Documents/com~apple~CloudDocs/Desktop/Skola/Master/HSG MA/Big_data/group-examination-brownian-boys/data/df_150k_Oct.csv")

df = df[df$event_type == 'purchase',]

df <- df %>%
  mutate(userproduct = paste(user_id, product_id))

df <- df[!duplicated(df$userproduct), ] %>%
  select(-userproduct )

ratings_matrix <- df %>%
  select(user_id, product_id) %>%
  mutate(purchased = 1) %>%
  spread(product_id, purchased, fill=0) %>%
  column_to_rownames("user_id") %>%
  as.matrix() %>%
  as("binaryRatingMatrix")

# evaluation method with 80% of data for train and 20% for test
set.seed(1)

evalu <- evaluationScheme(ratings_matrix,
                          method="split",
                          train=0.8,
                          given=1,
                          goodRating=4,
                          k=5)

# prep data
train <- getData(evalu, 'train')# Training Dataset 
dev_test <- getData(evalu, 'known') # Test data from evaluationScheme of type KNOWN
test <- getData(evalu, 'unknown') # Unknow datset used for RMSE / model evaluation

# configure spark connection
config <- spark_config()
config$spark.executor.memory <- "8G"
config$spark.executor.cores <- 2
config$spark.executor.instances <- 3
config$spark.dynamicAllocation.enabled <- "false"

# initiate connection
sc <- spark_connect(master = "local", config=config, version = "2.4.3")

# unhash to verify version:
spark_version(sc)

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
                  nonnegative = TRUE, 
                  rank = 2,
                  implicit_prefs = TRUE,
                  rating_col = "rating", 
                  user_col = "user_index", 
                  item_col = "item_index")

# predict from the model for the training data
als_predict_train <- ml_predict(als_fit, spark_train) %>% collect()
als_predict_test <- ml_predict(als_fit, spark_test) %>% collect()

# Remove NaN (result of test/train splits - not data)
als_predict_train <- als_predict_train[!is.na(als_predict_train$prediction), ] 
als_predict_test <- als_predict_test[!is.na(als_predict_test$prediction), ]

# View results
als_predict_test %>% head() %>% kable() %>% kable_styling()
