rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directory wherever you save this R script
setwd(wd)

# load packages

# if necessary: install.packages("RSQLite")
library(RSQLite)
library(data.table)
library(tidyverse)
library(sparklyr)
library(ggplot2)
library(doSNOW)
library(bench)
# spark_install(version = 3.1)

# initiate the database
ecom_db <- dbConnect(SQLite(), "../data/ecom_2.sqlite")

# purchases per user of all products
products_purchased_per_customer <-
  "
  SELECT user_id, product_id, 1 AS purchased
  FROM customer
  WHERE event_type IN ('purchase')
  ORDER BY user_id ASC;
  "

# use query to extract needed data
system.time(
  purchase_df <- dbGetQuery(ecom_db, products_purchased_per_customer) #Executing the purchases query on the entire database (OCT-NOV) takes 47 seconds
)["elapsed"]
purchase_df

# Disconnect from database
dbDisconnect(ecom_db)

# configure spark connection
config <- spark_config()
config$spark.executor.memory <- "8G"
config$spark.executor.cores <- 4
config$spark.executor.instances <- 3
config$spark.dynamicAllocation.enabled <- "false"

# initiate connection
sc <- spark_connect(master = "local", config=config, version = "3.1")

# Spark data
purchase_spark <- as(purchase_df,"data.frame")
purchase_spark <- sdf_copy_to(sc, purchase_spark, "purchase_spark", overwrite = TRUE) 
purchase_spark

#### Filters ####

# Filter for users who purchased more than 5 different products
filtered_users = purchase_spark %>% 
  group_by(user_id) %>%
  filter(n() >5) %>%
  pull(user_id) %>%
  sort() %>%
  unique()


# Filter for products purchased more than 15 times
filtered_products = purchase_spark %>% 
  group_by(product_id) %>%
  filter(n() >15) %>%
  pull(product_id) %>%
  sort() %>%
  unique() 


# Filter spark df for both above conditions
purchase_filtered_users_products = purchase_spark %>% 
  filter((user_id %in% filtered_users && product_id %in% filtered_products)) # & product_id %in% filtered_products)

# check if it worked
# sort(table(purchase_filtered_users_products %>% pull(product_id)))
# sort(table(purchase_filtered_users_products %>% pull(user_id)))

### Train test split ###
set.seed(42)
split = sdf_random_split(purchase_filtered_users_products, training = 0.95, test = 0.05)

spark_train <- sdf_copy_to(sc, split$training, "train", overwrite = TRUE) 
# spark_train %>% pull(user_id) %>% unique() %>% length() #9015 unique users in train set
# spark_train %>% pull(product_id) %>% unique() %>% length() #48187 unique products in train set

spark_test <- sdf_copy_to(sc, split$test, "test", overwrite = TRUE) 

### Train ALS model ###
als_fit <- ml_als(spark_train, 
                  max_iter = 5, 
                  implicit_prefs = TRUE,
                  nonnegative = TRUE, 
                  rank = 2,
                  rating_col = "purchased", 
                  user_col = "user_id", 
                  item_col = "product_id")

### Create test sample ###
create_test_sample = function(num_users, spark_test){
  # Prepare a test dataframe for prediction method
  # All combination of products per (e.g. 1000 users, 4500 products --> 1000*4500 = 4.5m rows)
  # 3 columns (user_id, product_id, purchased [0-1])
  set.seed(42)
  user_id = sample(spark_test %>% pull(user_id) %>% unique(),num_users)
  product_id = spark_test %>% pull(product_id) %>% unique() #pull unique product ids from all data instead of test
  # cross join products and users
  cartesian_product = data.frame(tidyr::crossing(user_id, product_id))
  # left join actual purchases from these users (binary) and fill NA with 0
  test_complete = left_join(cartesian_product, spark_test, on= c("user_id" = "user_id", "product_id" = "product_id"), copy = TRUE)
  test_complete = replace_na(test_complete, list(purchased = 0)) %>% distinct(user_id, product_id, .keep_all= TRUE)
  
  return(test_complete)
}

# Generate test set for 1000 users
test_complete = create_test_sample(1000, spark_test) #Lightning fast compared to for loop implementation

# Copy to spark cluster --> takes some time
system.time(
  spark_test_complete <- sdf_copy_to(sc, test_complete, "test_complete", overwrite = TRUE)  #6 seconds for 1000 users for OCT data (~2000 products) --> 14 seconds for 1000 users OCT-NOV data (~4500 products)
)["elapsed"]

# Predict on test set
system.time(
  predicted <- ml_predict(als_fit, spark_test_complete) %>% collect() #8 seconds for 1000 users for OCT data --> 24 seconds for 1000 users with OCT-NOV data
)["elapsed"]

# Benchmark prediction to compare with recommenderlab
# bench::mark(
#   ml_predict(als_fit, spark_test_complete) %>% collect()
# )


create_evaluation_data <- function(n, predicted){
  # Create evaluation data set with --> top-n recommendations
  # One row per customer containing various evaluation metrics (TP, FP etc)
  evaluation_data = predicted %>% 
    group_by(user_id) %>% 
    arrange(desc(prediction)) %>%
    mutate(actual_positives = sum(purchased)) %>%
    mutate(actual_negatives = sum(purchased == 0)) %>%
    slice(1:n) %>% 
    #filter(user_id %in% c(513997627, 512503671)) %>% #picking some users for sample table
    mutate(true_positives = sum(purchased)) %>%
    mutate(false_positives = sum(purchased == 0)) %>%
    mutate(true_negatives = actual_negatives - false_positives) %>%
    mutate(false_negatives = actual_positives - true_positives) %>%
    mutate(TPR = true_positives/(true_positives + false_negatives)) %>%
    mutate(FPR = false_positives/(true_negatives + false_positives)) %>%
    mutate(precision = true_positives/(false_positives + true_positives)) %>%
    mutate(conversion = true_positives/n) %>% # self-defined evaluation metric measuring the number of items purchased from top-n recommendations
    mutate(recall = TPR) %>%
    group_by(user_id) %>%
    slice(1:1) %>%
    select(-c("product_id", "purchased", "prediction"))
  return(evaluation_data)
}

# Test for top 3
evaluation_3 = create_evaluation_data(3,predicted) %>%
  # arrange(desc(true_positives)) 
  filter(user_id %in% c(513997627, 512503671)) #example users for sample table

evaluation_3

# Plot confusion matrix for example customer
example_user = evaluation_3[1,]
example_user
Actual <- factor(c("No Purchase", "No Purchase", "Purchase", "Purchase"))
Predicted <- factor(c("No Purchase", "Purchase", "No Purchase", "Purchase"))
Y      <- c(example_user$actual_negatives - example_user$false_negatives , example_user$false_positives, example_user$false_negatives, example_user$true_positives)
df <- data.frame(Actual, Predicted, Y)

plotTable <- df %>%
  group_by(Actual) %>%
  mutate(prop = Y/sum(Y))

conf_matrix = ggplot(data =  plotTable, mapping = aes(x = Actual, y = Predicted, alpha = prop)) +
  geom_tile(aes(fill = Y), colour = "white") +
  scale_fill_gradient(name = "Frequency", low = "lightblue", high = "blue") +
  geom_text(aes(label = Y), vjust = .5, fontface  = "bold", alpha = 1) +
  labs(title = "Confusion Matrix for Customer #512503671", colour = "Model",alpha="Proportion") +
  theme_light() 

conf_matrix

top_n_evaluation <- function(n, predicted){
  # Calculate evaluation metrics averaged for all users for different values of top-n
  evaluation_data = create_evaluation_data(n,predicted)
  TPR_n = mean(evaluation_data$TPR)
  FPR_n = mean(evaluation_data$FPR)
  precision_n = mean(evaluation_data$precision)
  conversion_n = mean(evaluation_data$conversion)
  return(list(name = "ALS_Spark", n = n, TPR = TPR_n, FPR = FPR_n, precision = precision_n, recall = TPR_n, conversion = conversion_n))
}

# Test for top-5 
results_5 = top_n_evaluation(5, predicted)
results_5

# Generate evaluation metrics for different top-n

res_list <- c()
j <- 1

system.time(
  for (i in c(1, 3, 5, 10, 15, 20, 25)){
    results_n <- top_n_evaluation(i, predicted)
    res_list[j] <- list(results_n)
    j <- j+1
  })["elapsed"] # 9.31 seconds for 1000 users for OCT-NOV data

res_df <- do.call(rbind.data.frame, res_list)

res_df



# ## parallelize evaluation
# 
# # get the number of cores available
# ncores <- parallel::detectCores() # set cores for parallel processing 
# ctemp <- makeCluster(ncores) # registerDoSNOW(ctemp)
# 
# system.time(
#   output <- foreach(i = c(1, 3, 5, 10, 15, 20, 25), .combine = rbind)%dopar%{
#         results_n = top_n_evaluation(i, predicted)
#     }
# )["elapsed"] #Interestingly parallelized calculation is slightly slower at 9.61 seconds for 1000 users using OCT-NOV data
# 
# # Unpack results to DF
# res_df = data.frame(output) %>%
#   `rownames<-`(1:7) %>%
#   unnest(cols = c(name, n, TPR, FPR, precision, recall, conversion)) 


### Plot Performance ###
# ROC
ROC_plot = res_df %>%
  ggplot(aes(FPR, TPR, 
             colour = "ALS_Spark")) +
  geom_line() +
  geom_label(aes(label = n))  +
  labs(title = "ROC Curve", colour = "Model") +
  theme_light(base_size = 14) + 
  scale_color_manual(values = c(ALS_Spark = "blue"))

ROC_plot

# Precision Recall
precision_recall_plot = res_df %>%
  ggplot(aes(recall, precision, 
             colour = "ALS_Spark")) +
  geom_line() +
  geom_label(aes(label = n))  +
  labs(title = "Precision-Recall Curve", colour = "Model") +
  theme_light(base_size = 14) + 
  scale_color_manual(values = c(ALS_Spark = "blue"))

precision_recall_plot

# Conversion
# We feel that conversion is a more appropriate metric of the usefulness of our recommender system
conversion_plot = res_df %>%
  ggplot(aes(n, conversion, 
             colour = "conversion")) +
  geom_line() +
  labs(title = "Conversions per N Recommendations", colour = "Metric") +
  theme_light(base_size = 14) + 
  scale_color_manual(values = c(conversion = "blue"))

conversion_plot

### Measuring economic value of our algorithm ###

get_TP_products <- function(n, predicted){
  # Get a data frame of True Positive products and their frequencies
  ture_positive_product_ids = predicted %>% 
    group_by(user_id) %>% 
    arrange(desc(prediction)) %>%
    slice(1:n) %>% 
    filter(purchased == 1) %>%
    pull(product_id) %>%
    table() %>%
    data.frame() %>%
    rename(product_id = ".") %>%
    mutate(product_id = as.numeric(levels(product_id))[product_id])
  return(ture_positive_product_ids)}

get_product_prices <- function(TP_product_ids){
  # initiate the database
  ecom_db <- dbConnect(SQLite(), "../data/ecom_2.sqlite")
  
  # get prices for products that we successfully recommended from SQL database
  products_prices <- paste0( 
    "
    SELECT DISTINCT(product_id), price
    FROM product
    WHERE product_id IN (",paste(TP_product_ids[,1], collapse=", "),")
    ORDER BY product_id ASC")
  
  price_df <- dbGetQuery(ecom_db, products_prices)
  
  # Disconnect from database
  dbDisconnect(ecom_db)
  
  return(price_df)}

generate_revenue_df <- function(n, predicted, assumed_conversion_rate){
  # Generate dataframe of true positive products, their prices and revenue contributions
  TP_products = get_TP_products(n, predicted)
  price_df = get_product_prices(TP_products) 
  
  # Create revenue table by joining prices to True Positives per product
  revenue_df = inner_join(TP_products, price_df) %>%
    mutate(revenue = price*Freq*assumed_conversion_rate) # multiply price*quantity by assumed conversion rate
  
  return(revenue_df)}


# Test for 5 recommendations
conversion_rate = 0.35 #based on a McKinsey study, 35% of amazon sales are due to recommendations. We assume 0.35 conversion rate for calculating expected revenue increase.
test_5 = generate_revenue_df(5,predicted,conversion_rate)

# Sum revenue from true positives:
sum(test_5$revenue)


# # Calculate revenues  for different top-n
# rev_list = c()
# j = 1
# system.time(
# for (i in c(1, 3, 5, 10, 15, 20, 25)){ #Try with doSNOW
#   revenues_n = generate_revenue_df(i,predicted, conversion_rate)
#   rev_list[j] = list(list(name = "ALS Spark", n = i, revenue = sum(revenues_n$revenue)))
#   j = j+1
# })["elapsed"] #6.93 seconds for 1000 users with OCT-NOV data
# 
# 
# rev_df = do.call(rbind.data.frame, rev_list)
# rev_df


# Paralellize revenue calculation
ncores <- parallel::detectCores()
ctemp <- makeCluster(ncores) 

system.time(
  output <- foreach(i = c(1, 3, 5, 10, 15, 20, 25), .combine = rbind)%dopar%{
    revenues_n = generate_revenue_df(i,predicted, conversion_rate)
    rev_n = list(name = "ALS Spark", n = i, revenue = sum(revenues_n$revenue))
  }
)["elapsed"] #Parallelized calculation is slightly faster

# Unpack results to DF
rev_df = data.frame(output) %>% `rownames<-`(1:7) %>% 
  unnest(cols = c(name, n, revenue))



#Plot revenues as a function of N
revenue_plot = rev_df %>%
  ggplot(aes(n, revenue/1000, 
             colour = "revenue")) +
  geom_line() +
  ylab("Revenue (thousand $)") + 
  labs(title = "Revenue per N Recommendations", colour = "Measure") +
  theme_light(base_size = 14) + 
  scale_color_manual(values = c(revenue = "blue"))

revenue_plot



























### Old code with for loop within evaluation function --> deprecated ###

# # Create test set for loop 
# spark_test_loop <- spark_test 
# 
# top_n_evaluation <- function(top_n, number_users_test, test_data) {
#   true_positive_list = c()
#   
#   #shuffle data
#   set.seed(42)
#   unique_users_test <- unique(test_data %>% pull(user_id))
#   unique_users_test <- unique_users_test[sample(1:length(unique_users_test))]
#   
#   
#   for (i in 1:number_users_test){
#     
#     unique_users_test_i <- unique_users_test[i]
#     
#     # Receive test data for one specific user
#     one_user <- filter(test_data, user_id == unique_users_test_i)[c("purchased", "user_id", "product_id")]
#     one_user <- copy_to(sc, one_user, overwrite = TRUE)
#     
#     # One_user
#     one_user_index <- one_user %>% pull(user_id) %>% unique()
#     
#     # Get product id for the products which were not bought; leave 1 out!
#     num_products = test_data %>% pull(product_id) %>% unique() %>% length()
#     list_product_id_popular_missing <- setdiff(1:num_products, one_user %>% pull(product_id))
#     
#     # Create the missing data for not purchased items
#     add_data <- tibble(
#       "purchased" = rep(0, length(list_product_id_popular_missing)),
#       "user_id" = rep(one_user_index, length(list_product_id_popular_missing)),
#       "product_id" = list_product_id_popular_missing
#     )
#     add_data <- copy_to(sc, add_data, overwrite = TRUE) #botthleneck
#     
#     # append the existing test data for the user with data for not purchased items
#     one_data_predict <- union_all(one_user, add_data)
#     
#     # predict for the complete test data for this one user
#     one_predict_test <- ml_predict(als_fit, one_data_predict) %>% collect() #bottleneck
#     
#     # Sort predictions
#     one_predict_sorted <- one_predict_test %>% 
#       arrange(desc(prediction))
#     # one_predict_sorted
#     
#     true_positive_i <- sum((one_predict_sorted %>% pull(purchased))[1:top_n]) / top_n
#     true_positive_list <- append(true_positive_list, true_positive_i)
#   }
#   
#   return(mean(true_positive_list))
# }
# 
# # Inefficient because of loop within the function
# system.time(
#   TPR <- top_n_evaluation(10,100, spark_test_loop) #10.95 seconds to predict for 10 users, 110.04 seconds for 100 users
# )["elapsed"]
# TPR
# 
# 
# tpr_list = c()
# for (i in c(1, 3, 5, 10, 15, 20)){
#   TPR = top_n_evaluation(i,100, spark_test_loop)
#   tpr_list = append(tpr_list, TPR)
# }
# 
# tpr_list
# 
# # Conclusion: evaluation using a loop within the function is not feasible --> would not allow for real time predictions.
# # Code had to be made more efficient with vectorised operations (joins) and paralelized loops (foreach)