rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directiory wherever you save this R script
setwd(wd)

library("recommenderlab")
library("tidyverse")

df = read.csv("data 10k rows.csv")

df = df[df$event_type == 'purchase',]


grp <- c('user_id','product_id')

# spread data from long to wide format 
matrix_data <- df %>% 
  # Add a column of 1s
  mutate(purchased = 1) %>%
  # Select columns
  select(user_id, product_id, purchased) %>% 
  # Spread
  # Group by two columns
  group_by(across(all_of(grp))) %>% 
  # Summarize
  summarize(sum_purchased = sum(purchased)) %>%
  # Spread
  spread(product_id, sum_purchased, fill=0) %>%
  # Set user_id to be index
  column_to_rownames('user_id')


# convert to matrix
ui_mat <- matrix_data %>% as.matrix()
ui_mat

# store matrix as realRatingMatrix
ui_mat <- as(ui_mat,"realRatingMatrix")


ratings_movies_norm <- normalize(ui_mat)



# #Test some stuff
# 
# eval_sets <- evaluationScheme(data = ratings_movies_norm,
#                               method = "cross-validation",
#                               k = 10,
#                               given = 5,
#                               goodRating = 0)
# models_to_evaluate <- list(
#   `IBCF Cosinus` = list(name = "IBCF", 
#                         param = list(method = "cosine")),
#   `IBCF Pearson` = list(name = "IBCF", 
#                         param = list(method = "pearson")),
#   `UBCF Cosinus` = list(name = "UBCF",
#                         param = list(method = "cosine")),
#   `UBCF Pearson` = list(name = "UBCF",
#                         param = list(method = "pearson")),
#   `Zufälliger Vorschlag` = list(name = "RANDOM", param=NULL)
# )
# n_recommendations <- c(1, 5, seq(10, 100, 10))
# list_results <- evaluate(x = eval_sets, 
#                          method = models_to_evaluate, 
#                          n = n_recommendations)
# 
# 
# plot(list_results)
# 
# 
# #Test more stuff
# 
# set.seed(1)
# 
# evalu <- eval_sets <- evaluationScheme(data = ratings_movies_norm,
#                                        method = "cross-validation",
#                                        k = 10,
#                                        given = 5,
#                                        goodRating = 0)
# # prep data
# train <- getData(evalu, 'train') # Training Dataset
# dev_test <- getData(evalu, 'known') # Test data from evaluationScheme of type KNOWN
# test <- getData(evalu, 'unknown') # Unknow datset used for RMSE / model evaluation
# # test@data
# 
# 
# # using recommender lab, create UBCF recommender with z-score normalized data using cosine similarity
# UB <- Recommender(getData(evalu, "train"), "UBCF",
#                   param=list(normalize = "Z-score", method="Cosine"))
# 
# # create rating predictions and store
# p <- predict(UB, dev_test, type="ratings")
# 
# 
# UB_acc <- calcPredictionAccuracy(p, getData(evalu, "unknown"))
# 
# 
# 
# p@data








