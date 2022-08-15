rm(list=ls())

library("recommenderlab")
library("tidyverse")
library("readr")
library("ggplot2")
library("dplyr")
library("fastDummies")

df <- read_csv("10k_rows")

# put in bins of 10 dollars: value represents bin ceiling
df$price_bin <- ceiling(df$price/10)*10

keeps <- c("product_id", "category_id", "brand", "price_bin")
df <- df[keeps]

# seeing whether there are any missing values (1=yes, 0=no)
missing_catIDs <- length(unique(is.na(df$category_id))) - 1
missing_brands <- length(unique(is.na(df$brand))) - 1

# replace missing brand with "unknown"
df$brand <- df$brand %>% replace_na("unknown")

#####
# histogram for price
# hist_5bin <- ggplot(df, aes(x=price)) + geom_histogram(binwidth = 5)
# hist_10bin <- ggplot(df, aes(x=price)) + geom_histogram(binwidth = 10)
# hist_25bin <- ggplot(df, aes(x=price)) + geom_histogram(binwidth = 25)
# hist_50bin <- ggplot(df, aes(x=price)) + geom_histogram(binwidth = 50)
#####

# n_bins <- length(unique(df$price_bin)) # will go into no. of columns
# 
# # no. of items --> no. of matrix rows
# n_items <- length(unique(df$product_id))
# 
# # no. of brands, category IDs --> no. matrix columns
# n_brands <- length(unique(df$brand))
# n_catIDs <- length(unique(df$category_id))
# 
# n_columns <- n_brands + n_catIDs + n_bins
# 
# # create item-feature matrix
# feat_mat <- as.data.frame(matrix(rep(0, n_items*n_columns), n_items, n_columns))
# 
# rownames(feat_mat) <- unique(df$product_id)
# 
# my_features <- c(unique(df$brand), unique(df$category_id), unique(df$price_bin))
# colnames(feat_mat) <- my_features

# insert ones

# item-feature matrix for category IDs

feature_matrix <- unique(dummy_cols(df, c("category_id", "brand", "price_bin"),
                             remove_selected_columns = TRUE))
feature_matrix <- feature_matrix[-1]
rownames(feature_matrix) <- unique(df$product_id)

feature_matrix <- feature_matrix %>%
  as.matrix() %>%
  as("binaryRatingMatrix")

scheme <- feature_matrix %>%
  evaluationScheme(method = "cross",
                   k = 5,
                   train = 0.8,
                   given = 0)

scheme

algorithms <- list(
  "association rules" = list(name  = "AR", 
                             param = list(supp = 0.01, conf = 0.01)),
  "random items"      = list(name  = "RANDOM",  param = NULL),
  "popular items"     = list(name  = "POPULAR", param = NULL),
  "item-based CF"     = list(name  = "IBCF", param = list(k = 5)),
  "user-based CF"     = list(name  = "UBCF", 
                             param = list(method = "Cosine", nn = 500))
)

results <- recommenderlab::evaluate(scheme, 
                                    algorithms, 
                                    type  = "topNList", 
                                    n     = c(1, 3, 5, 10, 15, 20)
)

results

# Pull into a list all confusion matrix information for one model 
tmp <- results$`user-based CF` %>%
  getConfusionMatrix()  %>%  
  as.list() 
# Calculate average value of 5 cross-validation rounds 
as.data.frame( Reduce("+",tmp) / length(tmp)) %>% 
  # Add a column to mark the number of recommendations calculated
  mutate(n = c(1, 3, 5, 10, 15, 20)) %>%
  # Select only columns needed and sorting out order 
  select('n', 'precision', 'recall', 'TPR', 'FPR')

avg_conf_matr <- function(results) {
  tmp <- results %>%
    getConfusionMatrix()  %>%  
    as.list() 
  as.data.frame(Reduce("+",tmp) / length(tmp)) %>% 
    mutate(n = c(1, 3, 5, 10, 15, 20)) %>%
    select('n', 'precision', 'recall', 'TPR', 'FPR') 
}

# Using map() to iterate function across all models
results_tbl <- results %>%
  map(avg_conf_matr) %>% 
  # Turning into an unnested tibble
  enframe() %>%
  # Unnesting to have all variables on same level
  unnest()
results_tbl

results_tbl %>%
  ggplot(aes(FPR, TPR, 
             colour = fct_reorder2(as.factor(name), 
                                   FPR, TPR))) +
  geom_line() +
  geom_label(aes(label = n))  +
  labs(title = "ROC curves", colour = "Model") +
  theme_grey(base_size = 14)

results_tbl %>%
  ggplot(aes(recall, precision, 
             colour = fct_reorder2(as.factor(name),  
                                   precision, recall))) +
  geom_line() +
  geom_label(aes(label = n))  +
  labs(title = "Precision-Recall curves", colour = "Model") +
  theme_grey(base_size = 14)