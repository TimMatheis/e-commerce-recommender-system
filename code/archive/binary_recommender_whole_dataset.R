rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directiory wherever you save this R script
setwd(wd)

# load packages

# if necessary: install.packages("RSQLite")
library(RSQLite)
library(data.table)
library(tidyverse)

# initiate the database
ecom_db <- dbConnect(SQLite(), "C:/Users/timma/OneDrive - Handelshögskolan i Stockholm/HSG/Big Data Analytics/project/ecom_2csv_tables_db.sqlite")

# purchases per user of all products
cosine_user_product_query <-
  "
SELECT user_id, product_id, COUNT(event_type) AS number_purchases
FROM customer
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
matrix_data <- cosine_user_product_clean_df %>% 
    select(user_id, product_id, number_purchases) %>% 
    spread(product_id, number_purchases, fill=0)
matrix_data

# create binary matrix
ratings_matrix <- matrix_data %>%
                  as.matrix() %>%
                  as("binaryRatingMatrix")

# Plot purchases per user
hist(rowCounts(ratings_matrix), main = "Distribution of Purchases per User")
abline(v=mean(rowCounts(ratings_matrix)),col="red")
abline(v=median(rowCounts(ratings_matrix)),col="blue")

# HERE IS STILL AN ERROR
# Plot purchases per product
hist(colCounts(ratings_matrix), breaks=c(1, seq(2,100, 1)), xlim=c(1,25), main = "Distribution of Purchases per Product")
abline(v=mean(colCounts(ratings_matrix)),col="red")
abline(v=median(colCounts(ratings_matrix)),col="blue")

scheme <- ratings_matrix %>%
  evaluationScheme(method = "cross",
                   k = 5,
                   train = 0.8,
                   given = -1)

scheme

algorithms <- list(
  "association rules" = list(name  = "AR", 
                             param = list(supp = 0.01, conf = 0.01)),
  "random items"      = list(name  = "RANDOM",  param = list(method = 'Jaccard')),
  "popular items"     = list(name  = "POPULAR", param = list(method = 'Jaccard')),
  "item-based CF"     = list(name  = "IBCF", param = list(method = 'Jaccard', k = 5)),
  "user-based CF"     = list(name  = "UBCF", 
                             param = list(method = "Jaccard", nn = 500))
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