# Reset the environment
rm(list=ls())

# Set the working directory wherever you save this R script
wd <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(wd)

# Load packages
my_packages <- c("recommenderlab", "tidyverse", "purrr", "tictoc")

for (i in 1:length(my_packages)){
  if(!require(my_packages[i], character.only = TRUE)){
    install.packages(my_packages[i])
  }
}

tic("Timer")

# Load data set
df <- read.csv("~/Library/Mobile Documents/com~apple~CloudDocs/Desktop/Skola/Master/HSG MA/Big_data/group-examination-brownian-boys/data/df_150k_Oct.csv")

# Filter out all non-purchase events
df <- df[df$event_type == 'purchase',]

# Create a column concatenating product id to user id 
df <- df %>%
  mutate(userproduct = paste(user_id, product_id))

# Drop duplicates (we're only interested in whether customer purchased, not
# how many times they purchased)
df <- df[!duplicated(df$userproduct), ] %>%
  select(-userproduct)

# Create a binary matrix of implicit ratings: buy = 1, not but = 0.
ratings_matrix <- df %>%
  select(user_id, product_id) %>%
  mutate(purchased = 1) %>%
  spread(product_id, purchased, fill=0) %>%
  column_to_rownames("user_id") %>%
  as.matrix() %>%
  as("binaryRatingMatrix")

# Plot purchases per user
hist(rowCounts(ratings_matrix), main = "Distribution of Purchases per User")
abline(v=mean(rowCounts(ratings_matrix)),col="red")
abline(v=median(rowCounts(ratings_matrix)),col="blue")

# Plot purchases per product
hist(colCounts(ratings_matrix), breaks=c(1, seq(2,100, 1)), xlim=c(1,25), main = "Distribution of Purchases per Product")
abline(v=mean(colCounts(ratings_matrix)),col="red")
abline(v=median(colCounts(ratings_matrix)),col="blue")

# Set up evaluation scheme
scheme <- ratings_matrix %>%
  evaluationScheme(method = "cross",
                   k = 5,
                   train = 0.8,
                   given = -1)

scheme

# Define recommender algorithms
algorithms <- list(
  "Random items"      = list(name  = "RANDOM",  param = list(method = 'Jaccard')),
  "Popular items"     = list(name  = "POPULAR", param = list(method = 'Jaccard')),
  "Item-based CF"     = list(name  = "IBCF", param = list(method = 'Jaccard', k = 5)),
  "User-based CF"     = list(name  = "UBCF", param = list(method = "Jaccard", nn = 500)),
  "ALS implicit"      = list(name  = "ALS_implicit",
                             param = list(lambda       = 0.1,
                                          alpha        = 10,
                                          n_factors    = 10,
                                          n_iterations = 10,
                                          seed         = NULL,
                                          verbose      = TRUE))
)

# Calculate results of the recommender algorithms
results <- recommenderlab::evaluate(scheme, 
                                    algorithms, 
                                    type = "topNList", 
                                    n    = c(1, 3, 5, 10, 15, 20)
)

results

# Create a function where we:
  # Get the confusion matrix for a model and turn it into a list
  # Get the average value of the five rounds of cross validation
  # Add a column keeping track of the no. of observations (top-n)
  # Subset the columns that we're interested in
avg_conf_matr <- function(results) {
  tmp <- results %>%
    getConfusionMatrix()  %>%  
    as.list() 
  as.data.frame(Reduce("+",tmp) / length(tmp)) %>% 
    mutate(n = c(1, 3, 5, 10, 15, 20)) %>%
    select('n', 'TPR', 'FPR') 
}

# Now, we iterate this function across all five models
results_tbl <- results %>%
  map(avg_conf_matr) %>% 
  enframe() %>%
  unnest()
results_tbl

# Plot ROC curves
results_tbl[25:30,] %>%
  ggplot(aes(FPR, TPR, 
             colour = fct_reorder2(as.factor(name), 
                                   FPR, TPR))) +
  geom_line() +
  geom_label(aes(label = n))  +
  labs(title = "ROC curves", colour = "Model") +
  theme_grey(base_size = 14)

# time the code 10k vs 150k
# function for recommending for any given user
# figure out definitions of TPR and FPR
#

toc()

