# Reset the environment
rm(list=ls())

# Set the working directory wherever you save this R script
wd <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(wd)

# initiate run time
ptm <- proc.time()

# Load packages
my_packages <- c("tidyverse", "purrr", "dplyr", "recommenderlab", "pryr",
                 "profvis", "data.table", "RSQLite")

for (i in 1:length(my_packages)){
  if(!require(my_packages[i], character.only = TRUE)){
    install.packages(my_packages[i])
  }
}

# Initiate the database
ecom_db <- dbConnect(SQLite(), "../data/ecom_2.sqlite")

# Get dataframe
get_df <-
  "
  SELECT *
  FROM customer
  WHERE event_type IN ('purchase')
  LIMIT 150000
"

df <- dbGetQuery(ecom_db, get_df)

# Disconnect from database
dbDisconnect(ecom_db)

# Create a column concatenating product id to user id 
df <- df %>%
  mutate(userproduct = paste(user_id, product_id))

# Drop duplicates (we're only interested in whether customer purchased, not
# how many times they purchased)
df <- df[!duplicated(df$userproduct), ] %>%
  select(-userproduct)

# Create a binary matrix of implicit ratings: buy = 1, not buy = 0.
# Columns are product IDs, rows are user IDs
ratings_matrix <- df %>%
  select(user_id, product_id) %>%
  mutate(purchased = 1) %>%
  spread(product_id, purchased, fill = 0) %>%
  column_to_rownames("user_id") %>%
  as.matrix() %>%
  as("binaryRatingMatrix")

plotting_matrix <- df %>%
  select(user_id, product_id) %>%
  mutate(purchased = 1) %>%
  spread(product_id, purchased, fill = 0) %>%
  column_to_rownames("user_id") %>%
  as.data.frame()

# Plot number of unique purchases per user
ggplot(plotting_matrix, aes(x = rowSums(plotting_matrix))) +
  geom_histogram(binwidth = 1) +
  xlab("Unique purchases per user") +
  ylab("Count")

# Set up evaluation scheme
scheme <- ratings_matrix %>%
  evaluationScheme(method = "cross",
                   k = 5,
                   train = 0.8,
                   given = -1,
                   goodRating = 1)

scheme

# Define recommender algorithms
algorithms <- list(
  "Random items"  = list(name = "RANDOM",  param = list(method = 'Jaccard')),
  "Popular items" = list(name = "POPULAR", param = list(method = 'Jaccard')),
  "Item-based CF" = list(name = "IBCF", param = list(method = 'Jaccard', k = 5)),
  "User-based CF" = list(name = "UBCF", param = list(method = "Jaccard", nn = 500)),
  "ALS implicit"  = list(name = "ALS_implicit",
                             param = list(lambda       = 0.1,
                                          alpha        = 10,
                                          n_factors    = 2,
                                          n_iterations = 5,
                                          seed         = NULL,
                                          verbose      = TRUE))
)

# Calculate results of the recommender algorithms
results <- recommenderlab::evaluate(scheme,
                                    algorithms,
                                    type = "topNList",
                                    n    = c(1, 3, 5, 10, 15, 20)
)

# Create a function where we:
  # Get the confusion matrix for a model and turn it into a list
  # Get the average value of the five rounds of cross validation
  # Add a column keeping track of the no. of observations (top-N)
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
results_tbl %>%
  ggplot(aes(FPR, TPR, colour = fct_reorder2(as.factor(name), FPR, TPR))) +
  geom_line() +
  geom_label(aes(label = n))  +
  labs(title = "ROC curves", color = "Model") +
  theme_light()

# Select number of users to make predictions for
n_pred <- 1000

# Function for recommending for a number of users
r2 <- Recommender(data = ratings_matrix[1:(nrow(ratings_matrix)-n_pred),],
                 method = "ALS",
                 parameter = list(lambda=0.1,
                                  alpha = 10,
                                  n_factors=2,
                                  n_iterations=5,
                                  seed = NULL,
                                  verbose = FALSE))

# number of recommendations (N in top-N)
n_rec = 15

# predict purchases
system.time(recom_topNList2 <- recommenderlab::predict(object = r2,
                              newdata = tail(ratings_matrix, n_pred),
                              type = "topNList",
                              n = n_rec)
)
as(recom_topNList2, "list")

# get run time
proc.time() - ptm
