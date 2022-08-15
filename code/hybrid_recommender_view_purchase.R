rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directory wherever you save this R script
setwd(wd)

library("recommenderlab")
library("tidyverse")
library("readr")
library("ggplot2")
library("dplyr")
library("fastDummies")

# user - item - purchase
df <- read_csv("data 10k rows.csv")
df = df[df$event_type == 'purchase',]

df <- df %>%
  mutate(userproduct = paste(user_id, product_id))

df <- df[!duplicated(df$userproduct), ] %>%
  select(-userproduct )

user_interaction_matrix <- df %>%
  select(user_id, product_id) %>%
  mutate(purchased = 1) %>%
  spread(product_id, purchased, fill=0) %>%
  column_to_rownames("user_id") %>%
  as.matrix() %>%
  as("binaryRatingMatrix")

set.seed(1)
dt = sort(sample(nrow(user_interaction_matrix), nrow(user_interaction_matrix)*.8))
train_purchases<-user_interaction_matrix[dt,]
test_purchases<-user_interaction_matrix[-dt,]



#user - item - view 
df = read.csv("data 10k rows.csv")
#df = read.csv("df_150k_Oct.csv")

df = df[df$event_type == 'view',]

df <- df %>%
  mutate(userproduct = paste(user_id, product_id))

df <- df[!duplicated(df$userproduct), ] %>%
  select(-userproduct )

view_matrix <- df %>%
  select(user_id, product_id) %>%
  mutate(purchased = 1) %>%
  spread(product_id, purchased, fill=0) %>%
  column_to_rownames("user_id") %>%
  as.matrix() %>%
  as("binaryRatingMatrix")


#filter item - feature to only have items also present in user-interaction matrix
view_matrix = view_matrix[rownames(user_interaction_matrix),colnames(user_interaction_matrix)]

train_views<-view_matrix[dt,]
test_views<-view_matrix[-dt,]




# Hybrid system
recom <- HybridRecommender(
  Recommender(train_purchases, method = "UBCF"),
  Recommender(train_views, method = "UBCF"),
  weights = c(.8, .2)
)

recom

getModel(recom)

pre = predict(recom, test_purchases, n = 3)

as(pre, "list")['513451653']


a = test_purchases['513451653']
a@data@itemInfo[["labels"]][33]
