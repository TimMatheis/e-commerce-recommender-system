rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directiory wherever you save this R script
setwd(wd)

library("recommenderlab")
library("tidyverse")

#df = read.csv("data 10k rows.csv")
df = read.csv("df_150k_Oct.csv")

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
  spread(product_id, sum_purchased) %>%
  # Set user_id to be index
  column_to_rownames('user_id')

# Resetting the rownames
#rownames(matrix_data) = seq(length=nrow(matrix_data))

# Adding values to rows and columns
#matrix_data[,2] = 1
#matrix_data[2,] = 1
matrix_data[1,] = 2
#matrix_data[,1] = 2

# for(i in 1:2000){
#   matrix_data[i,2] = 1
#   matrix_data[i,5] = 1
#   matrix_data[i,7] = 1
#   }
#   
# for(i in 1:1000){
#   matrix_data[2,i] = 1
#   matrix_data[5,i] = 1
#   matrix_data[7,i] = 1
#   }



ui_mat <- matrix_data %>% as.matrix()
# store matrix as realRatingMatrix
ui_mat <- as(ui_mat,"realRatingMatrix")


#Testing
dt = sort(sample(nrow(ui_mat), nrow(ui_mat)*.7))
train<-ui_mat[dt,]
test<-ui_mat[-dt,]

#train@data
test@data

rec <- Recommender(train, method = "UBCF")
rec

pre <- predict(rec, test, n = 10)
pre

