rm(list=ls())
wd <- dirname(rstudioapi::getSourceEditorContext()$path) # Set the working directory wherever you save this R script
setwd(wd)

library("recommenderlab")
data("MovieLense")


#Testing
dt = sort(sample(nrow(MovieLense), nrow(MovieLense)*.7))
train<-MovieLense[dt,]
test<-MovieLense[-dt,]

#train@data
test@data

rec <- Recommender(train, method = "UBCF")
rec

pre <- predict(rec, test, n = 10)
pre



