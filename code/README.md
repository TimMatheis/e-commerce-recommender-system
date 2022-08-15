# README: Code of our project

## How to execute our code

Our data is from [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (please click on the link). In order to execute our code, you first have to download both csv files for October and November 2019. 

1. `creating_csv_files_for_db.R`: Once you saved the csv files, you can use this script for obtaining the "cleaned" csv files for the database.
2. `creatingDatabase_2csv_tables.sh`: Creates a database containing 2 tables, one table for all orders ('customer') and one for additional product information ('product').
3. `sql_queries_2.R`: Includes queries for analyzing the data in the database(s).
4. `binary_recommender2.R`: Our first attempt of predicting customer purchases using the recommenderlab package.
5. `sparklyr_v2.R`: Our most successful attempt of predicting customer purchases using sparklyr and ALS.

<p>&nbsp;</p>  

![Summary of our project](../images/summary.drawio.png)
