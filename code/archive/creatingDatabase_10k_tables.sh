cd "C:\Users\timma\OneDrive - Handelshögskolan i Stockholm\HSG\Big Data Analytics\project"

sqlite3 ecom_10k_tables_db.sqlite

CREATE TABLE product(
"product_id" INTEGER PRIMARY KEY,
"category_id" INTEGER,
"category_code" STRING,
"brand" STRING,
"price" REAL
);

CREATE TABLE customer(
"event_time" DATE,
"event_type" STRING,
"product_id" INTEGER,
"user_id" INTEGER,
"user_session" STRING,
FOREIGN KEY(product_id) REFERENCES product(product_id)
);

-- prepare import
.mode csv

-- LINUX: split up csv file
cut -d, -f1,2,3,8,9 data_10k_rows.csv > data_10k_rows_customer.csv
cut -d, -f3,4,5,6,7 data_10k_rows.csv > data_10k_rows_product.csv

-- import data from csv
.import --skip 1 data_10k_rows_customer.csv customer
.import --skip 1 data_10k_rows_product.csv product