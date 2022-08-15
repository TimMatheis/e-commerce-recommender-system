cd "../data"

sqlite3 ecom_2.sqlite

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

-- import data from csv
.import --skip 1 data_customer_Oct.csv customer
.import --skip 1 data_customer_Nov.csv customer
.import --skip 1 data_product_OctNov.csv product