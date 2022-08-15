cd "C:\Users\timma\OneDrive - Handelshögskolan i Stockholm\HSG\Big Data Analytics\project"

sqlite3 ecom_db.sqlite

CREATE TABLE ecom(
"event_time" DATE,
"event_type" STRING,
"product_id" INTEGER,
"category_id" INTEGER,
"category_code" STRING,
"brand" STRING,
"price" REAL,
"user_id" INTEGER,
"user_session" STRING
);

-- prepare import
.mode csv

-- import data from csv
.import --skip 1 Oct19.csv ecom