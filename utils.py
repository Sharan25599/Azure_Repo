# Databricks notebook source
from pyspark.sql.functions import col, split, substring, when
from pyspark.sql.functions import col, date_format, to_date
from delta.tables import *

# COMMAND ----------

#Created mount from ADF

def mounting_from_adf(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

#Reading the CSV file

def read(file_format,path,options):
     return spark.read.format(file_format).options(**options).load(path)

# COMMAND ----------

#Converting columns to snake_case

def snake_case_to_lower(x):
    a = x.lower()
    return a.replace(' ','_')

# COMMAND ----------

#Creating two columns by first_name and last_name.

def split_name_column(df, name_column):
    return df.withColumn("first_name", split(col(name_column), " ")[0]) \
             .withColumn("last_name", split(col(name_column), " ")[1])


# COMMAND ----------

#Creating domain column and extracting email columns.

def domain_column(df, email_column, split_char, split_char2):
    return df.withColumn("domain", split(col(email_column),split_char)[1]).withColumn('domain', split(col(email_column), split_char2)[0])

# COMMAND ----------

#Create a column gender where male = "M" and Female="F".

def create_gender_column(df, gender_column):
    return df.withColumn("gender", when(col(gender_column) == "male", "M").otherwise("F"))

# COMMAND ----------

#From Joining date create two colums date and time by splitting based on " " delimiter.

def split_joining_date_column(df, joining_date_column):
    return df.withColumn("date", split(col(joining_date_column), " ")[0]) \
             .withColumn("time", split(col(joining_date_column), " ")[1])

# COMMAND ----------

#Date column should be on "yyyy-MM-dd" format.

def format_date_column(df, date_column, input_format='dd-MM-yyyy', output_format='yyyy-MM-dd'):
    return df.withColumn(date_column, date_format(to_date(col(date_column), input_format), output_format))

# COMMAND ----------

#Creating a column expenditure-status

def expenditure_status_column(df, spent_column):
    return df.withColumn("expenditure_status", when(col(spent_column) < 200, "MINIMUM").otherwise("MAXIMUM"))

# COMMAND ----------

#Write based on upsert table.

def save_as_delta(res_df, path, db_name, tb_name, mergecol):
    base_path = f"{path}/{db_name}/{tb_name}"
    mappedCol = " AND ".join(list(map((lambda x: f"old.{x} = new.{x} "),mergecol)))
    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        res_df.write.mode("overwrite").format("delta").option("path", base_path).saveAsTable(f"{db_name}.{tb_name}")
    else:
         deltaTable = DeltaTable.forPath(spark, f"{base_path}")
         deltaTable.alias("old").merge(res_df.alias("new"),mappedCol).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

#Create sub_category column based on category_id

def sub_category_column(df, category_column, sub_category_column):
    return df.withColumn(sub_category_column, 
                        when(col(category_column) == 1, "phone")
                        .when(col(category_column) == 2, "laptop")
                        .when(col(category_column) == 3, "playstation")
                        .when(col(category_column) == 4, "e-device")
                        .otherwise("unknown"))

# COMMAND ----------

#Create a store category columns and the value is exatracted from email

def extract_email_column(df, email_column, split_char, split_char2):
    return df.withColumn("store_category", split(col(email_column), split_char)[1]) \
             .withColumn('store_category', split(col(email_column), split_char2)[0])

# COMMAND ----------

#Selecting the colums based on product and store tables

def select_required_columns(product_df,store_df):
    return product_df.select(
        "store_id", "store_name", "location", "manager_name",
        "product_name", "product_code", "description", "category_id",
        "price", "stock_quantity", "supplier_id",
        "product_created_at", "product_updated_at", "image_url",
        "weight", "expiry_date", "is_active", "tax_rate"
    ).join(
        store_df.select("store_id", "store_name", "location", "manager_name"),
        "store_id"
    )

# COMMAND ----------

#transfering the data to gold layer

def transform_data_for_gold_layer(customer_sales_df, gold_df):
    return customer_sales_df.join(
        gold_df,
        customer_sales_df.product_id == gold_df.product_code,
        "inner"
    ).select(
        "order_date", "category", "city", "customer_id", "order_id",
        "product_id", "profit", "region", "sales", "segment",
        "ship_date", "ship_mode", "latitude", "longitude",
        "store_name", "location", "manager_name", "product_name",
        "price", "stock_quantity", "image_url"
    )