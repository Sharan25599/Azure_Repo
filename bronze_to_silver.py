# Databricks notebook source
# MAGIC %run /Workspace/Notebook/utils

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

#Mount from ADF pipeline

source='wasbs://bronze@adlsgen2storagedemo.blob.core.windows.net/'
mountpoint = '/mnt/mountpointstorageadf'
key='fs.azure.account.key.adlsgen2storagedemo.blob.core.windows.net'
value='H3wCjGz4E9GZGdxVGbwRomywoo6k2Y5Wwerut7owez5DDSR3tEwhiusJagkaUaZZhWV7nWCaAVR+AStXsbk5Q=='
mounting_from_adf(source,mountpoint,key,value)

# COMMAND ----------

#Reading the customer file

options={'header':'true','delimiter':',','inferSchema':'True'}
customer_df=read("csv", 'dbfs:/mnt/mountpointstorageadf/sales_view/customer/20240107_sales_customer.csv', options)


# COMMAND ----------

#Customer table columns converting camelcase to snake_case

a=udf(snake_case_to_lower,StringType())
lst = list(map(lambda x:snake_case_to_lower(x),customer_df.columns))
df = customer_df.toDF(*lst)

# COMMAND ----------

#"Name" column split by " " and create two columns first_name and last_name

df = split_column(df,'name')

# COMMAND ----------

#created new column domain and extracted from email columns Ex: Email = "josephrice131@slingacademy.com" domain="slingacademy"

df = domain_column(df, 'email_id','@','\.')

# COMMAND ----------

#created a column gender where male = "M" and Female="F"

df = create_gender_column(df, 'gender')

# COMMAND ----------

#created two colums date and time by splitting based on " " delimiter

df = split_joining_date_column(df, 'joining_date')

# COMMAND ----------

#Formatting the date column "yyyy-MM-dd"

df = format_date_column(df, 'date', input_format='dd-MM-yyyy', output_format='yyyy-MM-dd')

# COMMAND ----------

#Created a column expenditure-status

df= expenditure_status_column(df, 'spent')

# COMMAND ----------

#writing the customer table based on upsert

path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'customer'
save_as_delta(df, path, db_name, tb_name, ["employee_id"])

# COMMAND ----------

#Reading the product file

options={'header':'true','delimiter':',','inferSchema':'True'}
product_df=read("csv", "dbfs:/mnt/mountpointstorageadf/sales_view/products/20240107_sales_product.csv", options )

# COMMAND ----------

#product table columns converting camelcase to snake_case

a=udf(snake_case_to_lower,StringType())
lst = list(map(lambda x:snake_case_to_lower(x),product_df.columns))
df = product_df.toDF(*lst)

# COMMAND ----------

#Created a column sub_category 

df = sub_category_column(df, 'category_id', 'sub_category')

# COMMAND ----------

#writing the product table based on upsert

path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'product'

save_as_delta(df, path, db_name, tb_name, ["product_id"])

# COMMAND ----------

#Reading the store table 

options={'header':'true','delimiter':',','inferSchema':'True'}
store_df=read("csv", "dbfs:/mnt/mountpointstorageadf/sales_view/store/20240107_sales_store.csv", options )

# COMMAND ----------

#store table columns converting camelcase to snake_case

a=udf(snake_case_to_lower,StringType())
lst = list(map(lambda x:snake_case_to_lower(x),store_df.columns))
df = store_df.toDF(*lst)

# COMMAND ----------

#Created a store category columns and the value is exatracted from email 

df1 = extract_email_column(df_store, 'email_address', '@', '\.')

# COMMAND ----------

#writing the store table based on upsert

path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'Store'

save_as_delta(df, path, db_name, tb_name, ["store_id"])

# COMMAND ----------

#Reading the sales table based on upsert

options={'header':'true','delimiter':',','inferSchema':'True'}
sales_df=read("csv", "dbfs:/mnt/mountpointstorageadf/sales_view/sales/20240107_sales_data.csv", options)

# COMMAND ----------

#sales table columns converting camelcase to snake_case

a=udf(snake_case_to_lower,StringType())
lst = list(map(lambda x:snake_case_to_lower(x),sales_df.columns))
df = sales_df.toDF(*lst)


# COMMAND ----------

#Select Required Columns from Product and Store Tables

path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'sales'

save_as_delta(df, path, db_name, tb_name, ["product_id"])
