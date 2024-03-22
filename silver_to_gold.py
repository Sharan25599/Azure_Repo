# Databricks notebook source
# MAGIC %run /Workspace/Notebook/utils

# COMMAND ----------

#Selected required Columns from Product and Store Tables

gold_df = select_required_columns(product_df, store_df)

# COMMAND ----------

#Reading the delta table

customer_sales_gold_path = "gold/sales_view/store_product_sales_analysis"
customer_sales_gold_df = spark.read.format("delta").table(customer_sales_gold_path)

# COMMAND ----------


final_df = transform_data_for_gold_layer(customer_sales_gold_df, gold_df)

# COMMAND ----------

#writing to delta table using overwrite method

final_gold_path = "gold/sales_view/store_product_sales_analysis"
final_df.write.format("delta").mode("overwrite").save(final_gold_path)