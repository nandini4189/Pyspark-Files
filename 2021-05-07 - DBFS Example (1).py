# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/output_csv_files/part-00000-tid-9058030871796043574-cb82fb4d-e2ff-487b-a9bc-5fb3bfbef62c-41-1-c000.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Create a view or table

temp_table_name = "bounce_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `bounce_csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from bounce_csv

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "part-00000-tid-9058030871796043574-cb82fb4d-e2ff-487b-a9bc-5fb3bfbef62c-41-1-c000_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC insert into `blocks_csv`
# MAGIC select * from `bounce_csv`

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select count(*) from blocks_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from blocks_csv

# COMMAND ----------


