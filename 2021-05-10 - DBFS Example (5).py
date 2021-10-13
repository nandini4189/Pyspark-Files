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
file_location = "/FileStore/tables/df_output.csv/part-00001-tid-2074718805060583873-1dd21e22-ee50-4780-904a-4142924fda1f-224-1-c000.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
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

# Create a view or table

temp_table_name = "samplefile1"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC select * from samplefile1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into samplefile
# MAGIC select * from samplefile1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samplefile

# COMMAND ----------

permanent_table_name = "samplefile"

df.write.format("parquet").saveAsTable(permanent_table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samplefile

# COMMAND ----------


