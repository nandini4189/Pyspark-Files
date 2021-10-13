# Databricks notebook source

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

temp_table_name = "samplefile2"

df.createOrReplaceTempView(temp_table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samplefile2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samplefile

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into samplefile select * from samplefile2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from samplefile

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc samplefile

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc df_output_csv

# COMMAND ----------


