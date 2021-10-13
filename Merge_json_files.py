# Databricks notebook source
#zipcodes .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zipcodes.json"
data=spark.read.option("multiline",True).json(filepath)
df1=data.withColumn("source", lit('Zipcodes'))

df1.display()


# COMMAND ----------

df = sqlContext.read.json ("/FileStore/tables/Zipcodes.json")

# COMMAND ----------

# this creates a view of the json dataset
df.createOrReplaceTempView("json_view")

# COMMAND ----------

df.show()

# COMMAND ----------

df.withColumnRenamed("Location","Loc")

# COMMAND ----------

df.show()

# COMMAND ----------


