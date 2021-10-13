# Databricks notebook source
# Databricks notebook source
#zipcode .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip_5r.json"
data=spark.read.option("multiline",False).json(filepath)
df1=data.withColumn("source", lit('Zip_5r'))

df1.display()


# COMMAND ----------

# Databricks notebook source
#zipcode2r .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip_3r.json"
data=spark.read.option("multiline",False).json(filepath)
df2=data.withColumn("source", lit('Zip_3r'))

df2.display()


# COMMAND ----------

df=df1.union(df2)

# COMMAND ----------

df.count()

# COMMAND ----------

df.show()

# COMMAND ----------

#converting to csv file
result="/FileStore/tables/result.csv"
df.write.mode("overwrite").csv(result,sep=',',header=True)
display(df)


# COMMAND ----------


# Create a view or table

temp_table_name = "result_csv"

df.createOrReplaceTempView(temp_table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `result_csv`

# COMMAND ----------

permanent_table_name = "result_csv"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

#Zip63r .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip63r.json"
data=spark.read.option("multiline",False).json(filepath)
dataframe1=data.withColumn("source", lit('Zip63r'))

dataframe1.display()


# COMMAND ----------

#Zip42r .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip42r.json"
data=spark.read.option("multiline",False).json(filepath)
dataframe2=data.withColumn("source", lit('Zip42r'))

dataframe2.display()

# COMMAND ----------

dataframe3=dataframe1.union(dataframe2)

# COMMAND ----------

dataframe3.count()

# COMMAND ----------

#converting to csv file
result="/FileStore/tables/result.csv"
dataframe3.write.mode("append").csv(result,sep=',',header=True)
display(dataframe3)


# COMMAND ----------

dataframe3.count()

# COMMAND ----------


