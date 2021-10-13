# Databricks notebook source
dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

#dbutils.fs.mkdirs("/FileStore/tables/json1")
#dbutils.fs.mkdirs("/FileStore/tables/jsonoutput")
#dbutils.fs.mkdirs("/FileStore/tables/jsoninput")
dbutils.fs.mkdirs("/FileStore/tables/json2")

# COMMAND ----------

from pyspark.sql.functions import lit
filepath="/FileStore/tables/json2/blocks.json"
data=spark.read.option("multiline",True).json(filepath)
df1=data.withColumn("source", lit('blocks'))
df1.display()
#data['source'] = 'blocks'
#json.dump('/FileStore/tables/json2/blocks.json', data)

# COMMAND ----------

filepath="/FileStore/tables/json2/bounces.json"
data=spark.read.option("multiline",True).json(filepath)
df2=data.withColumn("source", lit('bounces'))
df2.display()

# COMMAND ----------

df = df1.union(df2)

# COMMAND ----------

###display(df)

# COMMAND ----------

output="/FileStore/tables/jsonoutput/output.csv"
df.write.mode("overwrite").csv(output,sep=',',header=True)
display(df)



