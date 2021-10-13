# Databricks notebook source
#zipcodes .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zipcodes.json"
data=spark.read.option("multiline",True).json(filepath)
df1=data.withColumn("source", lit('Zipcodes'))

df1.display()

# COMMAND ----------

#Zipcodes2 json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zipcodes2.json"
data=spark.read.option("multiline",True).json(filepath)
df2=data.withColumn("source", lit('Zipcodes2'))
df2.display()

# COMMAND ----------

#combining two json files data
df=df1.union(df2)

# COMMAND ----------

#converting to csv file
output="/FileStore/tables/output.csv"
df.write.mode("overwrite").csv(output,sep=',',header=True)
display(df)


# COMMAND ----------Read json file through sql command

df = sqlContext.read.json ("/FileStore/tables/Zipcodes.json")
















 