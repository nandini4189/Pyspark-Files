# Databricks notebook source
# Databricks notebook source
#zipcode .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip_5.json"
data=spark.read.option("multiline",False).json(filepath)
df1=data.withColumn("source", lit('Zip_5'))

df1.display()


# COMMAND ----------



#zipcode2r .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip_3.json"
data=spark.read.option("multiline",False).json(filepath)
df2=data.withColumn("source", lit('Zip_3'))

df2.display()

# COMMAND ----------

df2.count()

# COMMAND ----------

#combining two json files data
df=df1.union(df2)


# COMMAND ----------

#converting to csv file
output="/FileStore/tables/output.csv"
df.write.mode("overwrite").csv(output,sep=',',header=True)
display(df)


# COMMAND ----------

df.display()

# COMMAND ----------

#zipcodes .json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip_5.json"
data=spark.read.option("multiline",False).json(filepath)
Zip_5=data.withColumn("source", lit('Zip_5'))

Zip_5.display()


# COMMAND ----------

#Zipcodes2 json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/Zip_3.json"
data=spark.read.option("multiline",True).json(filepath)
Zip_3=data.withColumn("source", lit('Zip_3'))
Zip_3.display()

# COMMAND ----------

Zip_3.count()

# COMMAND ----------

Zip_3.display()

# COMMAND ----------

# MAGIC %fs ls /Filestore/tables

# COMMAND ----------

dataframe=Zip_5.union(Zip_3)

# COMMAND ----------

dataframe.show()

# COMMAND ----------

#converting to csv file
output_Zip="/FileStore/tables/output_Zip.csv"
dataframe.write.mode("overwrite").csv(output_Zip,sep=',',header=True)
display(dataframe)

# COMMAND ----------

df.count()

# COMMAND ----------

dataframe.count()

# COMMAND ----------


