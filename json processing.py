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


# COMMAND ----------

df = sqlContext.read.json ("/FileStore/tables/Zipcodes.json")

# COMMAND ----------

#converting to csv file
output="/FileStore/tables/output_add.csv"
df2.write.mode("overwrite").csv(output,sep=',',header=True)
display(df2)


# COMMAND ----------

# this creates a view of the json dataset
df.createOrReplaceTempView("json_view")


# COMMAND ----------

df.show()

# COMMAND ----------

df2.withColumnRenamed("Location","Loc")

# COMMAND ----------

df2.show()

# COMMAND ----------

people = sqlContext.read.json("/FileStore/tables/output.csv")

# COMMAND ----------

people.printSchema()

# COMMAND ----------

people.registerTempTable("people")

# COMMAND ----------

#converting to csv file
output="/FileStore/tables/output_sample.csv"
df.write.mode("overwrite").csv(output,sep=',',header=True)
display(df)


# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

# COMMAND ----------

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

# COMMAND ----------

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])

# COMMAND ----------

df =spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------


