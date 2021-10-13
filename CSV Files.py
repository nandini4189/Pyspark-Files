# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('Spark').getOrCreate()

df = spark.read.csv("/FileStore/tables/Zipcode.csv")

df.printSchema()



# COMMAND ----------

df.show()

# COMMAND ----------

df.count()

# COMMAND ----------


df2 = spark.read.option("header",True) \
     .csv("/FileStore/tables/Zipcode.csv")
df2.printSchema()
   


# COMMAND ----------

df2.show()

# COMMAND ----------

schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)
      
df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/FileStore/tables/Zipcode.csv")
df_with_schema.printSchema()

# COMMAND ----------

df2.write.option("header",True) \
 .csv("/user/hive/warehouse/zipcodes123")

# COMMAND ----------

df2.show()

# COMMAND ----------


