# Databricks notebook source
# DBTITLE 1,Blocks Data Processing
from pyspark.sql.types import StructField,StructType,StringType,TimestampType
from pyspark.sql.functions import lit

s=StructType([
  StructField("created",TimestampType(),False),
  StructField("email",StringType(),True),
  StructField("reason",StringType(),True),
  StructField("staus",StringType(),True)
])


filepath="/FileStore/tables/blocks1-1.json"
data=spark.read.option("multiLine",True).format("json").schema(s).option("lineSep ",'\n\r').load(filepath)
df=data.withColumn("source",lit('block'))
df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.write.mode("overwrite").json("/FileStore/tables/block_handle")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/block_handle

# COMMAND ----------

output="/FileStore/tables/block_handle"
df.write.mode("overwrite").csv(output,sep=',',header=True)
display(df)

# COMMAND ----------

# DBTITLE 1,Bounce Data Processing
from pyspark.sql.types import StructField,StructType,StringType,TimestampType
from pyspark.sql.functions import lit

s=StructType([
  StructField("created",TimestampType(),False),
  StructField("email",StringType(),True),
  StructField("reason",StringType(),True),
  StructField("staus",StringType(),True)
])


filepath="/FileStore/tables/bounces1-1.json"
data=spark.read.option("multiLine",True).format("json").schema(s).option("lineSep ",'\n\r').load(filepath)
df_bounce=data.withColumn("source",lit('bounce'))
display(df_bounce)

# COMMAND ----------

df_bounce.printSchema()

# COMMAND ----------

df_bounce.write.mode("append").json("/FileStore/tables/block_handle")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/block_handle

# COMMAND ----------

output="/FileStore/tables/block_handle"
df_bounce.write.mode("append").csv(output,sep=',',header=True)
#display(df_bounce)

# COMMAND ----------


