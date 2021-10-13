#blocks jjson file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/blocks1.json"
data=spark.read.option("multiline",True).json(filepath)
df1=data.withColumn("source", lit('blocks'))

df1.display()

#bounces json file
from pyspark.sql.functions import lit
filepath="/FileStore/tables/bounces1.json"
data=spark.read.option("multiline",True).json(filepath)
df2=data.withColumn("source", lit('bounces'))
df2.display()

#combining two json files data
df=df1.union(df2)

#converting to csv file
output="/FileStore/tables/output.csv"
df.write.mode("overwrite").csv(output,sep=',',header=True)
display(df)
