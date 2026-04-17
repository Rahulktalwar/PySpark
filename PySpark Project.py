# Databricks notebook source
# MAGIC %md
# MAGIC ###Data Reading

# COMMAND ----------

# DBTITLE 1,Untitled
dbutils.fs.ls("/Volumes/workspace/raw_gcd/contracts_file/")

# COMMAND ----------

df_sales=spark.read.csv("/Volumes/workspace/raw_gcd/contracts_file/BigMart Sales.csv", header=True, inferSchema=True)

# COMMAND ----------

df_sales.show(10)

# COMMAND ----------

df_sales.display(10)

# COMMAND ----------

df_drivers= spark.read.format('json').option('header',True).option('inferSchema',True).load("/Volumes/workspace/raw_gcd/contracts_file/drivers.json")

# COMMAND ----------

df_drivers.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Definition

# COMMAND ----------

df_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DDL Schema

# COMMAND ----------

my_ddl_schema = '''
Item_Identifier STRING,
Item_Weight STRING,
Item_Fat_Content STRING,
Item_Visibility DOUBLE,
Item_Type STRING,
Item_MRP DOUBLE,
Outlet_Identifier STRING,
Outlet_Establishment_Year INT,
Outlet_Size STRING,
Outlet_Location_Type STRING,
Outlet_Type STRING,
Item_Outlet_Sales DOUBLE
'''

# COMMAND ----------

df_sales=spark.read.format('csv').option('header',True).schema(my_ddl_schema).load("/Volumes/workspace/raw_gcd/contracts_file/BigMart Sales.csv")


# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_drivers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ###SELECT

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df_sales.select(col("Item_Identifier"),col("Item_Weight"),col("Item_Fat_Content")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ALIAS

# COMMAND ----------

df_sales.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

df_sales.select(col("Item_MRP").alias('MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###FILTER/WHERE
# MAGIC row level slicing

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####scenario-1: Filter the data with Fat Content Regular

# COMMAND ----------

df_sales.select(col("Item_Fat_Content")).distinct().display()

# COMMAND ----------

df_sales.filter(col("Item_Fat_Content")=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###scenario-2: Slice the data with Item_Type Soft Drinks and weight less than 10

# COMMAND ----------

df_sales.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##scenario-3: fetch the data with tier in (tier1,tier2) and outlet size is NULL

# COMMAND ----------

df_sales.filter(col("Outlet_Size").isNull() & col("Outlet_Location_Type").isin('Tier 1','Tier 2')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumnRenamed

# COMMAND ----------

df_sales.withColumnRenamed("Item_Weight","Item_Wt").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ###scenario-1: create a new column called flag with 'new' as values

# COMMAND ----------

# DBTITLE 1,Cell 39
df_sales=df_sales.withColumn('flag',lit('new'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###create a new column with product of Item_Weight and Item_MRP

# COMMAND ----------

df_sales=df_sales.withColumn('product',col("Item_Weight")*col("Item_MRP"))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###scenario-3: modify the Item_Fat_Content values

# COMMAND ----------

df_sales.withColumn('Item_Fat_Content',regexp_replace(col("Item_Fat_Content"),'Regular','Reg'))\
        .withColumn('Item_Fat_Content',regexp_replace(col("Item_Fat_Content"),'Low Fat','Lf')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Type Casting

# COMMAND ----------

df_sales= df_sales.withColumn('Item_Weight',col("Item_Weight").cast(StringType()))

# COMMAND ----------

df_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###sort/orderby

# COMMAND ----------

df_sales.sort(col("Item_Weight").desc()).display()

# COMMAND ----------

df_sales.sort(col("Item_MRP").asc()).display()

# COMMAND ----------

df_sales.sort(['Item_Weight','Item_Visibility'], ascending=[0,0]).display()

# COMMAND ----------

df_sales.sort(["Item_Weight","Item_MRP"], ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###LIMIT

# COMMAND ----------

df_sales.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###INTERMEDIATE

# COMMAND ----------

# MAGIC %md
# MAGIC ###DROP

# COMMAND ----------

df_sales.drop("Item_Visibility").display()

# COMMAND ----------

df_sales.drop("Item_Visibility","Item_Type").display()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###dropDuplicates

# COMMAND ----------

# MAGIC %md
# MAGIC scenario1

# COMMAND ----------

df_sales.dropDuplicates().display()

# COMMAND ----------

df_sales.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC scenario2

# COMMAND ----------

df_sales.dropDuplicates(subset=["Outlet_Type"]).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing DataFrames

# COMMAND ----------

data1=[('1','kad'),
       ('2','sid')
       ] 
schema1='id STRING, name STRING'

df1=spark.createDataFrame(data1,schema1)   

# COMMAND ----------

data2=[('3','rahul'),
       ('4','jas')
       ]
schema2='id STRING, name STRING'

df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df2)

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union by Name

# COMMAND ----------

data1=[('kad','1'),
       ('sid','2')
       ] 
schema1='name STRING, id STRING' 

df1=spark.createDataFrame(data1,schema1)   

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###STRING FUNCTIONS

# COMMAND ----------

df_sales.limit(100).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###initcap(), lower(), upper()

# COMMAND ----------

df_sales.select(upper("Item_Type").alias('Item_Type')).display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC CURRENT_DATE()\
# MAGIC DATE_ADD()\
# MAGIC DATE_SUB()

# COMMAND ----------

df_sales = df_sales.withColumn('curr_date',current_date())
df_sales.display()

# COMMAND ----------

df_sales=df_sales.withColumn('week_after',date_add('curr_date',7))
df_sales.display()

# COMMAND ----------

df_sales.withColumn('prev_week',date_add('curr_date',-7)).display()

# COMMAND ----------

df_sales.withColumn('prev_week',date_sub('curr_date',7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DateDIFF

# COMMAND ----------

df_sales.withColumn('datediff', datediff('week_after','curr_date')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###date_format()

# COMMAND ----------

df_sales.withColumn('week_after',date_format('week_after','dd/MM/yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###HANDLING NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC #####dropna()

# COMMAND ----------

df_sales.dropna('all').display()

# COMMAND ----------

df_sales.dropna('any').display()

# COMMAND ----------

df_sales.dropna(subset="Outlet_Size").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Filling NULLS  fillna()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.fillna('NA').display()

# COMMAND ----------

df_sales.fillna(0).display()

# COMMAND ----------

df_sales.fillna(2000,subset=["Item_Weight"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####SPLIT and Indexing

# COMMAND ----------

df_sales.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Explode,  new row for every value in the list

# COMMAND ----------

df_exp=df_sales.withColumn('Outlet_Type',split('Outlet_Type',' '))

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####ARRAY_CONTAINS()

# COMMAND ----------

df_exp.withColumn('type2_flag', array_contains('Outlet_Type','Type2')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####GROUP BY 

# COMMAND ----------

# MAGIC %md
# MAGIC #####find the sum of MRP for each Item Type

# COMMAND ----------

df_sales.groupBy("Item_Type").agg(sum("Item_MRP").alias('MRP')).display()

# COMMAND ----------

####avg

df_sales.groupBy('Item_Type').agg(avg("Item_MRP").alias('avg')).display()

# COMMAND ----------

df_sales.groupBy("Item_Type").min("Item_MRP").display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_sales.groupBy("Item_Type").max('Item_MRP').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####scenario-3

# COMMAND ----------

df_sales.groupBy("Item_Type","Outlet_Size",).agg(sum("Item_MRP").alias('Total_MRP')).display()

# COMMAND ----------

df_sales.groupBy("Item_Type","Outlet_Size").agg(sum("Item_MRP"),avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####ADVANCED LEVEL

# COMMAND ----------

