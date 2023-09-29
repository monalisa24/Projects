# Databricks notebook source
# source to Dataset https://datahub.io/core/glacier-mass-balance

# COMMAND ----------

# MAGIC %md
# MAGIC Getting Dataset from Source and reading data using libraries
# MAGIC

# COMMAND ----------

import requests
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC reading .csv file from source and trying to put data into dbfs 

# COMMAND ----------

with requests.get('https://datahub.io/core/glacier-mass-balance/r/glaciers.csv', stream=True) as r:
    with open('/dbfs/glacier.csv','wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

# COMMAND ----------

# MAGIC %md
# MAGIC Also writing above code in function so that it could be reused . another thing is giving filename at runtime which is cool

# COMMAND ----------

def get_data(url:str):
    filename = url.split('/')[-1] #splitting url and getting only csv filename so that same file name is entered into dbfs folder
    with requests.get('https://datahub.io/core/glacier-mass-balance/r/glaciers.csv', stream=True) as r:
        with open("/dbfs/{}".format(filename),'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return filename


# COMMAND ----------

file_name=get_data('https://datahub.io/core/glacier-mass-balance/r/glaciers.csv')

# COMMAND ----------

file_name

# COMMAND ----------

# MAGIC %md
# MAGIC Since all Data is copied into DBFS folder. 
# MAGIC Now reading the data from DBFS folder 

# COMMAND ----------

spark.read.format("csv").option("header","true").load("file:/dbfs/glacier.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Knowing the type of file format like json,.csv, txt etc

# COMMAND ----------

file_format = file_name.split(".")[-1]

# COMMAND ----------

def read_data(file_name):
    if file_format == 'csv':
        df=spark.read.format(file_format).option("header","true").load("file:/dbfs/{}".format(file_name))
    elif file_format == 'json':
        try:
            df=spark.read.format(file_format).load("file:/dbfs/{}".format(file_name))
        except:
            df=spark.read.format(file_format).option("multiline","true").load("file:/dbfs/{}".format(file_name))
    elif file_format=='parquet':
        df=spark.read.format(file_format).load("file:/dbfs/{}".format(file_name))
    elif file_format=='txt':
        df=spark.read.text("file:/dbfs/{}".format(file_name))
    return df
        
            

# COMMAND ----------

df=read_data(file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC After reading data from file lets display and check if the data loaded is correct or not

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Performaing ETA operation . First I want to split data into 2 tables depending upon year .i.e fro 1945 - 2000 in one table ans 2000 onwards in another

# COMMAND ----------

df.createOrReplaceTempView("df") # storing inot temporary view so that sql queries can be performed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view nintys as select * from df where Year like '19%' order by year asc; /*creating view to store data from year 1945 to 1999*/
# MAGIC create or replace temp view modern as select * from df where Year like '20%' order by year asc;/*creating view to store data for year 2000 onwards*/

# COMMAND ----------

nintys_df = spark.sql("select * from nintys")
modern_df = spark.sql("select * from modern")

# COMMAND ----------

display(nintys_df)

# COMMAND ----------

display(modern_df)

# COMMAND ----------

nintys_file_name =spark.sql("(select *  from nintys order by year asc limit 1) union (select * from nintys order by year Desc limit 1)")
modern_file_name =spark.sql("(select *  from modern order by year asc limit 1) union (select * from modern order by year Desc limit 1)")

# COMMAND ----------

nintys_file_name_df=nintys_file_name.collect()



# COMMAND ----------

modern_file_name_df=modern_file_name.collect()

# COMMAND ----------

display(nintys_file_name_df)

# COMMAND ----------

display(modern_file_name_df)

# COMMAND ----------

nintys_file_name_df[0].__getitem__('Year')+"-"+nintys_file_name_df[1].__getitem__('Year')

# COMMAND ----------

modern_file_name_df[0].__getitem__('Year')+"-"+modern_file_name_df[1].__getitem__('Year')

# COMMAND ----------

nintys_df.write.format('parquet').save("/dbfs/nintys_df.parquet")

# COMMAND ----------

modern_df.write.format('parquet').save("/dbfs/modern_df.parquet")

# COMMAND ----------

dbutils.fs.ls('/dbfs')

# COMMAND ----------


