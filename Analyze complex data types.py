# Databricks notebook source
dbutils.widgets.text("filename","")
filename = dbutils.widgets.get("filename")
dbutils.widgets.text("delimiter","")
delimiter = dbutils.widgets.get("delimiter") 
dbutils.widgets.text("adls_container_name","")
adls_container_name = dbutils.widgets.get("adls_container_name") 
dbutils.widgets.text("adls_storage_account_name","")
adls_storage_account_name = dbutils.widgets.get("adls_storage_account_name")

# COMMAND ----------

# DBTITLE 1,Import Libraries
## Import pandas dataframes for transformation
import pandas as pd
# Import pandas schema library for datatype and schema validation
#import pandas_schema
# Import schema validation librarires
#from pandas_schema import Column
# Import Custom validation pandas schema libraries
#from pandas_schema.validation import CustomElementValidation,CustomSeriesValidation
#import numpy as np
#Import decimal Library
#from decimal import *
#import sys
import math
from csv import writer
# contains string manipulation libraries
import re
#import datatime library
from datetime import datetime
#import pyspark libraries
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import col
# import time library to perfrom time related validations.
import time

# COMMAND ----------



# COMMAND ----------

 sp_client_id=dbutils.secrets.get(scope = "bbsession04", key = "DBxSPClient")

# COMMAND ----------

# DBTITLE 1,Extract Service Principal details from Azure Key Vault
#databricks client id
sp_client_id = dbutils.secrets.get(scope = "bbsession04", key = "DBxSPClient")
#databricks client secret
sp_client_secret = dbutils.secrets.get(scope = "bbsession04", key = "DBxSP")
#azure tenantid
az_tenant_id = dbutils.secrets.get(scope = "bbsession04", key = "DBxSPTenant")
#storage account name
adls_storage_account_name=dbutils.secrets.get(scope = "bbsession04", key = "BlobStorageName")
#set the configurations to establish a connection to ADLS using service principal.
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net",sp_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",sp_client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net",
               "https://login.microsoftonline.com/"+az_tenant_id+"/oauth2/token")
#set the hadoop configuration
spark.sparkContext._jsc.hadoopConfiguration().set("fs.azure.account.auth.type."+adls_storage_account_name, "OAuth")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", sp_client_id)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",sp_client_secret)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+az_tenant_id+"/oauth2/token")


# COMMAND ----------

raw_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/Raw/"+filename
#df = spark.read.format("csv"). \
#        option("header","true"). \
   #     option("inferSchema","true"). \
   #     option("delimiter", delimiter).load(raw_path)
df=spark.read.json(raw_path,multiLine=True)

# COMMAND ----------

from pyspark.sql.functions import col

def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType
df_flat = flatten_df(df)
display(df_flat.limit(10))

# COMMAND ----------

user_list = [
  {
    "id":1,
    "phone_numbers":["1111","2222"],
    "addresses":["abc","abc2"]
  },
  {
    "id":2,
    "phone_numbers":["3333","44444"],
    "addresses":["abc2","abc3"]
    
  }
]

# COMMAND ----------

from pyspark.sql import Row
user_rows = [Row(**user)   for user in user_list]

# COMMAND ----------

df1=spark.createDataFrame(user_rows)

# COMMAND ----------

from pyspark.sql.functions import explode,col, arrays_zip

# COMMAND ----------

df1.withColumn("array_combo",arrays_zip(*["phone_numbers","addresses"])).printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_flat.withColumn('exp_combo', arrays_zip(*["topping","batters_batter"])).withColumn('exp_combo2', explode('exp_combo')).show(truncate=False)

# COMMAND ----------

def explode_cols( df, cl):
    df = df.withColumn('exp_combo', arrays_zip(*cl))
    df = df.withColumn('exp_combo', explode('exp_combo'))
    for colm in cl:
        final_col = 'exp_combo.'+ colm+'.id'
        final_col_name = colm+'_'+'id'
        
        final_col_val = 'exp_combo.'+ colm+'.type'
        final_col_name_val = colm+'_'+'type'
        
        df = df.withColumn(final_col_name, col(final_col))
        df = df.withColumn(final_col_name_val, col(final_col_val))
        #print col
        #print ('exp_combo.'+ colm)
    return df #.drop(col('exp_combo'))

# COMMAND ----------

lst=["topping","batters_batter"]
result = explode_cols( df_flat, lst)
result.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



