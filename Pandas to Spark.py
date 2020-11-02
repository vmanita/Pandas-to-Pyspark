# Databricks notebook source
def pd_to_spark(path, dbfs_path = True, sheet_name = 0, header = 0):
  from pyspark.sql import types
  import pandas as pd
  
  # if dbfs, change to correct pandas format
  if dbfs_path:
    path = path.replace('dbfs:','/dbfs')
  
  # read excel file
  df = pd.read_excel(path)
  
  # Fix Unnamed columns (i.e.: from 'Unnamed: 10' to '_c10')
  df.columns = [x.replace('Unnamed: ', '_c') for x in df.columns]
  
  # dictionary of {'col name': type}
  d_dtypes = df.dtypes.apply(lambda x: x.name).to_dict()
  
  # create schema
  schema = []
  for col_name, col_type in d_dtypes.items():
    if 'int' in col_type:
      t =  types.IntegerType()
    elif 'float' in col_type:
      t = types.FloatType()
    elif 'datetime' in col_type:
      t = types.DateType()
    else: # Object
      df[col_name] = df[col_name].astype(str)
      t = types.StringType()
    
    schema.append(types.StructField(col_name, t, True))
  
  schema = types.StructType(schema)
  
  # create Spark DataFrame
  df = spark.createDataFrame(df,schema = schema)
  
  return df

# COMMAND ----------

# Usage example
#path = 'dbfs:/.../file.xlsx'
#df = pd_to_spark(path = path, dbfs_path = True, sheet_name = 0, header = 0)
#display(df)
