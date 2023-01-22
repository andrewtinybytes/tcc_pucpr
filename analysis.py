# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
SCHEMA_DATA_PATH = os.path.join(BASE_PATH, 'schema_data')
PLAYERS_ACTIONS_PATH = os.path.join(BASE_PATH, 'players_actions')

spark = SparkSession.builder.master('local[*]').appName('default').getOrCreate()
sc = spark.sparkContext

df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(PLAYERS_ACTIONS_PATH)
df.createOrReplaceTempView('full_data')
df.printSchema()
df = df.withColumnRenamed("logid", "logID")

logid_df = spark.read.csv(os.path.join(SCHEMA_DATA_PATH, 'BnS_LogSchema_GameLog.csv'), header=True, inferSchema=True)
logid_df = logid_df.select('logID', 'LogName_EN', 'Log_Detail_Code')

logdetail_df = spark.read.csv(os.path.join(SCHEMA_DATA_PATH, 'BnS_LogSchema_Code.csv'), header=True, inferSchema=True)
logdetail_df = logdetail_df.withColumnRenamed("Category", "Log_Detail_Code")
logdetail_df = logdetail_df.withColumnRenamed("Code", "log_detail_code")

df = logid_df.join(df, ['logID'], how='inner')
df = df.drop("actor_code","link_id")
df = df.join(logdetail_df, ['log_detail_code', 'Log_Detail_Code'], how='left')

# Create a new DataFrame with only unique columns

df = df.drop("Log_Detail_Code")
# %%

# Querying

df.createOrReplaceTempView('full_data')

spark.sql('SELECT * from full_data limit 100;').show(5)