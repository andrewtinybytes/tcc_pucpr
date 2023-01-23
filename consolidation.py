# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
SCHEMA_DATA_PATH = os.path.join(BASE_PATH, 'schema_data')
FULL_DATA_PATH = os.path.join(BASE_PATH, 'full_data')
PLAYERS_ACTIONS_PATH = os.path.join(BASE_PATH, 'players_actions')

conf = SparkConf().setAppName("myAppName").setMaster("local[*]").set("spark.executor.cores", "4")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(PLAYERS_ACTIONS_PATH)
df.printSchema()

df = df.withColumnRenamed("logid", "logID")

logid_df = spark.read.csv(os.path.join(SCHEMA_DATA_PATH, 'BnS_LogSchema_GameLog.csv'), header=True, inferSchema=True)
logid_df = logid_df.select('logID', 'LogName_EN', 'Log_Detail_Code')

logdetail_df = spark.read.csv(os.path.join(SCHEMA_DATA_PATH, 'BnS_LogSchema_Code.csv'), header=True, inferSchema=True)
logdetail_df = logdetail_df.withColumnRenamed("Category", "Log_Detail_Code")
logdetail_df = logdetail_df.withColumnRenamed("Code", "log_detail_code")

df = logid_df.join(df, ['logID'], how='inner')\
                                              .drop(logid_df.Log_Detail_Code)\
                                              .drop(df.actor_code)\
                                              .drop(df.link_id)

df = df.join(logdetail_df, ['log_detail_code', 'Log_Detail_Code'], how='left')\
       .drop(df['log_detail_code'])

df.createOrReplaceTempView('full_data')

spark.sql("""SELECT seq, 
                    time, 
                    LogName_EN, 
                    actor_account_id 
             from full_data
             
             limit 200;""").show(5)