# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
SCHEMA_DATA_PATH = os.path.join(BASE_PATH, 'schema_data')
EXTRACTED_DATA_PATH = os.path.join(BASE_PATH, 'extracted_data')
FULL_DATA_PATH = os.path.join(BASE_PATH, 'full_data')

conf = SparkConf().setAppName("myAppName").setMaster("local[*]").set("spark.executor.cores", "4").set("spark.executor.memory", '8g').set("spark.driver.memory", '8g')
spark = SparkSession.builder.config(conf=conf).getOrCreate()

parquet_files = [os.path.join(EXTRACTED_DATA_PATH, f) for f in os.listdir(EXTRACTED_DATA_PATH) if f.endswith('.parquet')]
df = spark.read.format("parquet").load(parquet_files)

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

df.select("seq", 
          "time", 
          "actor_account_id", 
          "LogName_EN").write\
                       .option("maxRecordsPerFile", 100000)\
                       .mode("overwrite")\
                       .parquet(os.path.join(FULL_DATA_PATH, 'consolidated.parquet'))