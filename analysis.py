# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FULL_DATA_PATH = os.path.join(BASE_PATH, 'full_data')

conf = SparkConf().setAppName("myAppName").setMaster("local[*]").set("spark.executor.cores", "4").set("spark.executor.memory", '8g').set("spark.driver.memory", '8g')
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.format("parquet").load(os.path.join(FULL_DATA_PATH, 'consolidated.parquet'))
df.createOrReplaceTempView('full_data')
# %%

spark.sql('select count(*) from full_data;').show()