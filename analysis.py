# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FULL_DATA_PATH = os.path.join(BASE_PATH, 'full_data')

conf = SparkConf().setAppName("myAppName").setMaster("local[*]").set("spark.executor.cores", "8").set("spark.executor.memory", '8g').set("spark.driver.memory", '8g')
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.format("parquet").load(os.path.join(FULL_DATA_PATH, 'consolidated.parquet'))
df.createOrReplaceTempView('full_data')
# %%


# %%
spark.sql("""
             with leave_table as (

                select seq,
                       to_timestamp(time) as ts,
                       actor_account_id,
                       LogName_EN as action
        
                from full_data
                
                where LogName_EN = 'LeaveWorld'

             )

             select full_data.actor_account_id,
                    count(distinct case when action = 'Die' then leave_table.seq end) as Die_tilt,
                    count(distinct case when action = 'LoseItem' then leave_table.seq end) as LoseItem_tilt

             from full_data
             left join leave_table 
             on full_data.actor_account_id = leave_table.actor_account_id
             and to_timestamp(leave_table.ts) between to_timestamp(full_data.time) and to_timestamp(full_data.time) + interval 10 seconds

             where full_data.LogName_EN in ('Die', 'LoseItem')
             
             group by 1

             limit 50

             ;""").show(50, truncate=False)

# %%

spark.sql("""
             select logname_en, count(*) from full_data

             group by 1

             ;""").show(500, truncate=False,)
# %%
