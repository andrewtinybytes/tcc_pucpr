# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FULL_DATA_PATH = os.path.join(BASE_PATH, 'full_data')

conf = SparkConf().setAppName("tilt_agg") \
                  .setMaster("local[*]") \
                  .set("spark.executor.cores", "8") \
                  .set("spark.executor.memory", '8g') \
                  .set("spark.driver.memory", '8g') \
              #     .set("spark.sql.warehouse.dir", "hdfs://namenode/sql/metadata/hive") \
              #     .set("spark.sql.catalogImplementation","hive")

spark = SparkSession.builder \
                    .config(conf=conf) \
                    .enableHiveSupport() \
                    .getOrCreate()

df = spark.read.format("parquet").load(os.path.join(FULL_DATA_PATH, 'consolidated.parquet'))
df.createOrReplaceTempView('full_data')
# %%

%%time
spark.sql("""
             create table tilt_agg as
             with leave_table as (

                select seq,
                       to_timestamp(time) as ts,
                       actor_account_id,
                       LogName_EN as action
        
                from full_data
                
                where LogName_EN = 'LeaveWorld'

             ),

             die_table as (

             select full_data.actor_account_id,
                    count(distinct leave_table.seq) as count_die_tilt

             from full_data
             join leave_table 
             on full_data.actor_account_id = leave_table.actor_account_id
             and to_timestamp(leave_table.ts) between to_timestamp(full_data.time) and to_timestamp(full_data.time) + interval 10 seconds

             where full_data.LogName_EN = 'Die'

             group by 1

             ),

             broke_item_table as (

             select full_data.actor_account_id,
                    count(distinct leave_table.seq) as count_broke_item_tilt

             from full_data
             join leave_table 
             on full_data.actor_account_id = leave_table.actor_account_id
             and to_timestamp(leave_table.ts) between to_timestamp(full_data.time) and to_timestamp(full_data.time) + interval 10 seconds

             where full_data.LogName_EN = 'ResultOfTransform'
             and Code = 2

             group by 1

             ),

             lost_duel_pc_table as (

             select full_data.actor_account_id,
                    count(distinct leave_table.seq) as lost_duel_pc_tilt

             from full_data
             join leave_table 
             on full_data.actor_account_id = leave_table.actor_account_id
             and to_timestamp(leave_table.ts) between to_timestamp(full_data.time) and to_timestamp(full_data.time) + interval 10 seconds

             where full_data.LogName_EN = 'DuelEnd(PC)'
             and Code = 2

             group by 1

             ),

             lost_duel_team_table as (

             select full_data.actor_account_id,
                    count(distinct leave_table.seq) as lost_duel_team_tilt

             from full_data
             join leave_table 
             on full_data.actor_account_id = leave_table.actor_account_id
             and to_timestamp(leave_table.ts) between to_timestamp(full_data.time) and to_timestamp(full_data.time) + interval 10 seconds

             where full_data.LogName_EN = 'DuelEnd(Team)'
             and Code = 2

             group by 1

             ),

             tilt_table as (

                     select distinct(full_data.actor_account_id) as id,
                            coalesce(die_table.count_die_tilt, 0) as count_die_tilt,
                            coalesce(broke_item_table.count_broke_item_tilt, 0) as count_broke_item_tilt,
                            coalesce(lost_duel_pc_table.lost_duel_pc_tilt, 0) as lost_duel_pc_tilt,
                            coalesce(lost_duel_team_table.lost_duel_team_tilt, 0) as lost_duel_team_tilt

                     from full_data

                     left join die_table 
                     on full_data.actor_account_id = die_table.actor_account_id

                     left join broke_item_table 
                     on full_data.actor_account_id = broke_item_table.actor_account_id

                     left join lost_duel_pc_table 
                     on full_data.actor_account_id = lost_duel_pc_table.actor_account_id

                     left join lost_duel_team_table 
                     on full_data.actor_account_id = lost_duel_team_table.actor_account_id

             )

             select *

             from tilt_table

             ;""").show(50, truncate=False)