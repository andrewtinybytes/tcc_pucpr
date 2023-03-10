# %%
import os
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FULL_DATA_PATH = os.path.join(BASE_PATH, 'full_data')
LABELS_PATH = os.path.join(BASE_PATH, 'labels')

conf = SparkConf().setAppName("tilt_agg") \
                  .setMaster("local[*]") \
                  .set("spark.executor.cores", "2") \
                  .set("spark.executor.memory", '2g') \
                  .set("spark.driver.memory", '2g') \
              #     .set("spark.sql.warehouse.dir", "hdfs://namenode/sql/metadata/hive") \
              #     .set("spark.sql.catalogImplementation","hive")

spark = SparkSession.builder \
                    .config(conf=conf) \
                    .enableHiveSupport() \
                    .getOrCreate()

df = spark.read.format("parquet").load(os.path.join(FULL_DATA_PATH, 'consolidated.parquet'))
df.createOrReplaceTempView('full_data')

labels = spark.read.format("csv").option("header","true").load(os.path.join(LABELS_PATH, 'train_labeld.csv'))
labels.createOrReplaceTempView('labels')


# %%

spark.sql('drop table if exists ranked_dates;')

spark.sql("""

create table ranked_dates as 
with max_table as (

       select actor_account_id, 
              date(max(to_timestamp(time))) last_date

       from full_data

       group by 1

),

distinct_dates_table as (

       select max_table.actor_account_id, 
              date(to_timestamp(full_data.time)) as distinct_date

       from max_table

       join full_data 
       on max_table.actor_account_id = full_data.actor_account_id

),

ranked_dates_table as (

       select actor_account_id,
              distinct_date,
              rank() over (partition by actor_account_id order by actor_account_id, distinct_date desc) rank_number
              
       from distinct_dates_table

       group by 1,
                2

)

select *
from ranked_dates_table

""").show(10, truncate=False)

# %%

spark.sql('drop table if exists tilt_agg;')


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

             windows_table as (


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

             activity_table as (

             select full_data.actor_account_id,
                    (max(unix_timestamp(to_timestamp(time))) - min(unix_timestamp(to_timestamp(time)))) / 60.0 minutes_played,
                    count(full_data.seq) as activity_count

             from full_data
              
             group by 1

             ),

             tilt_table as (

                     select full_data.actor_account_id,
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

                     group by 1,
                              2,
                              3,
                              4,
                              5

             )

             select tilt_table.actor_account_id,
                    tilt_table.count_die_tilt,
                    tilt_table.count_broke_item_tilt,
                    tilt_table.lost_duel_pc_tilt,
                    tilt_table.lost_duel_team_tilt,
                    activity_table.minutes_played,
                    activity_table.activity_count,
                    labels.churn_yn,
                    labels.survival_time

             from tilt_table

             join labels 
             on tilt_table.actor_account_id = labels.actor_account_id

             join activity_table
             on tilt_table.actor_account_id = activity_table.actor_account_id

             ;""").show(50, truncate=False)
# %%

spark.sql("drop table if exists frequency_table;")

spark.sql("""

create table frequency_table as 
select actor_account_id, 
       count(distinct date(to_timestamp(time))) as frequency_days,
       count(distinct date_trunc('month', to_timestamp(time))) as frequency_months

from full_data 

group by 1
;""").show()


# %%
# Exportar para CSV dentro da pasta

df_out = spark.sql('SELECT * FROM tilt_agg;')
df_out.toPandas().to_csv('out.csv', index=False)