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

spark.sql("drop table if exists time_series_table;")


spark.sql("""

create table time_series_table as
with max_date_table as (select actor_account_id,
                               max(date(to_timestamp(full_data.time))) as max_date
                        from full_data
                        group by 1),
     dates_cte as (select actor_account_id,
                          date_sub(max_date, seq) as date
                   from max_date_table lateral view explode(sequence(1, datediff(max_date, date_sub(max_date, 28)))) as seq),
     cte as (select date(to_timestamp(full_data.time))                                                                as date,
                    actor_account_id,
                    count(distinct case when full_data.LogName_EN = 'Die' then seq end)                               as die_count,
                    count(distinct case
                                       when full_data.LogName_EN = 'LeaveWorld'
                                           then seq end)                                                              as leave_world_count,
                    count(distinct case
                                       when full_data.LogName_EN = 'ResultOfTransform' and full_data.Code = 2
                                           then seq end)                                                              as break_item_count,
                    count(distinct case
                                       when full_data.LogName_EN = 'DuelEnd(PC)' and full_data.Code = 2
                                           then seq end)                                                              as lost_duel_pc_count,
                    count(distinct case
                                       when full_data.LogName_EN = 'DuelEnd(Team)' and full_data.Code = 2
                                           then seq end)                                                              as lost_duel_team_count
             from full_data
             group by 1, 2),
     last_table as (select subquery.actor_account_id,
                           subquery.date,
                           row_number() over (partition by subquery.actor_account_id order by subquery.date asc) as rn,
                           cte.die_count,
                           cte.leave_world_count,
                           cte.break_item_count,
                           cte.lost_duel_pc_count,
                           cte.lost_duel_team_count,
                           case when cte.actor_account_id is null then true else false end                       as null_row

                    from (select dates_cte.actor_account_id, dates_cte.date
                          from dates_cte) subquery
                             left join cte
                                       on subquery.actor_account_id = cte.actor_account_id and subquery.date = cte.date)

select actor_account_id,
       date,
       rn,
       null_row,
       coalesce(last_value(die_count, true) over (partition by actor_account_id order by date asc),
                last_value(die_count, true) over (partition by actor_account_id order by date desc ))            as die_count,
       coalesce(last_value(leave_world_count, true) over (partition by actor_account_id order by date asc),
                last_value(leave_world_count, true)
                over (partition by actor_account_id order by date desc ))                                        as leave_world_count,
       coalesce(last_value(break_item_count, true) over (partition by actor_account_id order by date asc),
                last_value(break_item_count, true)
                over (partition by actor_account_id order by date desc ))                                        as break_item_count,
       coalesce(last_value(lost_duel_pc_count, true) over (partition by actor_account_id order by date asc),
                last_value(lost_duel_pc_count, true)
                over (partition by actor_account_id order by date desc ))                                        as lost_duel_pc_count,
       coalesce(last_value(lost_duel_team_count, true) over (partition by actor_account_id order by date asc),
                last_value(lost_duel_team_count, true)
                over (partition by actor_account_id order by date desc ))                                        as lost_duel_team_count

from last_table

group by 1, 2, 3, 4,
         die_count,
         leave_world_count,
         break_item_count,
         lost_duel_pc_count,
         lost_duel_team_count

order by rn;


""")