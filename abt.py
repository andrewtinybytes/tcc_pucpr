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

#labels = spark.read.format("csv").option("header","true").load(os.path.join(LABELS_PATH, 'train_labeld.csv'))
labels = spark.read.format("csv").option("header","true").load(os.path.join(LABELS_PATH, 'test1_labeled.csv'))
labels.createOrReplaceTempView('labels')

#%%
spark.sql('drop table if exists tilt_agg_test1;')


spark.sql("""
             create table tilt_agg_test1 as
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

            --activity_table as (
            --
            --select full_data.actor_account_id,
            --       (max(unix_timestamp(to_timestamp(time))) - min(unix_timestamp(to_timestamp(time)))) / 60.0 minutes_played,
            --       count(full_data.seq) as activity_count
            --
            --from full_data
            -- 
            --group by 1
            --
            --),

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
                    --activity_table.minutes_played,
                    --activity_table.activity_count,
                    labels.churn_yn,
                    labels.survival_time

             from tilt_table

             join labels 
             on tilt_table.actor_account_id = labels.actor_account_id

             --join activity_table
             --on tilt_table.actor_account_id = activity_table.actor_account_id

             ;""").show(50, truncate=False)

# %%
# Exportar para CSV dentro da pasta

df_out = spark.sql('SELECT * FROM tilt_agg_test1;')
#df_out.toPandas().to_csv('tilt_agg_test1_1.csv', index=False)
#spark.sql('SELECT * FROM tilt_agg').write.option("header", "true").mode("overwrite").csv(os.path.join(BASE_PATH, 'out.csv'))
df_out.take(5).show()



# %% Criando uma tabela considerando periodicamente. Tabela 3D
# Fazer colunas 'tilt1', 'tilt2', 'tilt3', 'tilt4' assim por diante. Essas colunas são se teve tilt em determinado período. Por enquanto é a coluna 'period_activity'
# Deve-se primeiro analisar se todos os players tiveram o primeiro acesso no mesmo dia. Aí considerar o marco zero e usá-lo como referência no código.

spark.sql('drop table if exists tilt_agg_mod;')
#spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("""create table tilt_agg_mod
              with base_table as(
              
              select 
              time as ts, 
              to_date(date_format(time, 'yyyy-MM-dd')) as ts2,              
              --to_date('time', 'yyyy-MM-dd') as ts2,
              actor_account_id, 
              LogName_EN as action

              from full_data
              --where actor_account_id='00CFBAEA' and time like '%2016-07-13 14%'
              where actor_account_id='00CFBAEA' or actor_account_id='008B8315'
              

              )
              --SELECT SUM(CASE WHEN myColumn=1 THEN 1 ELSE 0 END)

              --SELECT *, IF(action IS NULL, 0, COUNTIF(action = 'LeaveWorld') OVER(period_activity_window)) AS period_activity
              SELECT *, IF(action IS NULL, 0, SUM(CASE WHEN action = 'LeaveWorld' THEN 1 ELSE 0 END) OVER(period_activity_window)) AS period_activity
              FROM base_table
              WINDOW period_activity_window AS (
                PARTITION BY actor_account_id 
                ORDER BY UNIX_DATE(ts2) 
                RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING
              )
              ORDER BY actor_account_id, ts2   


;""").show(50, truncate=False)

# %%
df_out=spark.sql('SELECT actor_account_id, MAX(period_activity) FROM tilt_agg_mod GROUP BY actor_account_id')
df_out.take(100)



# %%
