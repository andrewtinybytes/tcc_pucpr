# %%
import shutil
import os
import pandas as pd
import gzip
import pyspark

from tqdm import tqdm

from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.master('local[*]').appName('default').getOrCreate()
sc = spark.sparkContext

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_PATH, 'data')
EXTRACT_PATH = os.path.join(BASE_PATH, 'extracted_data')
EXTRACT_CSV_PATH = os.path.join(BASE_PATH, 'extracted_csv')
PLAYERS_ACTIONS_PATH = os.path.join(BASE_PATH, 'players_actions')
LABELS_PATH = os.path.join(BASE_PATH, 'labels')

for filename in os.listdir(DATA_PATH):
    
    if 'label' in filename:
        
        print('label')

    elif 'traindata' in filename:
        
        shutil.unpack_archive(os.path.join(DATA_PATH, filename), EXTRACT_PATH)

for filename in tqdm(os.listdir(EXTRACT_PATH)):

        df = spark.read.option("header", "true").csv(os.path.join(EXTRACT_PATH, filename))

        df.write.mode("overwrite").parquet(path=os.path.join(PLAYERS_ACTIONS_PATH, f"{str(filename).replace('.csv', '').replace('.gz', '')}.parquet"))