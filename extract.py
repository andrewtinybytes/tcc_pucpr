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

    #if 'traindata' in filename and not filename.startswith('.'):
    if 'testdata' in filename and not filename.startswith('.'):
        
        shutil.unpack_archive(os.path.join(DATA_PATH, filename), EXTRACT_PATH)

    else:

        if not filename.startswith('.'):

            shutil.unpack_archive(os.path.join(DATA_PATH, filename), LABELS_PATH)

filepaths = [os.path.join(EXTRACT_PATH, f) for f in os.listdir(EXTRACT_PATH) if f != '.placeholder' and f.endswith('.csv.gz')]

for filename in tqdm(filepaths):

    df = spark.read.option("header", "true").csv(os.path.join(EXTRACT_PATH, filename))

    df.write.mode("overwrite").parquet(path=f"{str(filename).replace('.csv', '').replace('.gz', '')}.parquet")
    os.remove(filename)