###### TEDx-Load-Aggregate-Model

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, length

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

##### FROM FILES
tedx_dataset_path = "s3://unibg-data-2023/tedx_dataset.csv"

##### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

##### CLEAN TEDX DATASET (NULL IDX AND DUPLICATES)
print(f"TEDX DATASET NULL IDX count (PRE) {tedx_dataset.count()}")
tedx_dataset = tedx_dataset.filter(length("idx") == 32)
print(f"TEDX DATASET NULL IDX count (PRE) {tedx_dataset.count()}")

print(f"TEDX DATASET DUPLICATE count (PRE) {tedx_dataset.count()}")
tedx_dataset = tedx_dataset.dropDuplicates()
print(f"TEDX DATASET DUPLICATE count (POST) {tedx_dataset.count()}")

##### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

##### READ TAGS DATASET
tags_dataset_path = "s3://unibg-data-2023/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

##### CLEAN TAGS DATASET DATASET(NULL IDX AND DUPLICATES)
print(f"TAGS DATASET DUPLICATE count (PRE) {tags_dataset.count()}")
tags_dataset = tags_dataset.dropDuplicates()
print(f"TAGS DATASET DUPLICATE count (POST) {tags_dataset.count()}")

##### READ WATCH NEXT DATASET
watch_next_dataset_path = "s3://unibg-data-2023/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path)

##### CLEAN WATCH NEXT DATASET(NULL IDX AND DUPLICATES)
print(f"WATCH NEXT DATASET DUPLICATE count (PRE) {watch_next_dataset.count()}")
watch_next_dataset = watch_next_dataset.dropDuplicates()
print(f"WATCH NEXT DATASET DUPLICATE count (POST) {watch_next_dataset.count()}")

##### CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg_1 = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \

##### CREATE THE AGGREGATE MODEL, ADD WATCH_NEXT TO TEDX_DATASET
watch_next_dataset_agg_a  = watch_next_dataset.groupBy(col("watch_next_idx")).agg({'watch_next_idx':'count'}).withColumnRenamed("count(watch_next_idx)", "count_wn")
watch_next_dataset_agg_b = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("watch_next_idx").alias("watch_next_s"))
watch_next_dataset_agg_2 = watch_next_dataset_agg_a.join(watch_next_dataset_agg_b, watch_next_dataset_agg_a.watch_next_idx == watch_next_dataset_agg_b.idx_ref, "left")\
    .drop("watch_next_idx") \


tedx_dataset_agg_3 = tedx_dataset_agg_1.join(watch_next_dataset_agg_2, tedx_dataset_agg_1.idx == watch_next_dataset_agg_2.idx_ref, "left")\
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

##### MONGODB
mongo_uri = "mongodb+srv://dgirolamo:dgirolamo@mongodb.yqdpqw3.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2023",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg_3, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
