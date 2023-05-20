###### TEDx-Load-Aggregate-Model
####

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
tedx_dataset_path = "s3://bucket-dati-test1-2023/tedx_dataset.csv"

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

##### CLEAN TEDX DATASET
print(f"TOTAL DATASET IDX: {tedx_dataset.count()}")
tedx_dataset = tedx_dataset.filter(length("idx") == 32)     #### REMOVE INVALID IDX
print(f"DATASET without INVALID IDX: {tedx_dataset.count()}")

tedx_dataset = tedx_dataset.dropDuplicates()  #### REMOVE DUPLICATES
print(f"DATASET without DUPLICATES: {tedx_dataset.count()}")

##### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

##### READ TAGS DATASET
tags_dataset_path = "s3://bucket-dati-test1-2023/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

##### CLEAN TAGS DATASET DATASET
print(f"TOTAL TAGS DATASET: {tags_dataset.count()}")
tags_dataset = tags_dataset.dropDuplicates()    
#### REMOVE DUPLICATES
print(f"TAGS DATASET without DUPLICATES {tags_dataset.count()}")

##### READ WATCH NEXT DATASET
watch_next_dataset_path = "s3://bucket-dati-test1-2023/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path)

##### CLEAN WATCH NEXT DATASET
print(f"TOTAL WATCH NEXT DATASET: {watch_next_dataset.count()}")
watch_next_dataset = watch_next_dataset.dropDuplicates()    #### REMOVE DUPLICATES
print(f"WATCH NEXT DATASET without DUPLICATES {watch_next_dataset.count()}")

##### CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \

##### CREATE THE AGGREGATE MODEL, ADD WATCH_NEXT TO TEDX_DATASET
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("watch_next_idx").alias("watch_next"))


tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg.idx == watch_next_dataset_agg.idx_ref, "left")\
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

##### MONGODB
mongo_uri = "mongodb+srv://admin123:admin123@cluster0.i9ba1mf.mongodb.net"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2023",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
