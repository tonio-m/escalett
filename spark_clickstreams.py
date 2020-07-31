import os
import json
import errno
import shutil
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    LongType,
    FloatType,
    StringType,
    StructType,
    StructField,
    IntegerType
)

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("EscaleTT") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()

df = spark \
    .read \
    .json("/user/hadoop/data-engineer-test/part-00*.json.gz") \
    .withColumn("file",f.input_file_name()) \
    .withColumn("device_sent_timestamp", f.substring(f.col("device_sent_timestamp"),0,10)) \
    .withColumn("device_sent_timestamp", f.from_unixtime(f.col("device_sent_timestamp")))

df = df.withColumn("session_timestamp", f.date_trunc("hour","device_sent_timestamp")) 

session_cols = [
    f.col("os_family"),
    f.col("device_family"),
    f.col("browser_family"),
    f.col("session_timestamp") 
]
df = df.withColumn("session_id", f.sha2(f.concat(*session_cols),256))

stage1 = df \
    .groupBy(f.col("file")) \
    .agg(f.countDistinct(f.col("session_id")).alias("unique_sessions")) \
    .rdd.collectAsMap()


def unique_sessions(family):
    return df \
    .groupBy(f.col(family)) \
    .agg(f.countDistinct(f.col("session_id")).alias("unique_sessions")) \
    .rdd.collectAsMap()


stage2 = {
    "os_family": unique_sessions("os_family"),
    "device_family": unique_sessions("device_family"),
    "browser_family": unique_sessions("browser_family")
} 

session_duration = f.unix_timestamp(f.col("session_end")) - f.unix_timestamp(f.col("session_start"))
session_start_and_end = ( f.min("device_sent_timestamp").alias("session_start"), f.max("device_sent_timestamp").alias("session_end") )

df2 = df.groupBy(f.col("session_id")) \
    .agg(*session_start_and_end) \
    .withColumn("session_duration", session_duration)

df = df.join(df2,"session_id")


def median_session_duration(family):
    return df \
    .groupBy(f.col("os_family")) \
    .agg(f.expr('percentile_approx(session_duration, 0.5)').alias("median_duration")) \
    .rdd.collectAsMap()


stage3 = {
    "os_family": median_session_duration("os_family"),
    "browser_family": median_session_duration("browser_family"),
    "device_family": median_session_duration("device_family")
}


def safe_write_json(dict_,path):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise exc
    with open(path,"w") as f:
        json.dump(dict_,f)


safe_write_json(stage1,"sessions/unique/by_file.json")
safe_write_json(stage2,"sessions/unique/by_family.json")
safe_write_json(stage3,"sessions/median/by_family.json")
