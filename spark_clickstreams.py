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


urls = [
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00000.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00001.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00002.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00003.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00004.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00005.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00006.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00007.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00008.json.gz",
        "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00009.json.gz"
]

for url in urls:
    spark.sparkContext.addFile(url)

datapath = SparkFiles.get("")
datapath = shutil.move(datapath, "/home/hadoop/")

schema = StructType([
            StructField("n", IntegerType(), True),
            StructField("event", StringType(), True),
            StructField("version", FloatType(), True),
            StructField("platform", StringType(), True),
            StructField("os_family", StringType(), True),
            StructField("anonymous_id", StringType(), True),
            StructField("device_family", StringType(), True),
            StructField("browser_family", StringType(), True),
            StructField("device_sent_timestamp", StringType(), True)
    ]
)

df = spark \
    .read \
    .schema(schema) \
    .json(datapath) \
    .withColumn("file",f.input_file_name()) \
    .withColumn("device_sent_timestamp", f.substring(f.col("device_sent_timestamp"),0,10)) \
    .withColumn("device_sent_timestamp", f.from_unixtime(f.col("device_sent_timestamp")))


# FAZENDO SESSIONAMENTO
# IMPORTANTE: arredondei o tempo máximo de sessão pra 1 hora, ao invés de 30 minutos
df = df.withColumn("session_timestamp", f.date_trunc("hour","device_sent_timestamp")) 

session_cols = [
    # f.col("anonymous_id"), # IMPORTANTE:  anonymous_id gerava um hash único pra cada linha, decidi dropar
    # fingerprinting está sendo feito por User-Agent e timestamp
    f.col("os_family"),
    f.col("device_family"),
    f.col("browser_family"),
    f.col("session_timestamp") 
]
df = df.withColumn("session_id", f.sha2(f.concat(*session_cols),256))

# ETAPA 1
stage1 = df \
.groupBy(f.col("file")) \
.agg(f.countDistinct(f.col("session_id")).alias("unique_sessions")) \
.rdd.collectAsMap()

# ETAPA 2
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

# ETAPA 3
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

stage1,stage2,stage3
