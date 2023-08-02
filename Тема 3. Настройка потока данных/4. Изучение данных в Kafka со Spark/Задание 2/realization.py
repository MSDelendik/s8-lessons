from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import pyspark.sql.types as T

#from kafka import KafkaConsumer
import multiprocessing
import json

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0" ]
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
                

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# def spark_init() -> SparkSession:
spark = SparkSession.builder\
    .master('local[*]')\
    .appName('test connect to kafka')\
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0")\
    .config("spark.jars.packages", spark_jars_packages)\
    .getOrCreate()



# def load_df(spark: SparkSession) -> DataFrame:
value_schema = T.StructType([
        T.StructField('subscription_id', T.IntegerType(), True),
        T.StructField('name', T.StringType(), True),
        T.StructField('description', T.StringType(), True),
        T.StructField('price', T.DoubleType(), True),
        T.StructField('currency', T.StringType(), True)
        ])

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")\
    .options(**kafka_security_options)\
    .option("failOnDataLoss", False)\
    .option("subscribe", "persist_topic")\
    .load()\
    .select(
        f.col('key').cast('string'),
        f.col('value').cast('string'),
        'topic',
        'partition',
        'offset',
        'timestamp',
        'timestampType',
            )\
    .withColumn('value', f.from_json(col=f.col('value'), schema=value_schema))\
    .select (
        f.col('key').cast('string'),
        f.col('value').cast('string'),
        'value.subscription_id',
        'value.name',
        'value.description',
        'value.price',
        'value.currency',
        'topic',
        'partition',
        'offset',
        'timestamp',
        'timestampType',
    )
 

df.printSchema()
df.show(truncate=False)
 
# def transform(df: DataFrame) -> DataFrame:
#     DataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# spark = spark_init()

# source_df = load_df(spark)
# df = transform(source_df)


