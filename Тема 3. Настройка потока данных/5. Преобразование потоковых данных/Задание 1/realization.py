from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
 
# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
 
# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}
 
def spark_init() -> SparkSession:
 
    return (
        SparkSession
            .builder
            .master('local[*]')
            .appName("test")
            .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .getOrCreate()
    )
    
 
 
def load_df(spark: SparkSession) -> DataFrame:
 
    msg_value_schema = T.StructType([
        T.StructField('client_id', T.StringType(), True),
        T.StructField('timestamp', T.DoubleType(), True),
        T.StructField('lat', T.DoubleType(), True),
        T.StructField('lon', T.DoubleType(), True),
        ])
 
    df = (
        spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .option('kafka.security.protocol', 'SASL_SSL')
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
            .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
            .option("failOnDataLoss", False)
            .option("startingOffsets", "latest")
            .option('subscribe', 'student.topic.cohort12.msdelendik')
            .load()
            .select(
                'key',
                'value',
                'topic',
                'partition',
                'offset',
                'timestamp',
                'timestampType',
                )
        )
    return df
 
 
def transform(df: DataFrame) -> DataFrame:
 
    msg_value_schema = T.StructType([
        T.StructField('client_id', T.StringType(), True),
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('lat', T.DoubleType(), True),
        T.StructField('lon', T.DoubleType(), True),
        ])
 
    df = df.select(F.col('value').cast('string'))
 
    df = (
        df
            .withColumn('value', F.from_json(col=F.col('value'), schema=msg_value_schema))
            .select(
                F.col('value.client_id').alias('client_id'),
                F.col('value.timestamp').alias('timestamp'),
                F.col('value.lat').alias('lat'),
                F.col('value.lon').alias('lon'),
            )
    )
    return df
 
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 pyspark-shell'
 
spark = spark_init()
 
source_df = load_df(spark)
output_df = transform(source_df)
 
query = (
    output_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(once=True)
        .start()
    )
try:
    query.awaitTermination()
finally:
    query.stop()
