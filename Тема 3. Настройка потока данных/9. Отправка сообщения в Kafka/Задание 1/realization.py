from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
import pyspark.sql.types as T
import os
from math import pi
 
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 pyspark-shell'

TOPIC_NAME_91 = 'student.topic.cohort12.msdelendik.out'  # Это топик, в который Ваше приложение должно отправлять сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>.out
TOPIC_NAME_IN = 'student.topic.cohort12.msdelendik' # Это топик, из которого Ваше приложение должно читать сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>


spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0"

def spark_init(test) -> SparkSession:
    return (
        SparkSession
            .builder
            .master('local[*]')
            .appName(test)
            .config("spark.jars.packages",  spark_jars_packages)
            .getOrCreate()
    )

postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    marketing_df = spark.read\
        .format('jdbc')\
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('schema', 'public') \
        .option('dbtable', 'marketing_companies') \
        .options(**postgresql_settings) \
        .load()
    
    return marketing_df


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}

def get_dist(lat2, lat, lng2, lng):
    d =  12742000 
    return d * f.asin(f.sqrt(
                          f.pow(f.sin((f.col(lat2)*pi/180 - f.col(lat)*pi/180)/f.lit(2)),2)+
                          f.cos(f.col(lat)*pi/180)*f.cos(f.col(lat2)*pi/180)*
                          f.pow(f.sin((f.col(lng2)*pi/180 - f.col(lng)*pi/180)/f.lit(2)),2)))

def read_client_stream(spark: SparkSession) -> DataFrame:
    msg_value_schema = T.StructType([
        T.StructField('client_id', T.StringType(), True),
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('lat', T.DoubleType(), True),
        T.StructField('lon', T.DoubleType(), True)
        ])
 
    user_df = (
        spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            # .option('kafka.security.protocol', 'SASL_SSL')
            # .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
            # .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
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
    user_df = user_df.select(f.col('value').cast('string'))
 
    user_df = (
        user_df
            .withColumn('value', f.from_json(col=f.col('value'), schema=msg_value_schema))
            .select(
                f.col('value.client_id').alias('client_id'),
                f.col('value.timestamp').alias('timestamp'),
                f.col('value.lat').alias('lat'),
                f.col('value.lon').alias('lon')
            )
    ).dropDuplicates(['client_id', 'timestamp']).withWatermark('timestamp', '10 minutes')
    return user_df


def join(user_df, marketing_df) -> DataFrame:
    df_result = (user_df.crossJoin(marketing_df)
    .withColumn("adv_campaign_id", marketing_df.id)
            .withColumn("adv_campaign_description", marketing_df.description)
            .withColumn("adv_campaign_start_time", marketing_df.start_time)
            .withColumn("adv_campaign_end_time", marketing_df.end_time)
            .withColumn("adv_campaign_point_lat", marketing_df.point_lat)
            .withColumn("adv_campaign_point_lon", marketing_df.point_lon)
            .withColumn("client_id", f.substring('client_id', 0, 6))
            .withColumn("created_at", f.lit(datetime.now()))
            .withColumn("a", (
                f.pow(f.sin(f.radians(marketing_df.point_lat - user_df.lat) / 2), 2) +
                f.cos(f.radians(user_df.lat)) * f.cos(f.radians(marketing_df.point_lat)) *
                f.pow(f.sin(f.radians(marketing_df.point_lon - user_df.lon) / 2), 2)))
            .withColumn("distance", (f.atan2(f.sqrt(f.col("a")), f.sqrt(-f.col("a") + 1)) * 12742000))
            .withColumn("distance", f.col('distance').cast(T.IntegerType()))
            .where(f.col("distance") <= 1000)
            .dropDuplicates(['client_id', 'adv_campaign_id'])
            .withWatermark('timestamp', '1 minutes')
            .select('client_id',
                    'distance',
                    'adv_campaign_id',
                    'adv_campaign_description',
                    'adv_campaign_start_time',
                    'adv_campaign_end_time',
                                        'adv_campaign_point_lat',
                                        'adv_campaign_point_lon',
                    'created_at'))
    
    return df_result

def serialize(df_result):
    df= (df_result
    .withColumn('value', f.to_json(
        f.struct(f.col('client_id'), f.col('distance'), f.col('adv_campaign_id'), f.col('adv_campaign_description'), f.col('adv_campaign_start_time'),
                 f.col('adv_campaign_end_time'),f.col('adv_campaign_point_lat'),f.col('adv_campaign_point_lon'),f.col('created_at'), ))))
    return df

def run_query(df):
    return (df
            .writeStream
            .outputMode("append")
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option("topic", TOPIC_NAME_91)
            .option("checkpointLocation", "test_query")
            .trigger(processingTime="1 minute")
            .start())


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    output = join(client_stream, marketing_df)
    ser_output = serialize(output)
    query = run_query(ser_output)

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()
    