from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
import pyspark.sql.types as T
import os
from math import pi
 
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 pyspark-shell'

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
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
}


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
    df_result = user_df.crossJoin(marketing_df)
    return df_result


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()