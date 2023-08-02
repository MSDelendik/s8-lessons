from pyspark.sql import SparkSession

spark = (
        SparkSession.builder.appName("test_name")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
    )

df = spark.read\
        .format('jdbc')\
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('schema', 'public') \
        .option('dbtable', 'marketing_companies') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()

df.count()