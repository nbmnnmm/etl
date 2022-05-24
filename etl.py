from pyspark.sql import SparkSession 
from pyspark import SparkConf
from pyspark.sql.types import FloatType , IntegerType
from pyspark.sql.functions import mean , col , concat_ws , round
import os 
conf = SparkConf()
path = os.getenv("PATH_POSTGRES_DRIVER")
print(path)
conf.set("spark.jars", path) 
username = "myuser"
spark = SparkSession.builder.config(conf=conf).appName("connect postgresql").getOrCreate()

table = 'test1'
port = 5432
server = "localhost"
username = "myuser"
password = "mypass"
db = "mydb"





def extract(spark):
    path = os.getenv("PATH_FILE_CSV")
    df1 = spark.read.option('inferSchema',True).option('header' , True).csv(path).select('year','month','dayofmonth','origin','dest','distance','carrier_name','airtime','depdelay','origin_city','dest_city','cancelled')
    return df1

def tranform(df):
     # chuyển đổi kiểu dữ liệu về dạng đúng
        df = df.withColumn("airtime",df["airtime"].cast(FloatType()))\
        .withColumn("depdelay",df["depdelay"].cast(IntegerType()))
    # điền giá trị còn thiều bằng giá trị mean
        mean_airtime = df.select(round(mean(df['airtime']),2)).collect()[0][0]
        df = df.fillna(value=mean_airtime, subset=['airtime'])\
        .fillna(value=0, subset=['depdelay'])
# hợp nhất các cốt thứ ngày tháng 
        df = df.withColumn("date",concat_ws("-",col("year"),col("month"),col("dayofmonth")).cast("date"))\
        .drop('year', 'month','dayofmonth')\
        .select('origin','dest','date','distance','carrier_name','airtime','depdelay','origin_city','dest_city','cancelled')
        return df 
    

def load(df):
    df = df.write \
    .format("jdbc") \
    .option(f"url", "jdbc:postgresql://192.168.1.101:5432/mydb") \
    .option("dbtable","etl2") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .mode('overwrite')\
    .save()


if __name__ == "__main__":
    df = extract(spark=spark)
    df = tranform(df)
    load(df)
