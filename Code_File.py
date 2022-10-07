# Import libraries
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *

# Your code goes inside this function
# Function input - spark object, click data path, resolved data path
# Function output - final spark dataframe
def sample_function(spark, s3_clickstream_path, s3_login_path):
    df_clickstream =  spark.read.format("json").load(s3_clickstream_path)
    user_mapping =  spark.read.format("csv").option("header",True).load(s3_login_path)

    df_clickstream = df_clickstream.withColumn("current_page_url", col("client_side_data.current_page_url"))

    table = df_clickstream.join(user_mapping, (user_mapping["session_id"] == df_clickstream["session_id"])&(to_date(user_mapping["login_date_time"]) == to_date(df_clickstream["event_date_time"])), "left").drop(user_mapping["session_id"])
    table = table.fillna("unregistered", subset=["user_id"])
    table = table.withColumn("current_date", substring(col("event_date_time"),1,10))
    table = table.withColumn("login_date", to_date("login_date_time"))
    table = table.withColumn("logged_in", when(table["user_id"] == "unregistered", 0).otherwise(1))
    table = table.withColumn("Click", when(table["event_type"] == "click", 1).otherwise(0))
    table = table.withColumn("Pageload", when(table["event_type"] == "pageload", 1).otherwise(0))

    url = table.groupBy("current_date", "user_id", "session_id", "browser_id").agg(min(to_timestamp("event_date_time")).alias("login"))
    url = url.join(table.select("event_date_time","current_page_url"), ((url["login"] == table["event_date_time"]) &(url["user_id"]== table["user_id"]) &(url["session_id"] == table["session_id"]) &(url["browser_id"]==table["browser_id"])), "left")
    url = url.withColumnRenamed("current_page_url", "first_url")
    
    table = table.join(url,((table["current_date"] == url["current_date"])& (table["user_id"]== url["user_id"])&(url["session_id"]== table["session_id"])&(url["browser_id"]==table["browser_id"])), "left").drop(url["current_date"]).drop(url["user_id"]).drop(url["event_date_time"]).drop(url["session_id"]).drop(url["browser_id"])
    
    grouped = table.groupBy("current_date","browser_id","user_id","logged_in","first_url").agg(sum(table["Click"]).alias("number_of_clicks"), sum(table["Pageload"]).alias("number_of_pageloads"))
    
    grouped = grouped.replace("unregistered", None, subset = ["user_id"])
    
    df_union = grouped.select("current_date","browser_id","user_id","logged_in","first_url","number_of_clicks","number_of_pageloads")
    
    df_union.createOrReplaceTempView("df_union_tbl")
    
    df_result = spark.sql("select * from df_union_tbl")
		
    return df_result

session = SparkSession.builder.appName("firstsql").master("local").getOrCreate()
s3_clickstream_path = "jobathon_click_data.json"
s3_login_path = "jobathon_login_data.csv"
sample_function(session, s3_clickstream_path, s3_login_path).count()