from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as sql_functions

APP_NAME = "Granify"
FEATURES_FILE = "features.gz"
ORDERS_FILE = "orders.gz"
SESSIONS_FILE = "sessions.gz"


def split_ssid(data_frame):
    return data_frame.select(sql_functions.split(data_frame.ssid, ":"))


if __name__ == "__main__":
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    sessions = spark.read.json(SESSIONS_FILE)
    features = spark.read.json(FEATURES_FILE)
    orders = spark.read.json(ORDERS_FILE)
    # Dumb debug prints to show that we are actually reading the files and have
    # Spark running.
    print("Sessions count: %s" % sessions.count())
    print(sessions.first())
    print("Features count: %s" % features.count())
    print(features.first())
    print("Orders count: %s" % orders.count())
    print(orders.first())
    sample = split_ssid(sessions).first()[0]
    print("Sample split ssid: %s" % sample)
    user_id = sample[0]
    site_id = sample[1]
    session_time = datetime.fromtimestamp(int(sample[2]))
    print("User id: %s, Site id: %s, Time: %s" % (user_id, site_id,
                                                  session_time.isoformat()))

    joined_sessions = sessions.join(
        orders, sessions.ssid == orders.ssid, "left").join(
            features, sessions.ssid == features.ssid, "left")
    select_features = joined_sessions.select(
        sql_functions.from_unixtime(sessions.st).alias("start_time"),
        sql_functions.split(sessions.ssid, ":")[1].alias("site_id"),
        sessions.gr, features.ad, sessions.browser, orders.revenue)
    print(select_features.show())

    summary = select_features.groupBy(
        sql_functions.window(select_features.start_time, "1 hours"),
        select_features.site_id, select_features.gr, select_features.ad,
        select_features.browser).agg(
            sql_functions.sum(select_features.revenue),
            sql_functions.count(
                select_features.start_time).alias("Number of sessions"))
    print(summary.show())
