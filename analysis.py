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

    joined_sessions = sessions.join(
        orders, sessions.ssid == orders.ssid, "left").join(
            features, sessions.ssid == features.ssid, "left")
    start_time_fn = sql_functions.from_unixtime(
        sessions.st).alias("start_time")
    user_id_fn = sql_functions.split(sessions.ssid, ":")[0].alias("user_id")
    site_id_fn = sql_functions.split(sessions.ssid, ":")[1].alias("site_id")
    select_features = joined_sessions.select(
        start_time_fn, user_id_fn, site_id_fn, sessions.ssid, sessions.gr,
        features.ad, sessions.browser, orders.revenue)

    time_window = sql_functions.window(select_features.start_time, "1 hours")
    grouped_features = select_features.groupBy(
        time_window, select_features.site_id, select_features.gr,
        select_features.ad, select_features.browser)
    revenue_fn = sql_functions.sum(select_features.revenue).alias("revenue")
    session_count_fn = sql_functions.count(
        select_features.ssid).alias("sessions")
    transaction_count_fn = sql_functions.count(
        sql_functions.when(select_features.revenue > 0,
                           1)).alias("transactions")
    conversion_count_fn = sql_functions.countDistinct(
        sql_functions.when(select_features.revenue > 0,
                           select_features.ssid)).alias("conversions")
    summary = grouped_features.agg(revenue_fn, session_count_fn,
                                   transaction_count_fn, conversion_count_fn)
    report = summary.select(
        (summary.window.start).alias("session_start"), summary.site_id,
        summary.gr, summary.ad, summary.browser,
        sql_functions.round(summary.revenue).alias("revenue"),
        summary.sessions, summary.transactions, summary.conversions)
    report.coalesce(1).write.csv(
        "output", sep="\t", mode="overwrite", header="true")
