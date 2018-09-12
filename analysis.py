import itertools
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as sql_functions

APP_NAME = "Granify"
FEATURES_FILE = "features.gz"
ORDERS_FILE = "orders.gz"
SESSIONS_FILE = "sessions.gz"


def process_aggregations(select_features):
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


def process_statistics(select_features):
    grouped_features = select_features.groupBy(select_features.site_id,
                                               select_features.ad)
    mean_stddev_fns = \
        [(sql_functions.mean(select_features[feature_name]).alias(
            '%s_mean' % feature_name),
          sql_functions.stddev_pop(select_features[feature_name]).alias(
              '%s_stddev' % feature_name)) for feature_name in
         ['feature_1', 'feature_2', 'feature_3', 'feature_4']]
    report = grouped_features.agg(*list(itertools.chain(*mean_stddev_fns)))
    report.coalesce(1).write.csv(
        "output2", sep="\t", mode="overwrite", header="true")


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

    process_aggregations(select_features)

    select_features = joined_sessions.select(
        site_id_fn, features.ad, features['feature-1'].alias("feature_1"),
        features['feature-2'].alias("feature_2"),
        features['feature-3'].alias("feature_3"),
        features['feature-4'].alias("feature_4")).filter(
            joined_sessions.ad.isNotNull())

    process_statistics(select_features)
