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
    print("Session count: %s" % sessions.count())
    print("Features count: %s" % features.count())
    print("Orders count: %s" % orders.count())
    print("Sample split ssid: %s" % split_ssid(sessions).first())
