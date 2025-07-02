from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Big Data Analysis Example") \
        .getOrCreate()

    # Load large dataset (update path as needed)
    df = spark.read.csv("user_logs.csv", header=True, inferSchema=True)

    # Show total number of records (demonstrates scalability)
    total_records = df.count()
    print(f"Total records: {total_records}")

    # Example Analysis 1: Most popular actions
    action_counts = df.groupBy("action").count().orderBy(desc("count"))
    print("Action Counts:")
    action_counts.show()

    # Example Analysis 2: Most active users
    user_activity = df.groupBy("user_id").count().orderBy(desc("count")).limit(10)
    print("Top 10 Most Active Users:")
    user_activity.show()

    # Example Analysis 3: Actions by device type
    device_action = df.groupBy("device_type", "action").count().orderBy(desc("count"))
    print("Device Type - Action Counts:")
    device_action.show()

    # Save insights to CSV
    action_counts.coalesce(1).write.csv("output/action_counts", header=True, mode="overwrite")
    user_activity.coalesce(1).write.csv("output/top_users", header=True, mode="overwrite")
    device_action.coalesce(1).write.csv("output/device_action_counts", header=True, mode="overwrite")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()