from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Big Data Analysis Example") \
        .getOrCreate()

    # Load a large dataset
    df = spark.read.csv("students.csv", header=True, inferSchema=True)

    # Show total number of records (demonstrates scalability)
    total_records = df.count()
    print(f"Total records: {total_records}")

    # Example Analysis 1: Highest marks
    print("Top 3 students by marks:")
    df.orderBy(desc("marks")).show(3)

    # Example Analysis 2: Average marks
    avg_marks = df.agg(avg(col("marks"))).first()[0]
    print(f"Average marks: {avg_marks:.2f}")

    # Example Analysis 3: Average marks by gender
    print("Average marks by gender:")
    df.groupBy("gender").agg(avg("marks").alias("avg_marks")).show()

    # Save insights to CSV
    df.orderBy(desc("marks")).coalesce(1).write.csv("output/top_students", header=True, mode="overwrite")
    df.groupBy("gender").agg(avg("marks").alias("avg_marks")).coalesce(1).write.csv("output/avg_marks_by_gender", header=True, mode="overwrite")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()