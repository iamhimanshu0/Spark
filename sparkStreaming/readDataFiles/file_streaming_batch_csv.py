from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Application Started.....")

    spark = SparkSession.builder.appName("CSV file streaming Batch").master("local[*]").getOrCreate()

    # id, first_name, last_name, option
    input_csv_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("option", IntegerType(), True)
    ])

    # need to pass folder path in order to get stream
    batch_df = spark.read.format('csv').option("header","true").schema(input_csv_schema).load(path="dataFiles/csv")

    batch_df.printSchema()

    batch_df.show()

    print("Application Completed...")