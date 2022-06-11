from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Application Started.....")

    spark = SparkSession.builder.appName("CSV file streaming").master("local[*]").getOrCreate()

    # id, first_name, last_name, option
    input_csv_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("option", IntegerType(), True)
    ])

    # need to pass folder path in order to get stream
    stream_df = spark.readStream.format('csv').option("header","true").schema(input_csv_schema).load(path="dataFiles/csv")

    stream_df.printSchema()

    stream_df_query = stream_df.writeStream.format("console").start()

    # when ever the new file comes to csv file it will take it and stream it 
    stream_df_query.awaitTermination()

    print("Application Completed...")