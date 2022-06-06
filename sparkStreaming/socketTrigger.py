from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    print("Word Count Trigger Application....")

    spark = SparkSession.builder.appName("Word Count Trigger Application").master("local[*]").getOrCreate()

    stream_df = spark.readStream.format("socket").option("host","localhost").option("port","1100").load()

    stream_df.printSchema()

    # splitting the data in space seperator 
    stream_words_df = stream_df.select(explode(split("value", " ")).alias("word"))

    # count the word count
    stream_word_count_df = stream_words_df.groupBy("word").count()

    # writing the query 
    write_query = stream_word_count_df.writeStream.outputMode("complete").format("console").trigger(processingTime="2 second").start()
    """
        for output mode we have 3 options
        - append (by default) :- it will going to append data 
        - update -: it will going to update only the updated data or new data
        - complete :- it will going to update complete data
    """

    write_query.awaitTermination()

    print("Application Completed....")