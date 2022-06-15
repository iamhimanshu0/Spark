from pyspark.sql import SparkSession

# creating spark seasion
spark = SparkSession.builder.appName("Socket Example").master('local[*]').getOrCreate()

def run_socket(host, port, spark):
    # loading/reading data from socket
    stream_df = spark.readStream.format('socket').option("host",host).option("port",port).load()

    # check if still streaming
    print(stream_df.isStreaming)
    stream_df.printSchema()

    # write stream in console
    write_query = stream_df.writeStream.format('console').start()

    # wait until socket is closed
    write_query.awaitTermination()

    print("Application Completed....")


if __name__ == "__main__":
    print("Application Started....")
   
    # calling the function
    run_socket("localhost","1100",spark)