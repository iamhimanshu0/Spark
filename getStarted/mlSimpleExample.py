"""
# https://www.guru99.com/pyspark-tutorial.html

Following are the steps to build a Machine Learning program with PySpark:

Step 1) Basic operation with PySpark
Step 2) Data preprocessing
Step 3) Build a data processing pipeline
Step 4) Build the classifier: logistic
Step 5) Train and evaluate the model
Step 6) Tune the hyperparameter

"""

from email import header
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkFiles

sc= SparkContext()

# STEP 1:
url = "https://raw.githubusercontent.com/guru99-edu/R-Programming/master/adult_data.csv"
sc.addFile(url)
sqlcontext = SQLContext(sc)

"""
# read csv file ->
You use inferSchema set to True to tell Spark to guess automatically 
the type of data. By default, it is turn to False.
"""

df = sqlcontext.read.csv(SparkFiles.get('/home/himanshu/Desktop/sparkLearn/getStarted/data/adult_data.csv'),
                            header=True, inferSchema=True)

# df.printSchema()
# df.show(5, truncate=False)

# Selecting columns
# df.select('age','fnlwgt').show(5)

# count by group (groupby(), count())
"""
together. In the PySpark example below, you count the number of rows by the education level.
"""
# df.groupBy("education").count().sort("count", ascending=True).show()

"""
Describe the data
To get a summary statistics, of the data, you can use describe(). It will compute the :

    count
    mean
    standarddeviation
    min
max
"""

df.describe().show()