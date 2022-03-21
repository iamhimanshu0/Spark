import pyspark
from pyspark import SparkContext

sc= SparkContext()

"""
Now that the SparkContext is ready, you can create a collection of data called RDD, 
Resilient Distributed Dataset. Computation in an RDD is automatically parallelized
across the cluster.
"""
nums = sc.parallelize([1,2,3,4])
# print(nums.take(2)) #accessing the values 

"""
You can apply a transformation to the data with a lambda function. 
In the PySpark example below, you return the square of nums. 
It is a map transformation
"""
squared = nums.map(lambda x:x*x).collect()
# print(squared)


# SQLContext
from pyspark.sql import Row
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

"""
let’s create a list of tuple. 
Each tuple will contain the name of the people and their age.

Four steps are required:
step 1:- create the list of tuple with information
step 2:- Build a RDD
step 3:- convert the tuples
step 4:- create DataFrame context
"""

# STEP 1:- Create the list of tuple with information
list_p = [('John',19),('Smith',29),('Adam',35),('Henry',50)]

# STEP 2:- Build a RDD
rdd =  sc.parallelize(list_p)

# STEP 3:- convert the tuples
ppl = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))

# STEP 4:- create a DataFrame context
sqlContext.createDataFrame(ppl)

df_ppl = sqlContext.createDataFrame(ppl)

df_ppl.show()
