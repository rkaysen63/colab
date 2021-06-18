# 16.4.2: spark_dataframes.ipynb
# # https://colab.research.google.com/drive/1JtOn_eEF0R1SAg-WYOkyDzEfNBerRhs8?authuser=1#scrollTo=l9NbTofeBEdF

import os
# Find the latest version of spark 3.0  from http://www-us.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.0.2'
spark_version = 'spark-3.<enter version>'
os.environ['SPARK_VERSION']=spark_version

# Install Spark and Java
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://www-us.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop2.7.tgz
!tar xf $SPARK_VERSION-bin-hadoop2.7.tgz
!pip install -q findspark

# Set Environment Variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop2.7"

# Start a SparkSession
import findspark
findspark.init()

# Start Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrameBasics").getOrCreate()

dataframe = spark.createDataFrame([
                                   (0, "Here is our DataFrame"),
                                   (1, "We are making one from scratch"),
                                   (2, "This will look very similar to a Pandas DataFrame")
],["id", "words"])
dataframe.show()

# Read in data from S3 Buckets
from pyspark import SparkFiles
url = "https://s3.amazonaws.com/dataviz-curriculum/day_1/food.csv"
spark.sparkContext.addFile(url)
# import data directly into a DataFrame
df = spark.read.csv(SparkFiles.get("food.csv"), sep=",", header=True)
df.show()

# Print our schema
df.printSchema()

# Show the columns
df.columns

# Describe our data
df.describe

# Import struct fields that we can use
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# Next we need to create the list of struct fields
schema = [StructField("food", StringType(), True), StructField("price", IntegerType(), True),]
schema

# Pass in our fields
final = StructType(fields=schema)
final

# Read our data with our new schema
dataframe = spark.read.csv(SparkFiles.get("food.csv"), schema=final, sep=",", header=True)
dataframe.printSchema()

dataframe['price']

type(dataframe['price'])

type(dataframe.select('price'))

dataframe.select('price').show()

# Add new column
dataframe.withColumn('newprice', dataframe['price']).show()

# Update column name
dataframe.withColumnRenamed('price','newerprice').show()

# Double the price
dataframe.withColumn('doubleprice',dataframe['price']*2).show()

# Add a dollar to the price
dataframe.withColumn('add_one_dollar',dataframe['price']+1).show()

# Half the price
dataframe.withColumn('half_price',dataframe['price']/2).show()

