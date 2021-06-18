# 16.6.1: tokenizing.ipynb  (pyspark, output words)
# https://colab.research.google.com/drive/1j5nQa7zfdi-kdlR2KsUV7afDTSX_kpyt?authuser=1

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
spark = SparkSession.builder.appName("Tokens").getOrCreate()

from pyspark.ml.feature import Tokenizer

# Create sample DataFrame showing pre-tokenized data,
dataframe = spark.createDataFrame([
                                   (0, "Spark is great"),
                                   (1, "We are learning Spark"),
                                   (2, "Spark is better than hadoop no doubt")
], ["id", "sentence"])
dataframe.show()

# Tokenize sentences
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenizer

# Transform and show DataFrame
tokenized_df = tokenizer.transform(dataframe)
# Show results without shortening the output
tokenized_df.show(truncate=False)

# Create a function to return the length of a list
def word_list_length(word_list):
  return len(word_list)
  
# Import column function to select a column to be passed into a function
from pyspark.sql.functions import col, udf
# Import InterType to define the data type of the output
from pyspark.sql.types import IntegerType

# Create a user defined function
count_tokens = udf(word_list_length, IntegerType())

# Create our Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenizer

# Transform and show DataFrame
tokenized_df = tokenizer.transform(dataframe)
# Show results without shortening the output
tokenized_df.show(truncate=False)

# Select the needed columns and don't truncate results
tokenized_df.withColumn("tokens", count_tokens(col("words"))).show(truncate=False)

