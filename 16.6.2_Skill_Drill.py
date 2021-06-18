# 16.6.2_Skill_Drill.ipynb
# https://colab.research.google.com/drive/1qqQszPc3p6hMCu-RmpsCoKjSzLihvOrt?authuser=1#scrollTo=8UwOuDhqdTOc

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
spark = SparkSession.builder.appName("Tokens_StopWords").getOrCreate()

from pyspark.ml.feature import Tokenizer

# Create sample DataFrame showing pre-tokenized data,
dataframe = spark.createDataFrame([
                                   (0, "I choose a lazy person to do a hard job"),
                                   (1, "because a lazy person will find an easy way to do it")
], ["id", "sentence"])
dataframe.show(truncate=False)

# Tokenize sentences
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenizer

# Transform and show DataFrame
tokenized_df = tokenizer.transform(dataframe)
# Show results without shortening the output
tokenized_df.show(truncate=False)

# Import stop words library
from pyspark.ml.feature import StopWordsRemover

# Run the Remover
remover = StopWordsRemover(inputCol="words", outputCol="filtered")

# Transform and show data
remover.transform(tokenized_df).show(truncate=False)