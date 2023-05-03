from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from test.config.ConfigStore import *
from test.udfs.UDFs import *

def customer_totals(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("full_name", StringType(), True), StructField("amount", LongType(), True), StructField("customer_id", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .option("ignoreLeadingWhiteSpace", True)\
        .option("ignoreTrailingWhiteSpace", True)\
        .csv("dbfs:/Prophecy/jagmeet@prophecy.io/CustomerTotals.csv")
