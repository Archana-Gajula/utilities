from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()
data = [("Archana", "crn1"),
        ("Arun", "crn2"),
        ("Paddu", "crn3"),
        ("charan", "crn4")]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("crn", StructType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show()
