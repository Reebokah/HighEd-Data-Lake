from pyspark.sql import SparkSession

%%configure -f
{
    "conf":{
        "spark.jars.packages": "io.delta:delta-core_2.12:0.8.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.catalogImplementation": "hive"
    }}

df = spark.read.csv("s3://hied-delta-lake/raw/effy2019.csv", header=True)
df.write. format ("delta") .save("s3://hied-delta-lake/delta-tables/source/effy2019")

df = spark.read.csv("s3://hied-delta-lake/raw/effy2020.csv", header=True)
df.write. format ("delta") .save("s3://hied-delta-lake/delta-tables/source/effy2020")

df = spark.read.csv("s3://hied-delta-lake/raw/effy2021.csv", header=True)
df.write. format ("delta") .save("s3://hied-delta-lake/delta-tables/source/effy2021")

spark.sql('show databases').show()

spark.sql('create database source')
spark.sql('create database processed')

spark.sql('show databases').show()

spark.sql("create table source.effy2019 using delta location 's3://hied-delta-lake/delta-tables/source/effy2019'")
spark.sql("create table source.effy2020 using delta location 's3://hied-delta-lake/delta-tables/source/effy2020'")
spark.sql("create table source.effy2021 using delta location 's3://hied-delta-lake/delta-tables/source/effy2021'")

spark.sql('use source')
spark.sql('show tables').show()