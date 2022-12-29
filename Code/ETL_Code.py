from pyspark.sql import SparkSession

jobname = "ETL_Code"
spark = SparkSession.builder.master("local[*]").appName(jobname).getOrCreate()

file_path = "Data/airports.csv"
airports = spark.read.csv(file_path, header=True)
airports.show()

type(airports)

flights = spark.read.csv("Data/flights_small.csv", header = True)
flights.show()

