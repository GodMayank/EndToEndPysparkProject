from pyspark.sql import SparkSession

jobname = "ETL_Code"
spark = SparkSession.builder.master("local[*]").appName(jobname).getOrCreate()

file_path = "Data/airports.csv"
airports = spark.read.csv(file_path, header=True)
airports.show()

type(airports)

flights = spark.read.csv("Data/flights_small.csv", header = True)
flights.show()

flight_add_col = flights.withColumn("duration_hrs", flights.air_time / 60)
flight_add_col.show()

long_flights1 = flights.filter("distance > 1000")

selected1 = long_flights1.select('tailnum', 'origin', 'dest')

temp = flights.select(flights.origin, flights.dest, flights.carrier)

filter1 = flights.origin == 'SEA'

filter2 = flights.dest == 'PDX'

selected_2 = temp.filter(filter1).filter(filter2)

selected_2.show()

avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

speed_1 = flights.select('origin','dest','tailnum', avg_speed)

speed_2 = flights.selectExpr('origin', 'dest', 'tailnum', 'distance/(air_time/60) as avg_speed')
speed_2.show()

flights.describe()
flights_cast = flights.withColumn('distance', flights.distance.cast('float'))
flights_cast = flights_cast.withColumn('air_time', flights_cast.air_time.cast('float'))
flights_cast.describe('distance', 'air_time').show()


flights_cast.filter(flights_cast.origin == 'PDX').groupBy().min('distance').show()

flights_cast.filter(flights_cast.origin == 'SEA').groupBy().max('air_time').show()

flights_cast.filter(flights_cast.carrier == 'DL').filter(flights_cast.origin == 'SEA').groupBy().avg('air_time').show()

flights_cast.filter(flights_cast.carrier == 'DL').filter(flights_cast.origin == 'SEA').groupBy().avg('air_time').show()

by_plane = flights.groupBy('tailnum')
by_plane.count().show()

by_origin = flights_cast.groupBy('origin')

by_origin.avg('air_time').show()


import pyspark.sql.functions as F
flights_cast = flights_cast.withColumn('dep_delay', flights_cast.dep_delay.cast('float'))
by_month_dest = flights_cast.groupBy('month', 'dest')

by_month_dest.avg('dep_delay').show()

airports.show()

airports = airports.withColumnRenamed('faa', 'dest')

