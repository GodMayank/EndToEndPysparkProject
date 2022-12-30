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

flights_with_airport = flights.join(airports, on="dest", how="leftouter")
flights_with_airport.show()

planes = spark.read.csv("Data/planes.csv", header = True, inferSchema = True)
planes.show()

planes = planes.withColumnRenamed('year', 'plane_year')

model_data = flights.join(planes, on='tailnum', how='leftouter')
model_data.show()

model_data.describe()

model_data = model_data.withColumn('arr_delay', model_data.arr_delay.cast('integer'))

model_data = model_data.withColumn('air_time', model_data.air_time.cast('integer'))

model_data = model_data.withColumn('month', model_data.month.cast('integer'))

model_data = model_data.withColumn('plane_year', model_data.plane_year.cast('integer'))

model_data.describe('arr_delay', 'air_time', 'month', 'plane_year').show()

model_data = model_data.withColumn('plane_age', model_data.year - model_data.plane_year)

model_data = model_data.withColumn('is_late', model_data.arr_delay > 0)

model_data = model_data.withColumn('label', model_data.is_late.cast('integer'))

model_data.select('is_late', 'arr_delay').show()

model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

from pyspark.ml.feature import StringIndexer, OneHotEncoder

carr_indexer = StringIndexer(inputCol = 'carrier', outputCol = 'Carrier_index')

carr_encoder = OneHotEncoder(inputCol = 'carrier_index', outputCol = 'carr_fact')

dest_indexer = StringIndexer(inputCol = 'dest', outputCol = 'dest_index')

dest_encoder = OneHotEncoder(inputCol = 'dest_index', outputCol = 'dest_fact')

from pyspark.ml.feature import VectorAssembler

##skiping rest of notebook bcoz not relavent to me.. ML stuff.

print('FINISHED')