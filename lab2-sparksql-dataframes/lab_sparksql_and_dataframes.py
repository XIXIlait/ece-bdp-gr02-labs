#!/usr/bin/env python
# coding: utf-8

# # Exploratory Data Analysis with Pyspark and Spark SQL
# 
# The following notebook utilizes New York City taxi data from [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
# 
# ## Instructions
# 
# - Load and explore nyc taxi data from january 0f 2019. The exercises can be executed using pyspark or spark sql (a subset of the questions will be re-answered using the language not chosen for the  main work).
# - Load the zone lookup table to answer the questions about the nyc boroughs.  
# - Load nyc taxi data from January of 2025 and compare data.  
# - With any remaining time, work on the where to go from here section.
# - Note: the initial lab is opened as read only. To save work completed utilize the `save notebook as` option and give the lab a new name.

# In[1]:


import requests

# start a spark session and create a spark context
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("nyc_taxi") \
    .getOrCreate()

sc = spark.sparkContext


# In[2]:


import os

# set dl url for January 2019 trip data
download_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet'
jan_2019_trip_data = "yellow_tripdata_2019-01.parquet"

# download only if the file is not already there
if not os.path.exists(jan_2019_trip_data):
    response = requests.get(download_url)
    if response.status_code == 200:
        with open(jan_2019_trip_data, "wb") as f:
            f.write(response.content)


# In[3]:


# create the dataframe
df_trips = spark.read.parquet(jan_2019_trip_data)


# # A brief note on handling data sources in spark
# 
# The command above works well for loading data from parquet files because parquet is a self descibing file format, meaning that the metadata needed to build the dataframe is included directly in the format. However, when working with other formats such as csv or json, a schema must be provided or infered. In production code the schema should always be explicitly provided but during the data exploration phase it is acceptable to infer the schema, and when infering the schema it often best to use `.option("samplingRatio", <small-portion-of-data>)` to avoid using the entire dataset for schema inference.
# 
# ```python
# df_trips = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("sep", ",") \
#     .option("samplingRatio", 0.01) \
#     .load("large_dataset.csv")
# ```

# In[4]:


# Show the dataframe
df_trips.show()


# ## Lab
# 
# ### Part 1
# This section can be completed either using pyspark commands or sql commands ( There will be a section after in which a self-chosen subset of the questions are re-answered using the language not used for the main section. i.e. if pyspark is chosen for the main lab, sql should be used to repeat some of the questions. )
# 
# - Add a column that creates a unique key to identify each record in order to answer questions about individual trips
# - Which trip has the highest passanger count
# - What is the Average passanger count
# - Shortest/longest trip by distance? by time?.
# - busiest day/slowest single day
# - busiest/slowest time of day ( you may want to bucket these by hour or create timess such as morning, afternoon, evening, late night )
# - On average which day of the week is slowest/busiest
# - Does trip distance or num passangers affect tip amount
# - What was the highest "extra" charge and which trip
# - Are there any datapoints that seem to be strange/outliers (make sure to explain your reasoning in a markdown cell)?

# In[5]:


from pyspark.sql import functions as F


# #### 1.1 Unique key for each trip

# In[6]:


# unique id for each trip
df_trips = df_trips.withColumn("trip_id", F.monotonically_increasing_id())

# trip duration in minutes (used in several questions)
df_trips = df_trips.withColumn(
    "duration_min",
    (F.unix_timestamp("tpep_dropoff_datetime")
     - F.unix_timestamp("tpep_pickup_datetime")) / 60
)

df_trips.select("trip_id", "tpep_pickup_datetime",
                "tpep_dropoff_datetime", "duration_min").show(5)


# The ids are unique but not consecutive: each partition gets its own range.

# #### 1.2 Highest and average passenger count

# In[7]:


df_trips.orderBy(F.desc("passenger_count")) \
    .select("trip_id", "passenger_count", "trip_distance", "total_amount") \
    .show(5)

df_trips.agg(F.avg("passenger_count").alias("avg_passengers")).show()


# Max is 9 passengers (several trips, all with a distance of 0, so probably errors). The average is about 1.57, most people ride alone.

# #### 1.3 Shortest / longest trip by distance and by time

# In[8]:


# by distance
df_trips.orderBy("trip_distance") \
    .select("trip_id", "trip_distance", "duration_min").show(3)
df_trips.orderBy(F.desc("trip_distance")) \
    .select("trip_id", "trip_distance", "duration_min").show(3)

# shortest trip with a distance above 0
df_trips.filter(F.col("trip_distance") > 0).orderBy("trip_distance") \
    .select("trip_id", "trip_distance", "duration_min").show(3)


# In[9]:


# by time
df_trips.orderBy("duration_min") \
    .select("trip_id", "tpep_pickup_datetime",
            "tpep_dropoff_datetime", "duration_min").show(3)
df_trips.orderBy(F.desc("duration_min")) \
    .select("trip_id", "tpep_pickup_datetime",
            "tpep_dropoff_datetime", "duration_min").show(3)


# - Distance: many trips at 0 miles, shortest real trip is 0.01 mile. Longest is 831.8 miles in 9 minutes: impossible.
# - Time: shortest has a negative duration, longest lasts about 30 days.
# 
# These extremes are data errors (see 1.9).

# #### 1.4 Busiest / slowest day

# In[10]:


# keep only January 2019 (some dates are in 2008, 2018, 2088...)
df_jan = df_trips.filter(
    (F.col("tpep_pickup_datetime") >= "2019-01-01")
    & (F.col("tpep_pickup_datetime") < "2019-02-01")
)

df_days = df_jan.groupBy(
    F.to_date("tpep_pickup_datetime").alias("day")
).count()

df_days.orderBy(F.desc("count")).show(3)
df_days.orderBy("count").show(3)


# Busiest: Friday January 25 (292,499 trips). Slowest: January 1 (189,432), a holiday. January 21 (Martin Luther King Day) is second slowest.

# #### 1.5 Busiest / slowest time of day

# In[11]:


df_hours = df_jan.groupBy(
    F.hour("tpep_pickup_datetime").alias("hour")
).count()

df_hours.orderBy(F.desc("count")).show(3)
df_hours.orderBy("count").show(3)


# In[12]:


df_periods = df_jan.withColumn("hour", F.hour("tpep_pickup_datetime")) \
    .withColumn(
        "period",
        F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "morning")
         .when((F.col("hour") >= 12) & (F.col("hour") < 18), "afternoon")
         .when(F.col("hour") >= 18, "evening")
         .otherwise("late night")
    )

df_periods.groupBy("period").count().orderBy(F.desc("count")).show()


# Busiest hour: 18h (end of work day). Slowest: 4h. By period, afternoon is the busiest and late night (0h to 6h) the slowest.

# #### 1.6 Busiest / slowest day of the week on average

# In[13]:


# January has 4 or 5 of each weekday, so we average per day
df_days.withColumn("weekday", F.date_format("day", "EEEE")) \
    .groupBy("weekday") \
    .agg(F.avg("count").alias("avg_trips")) \
    .orderBy(F.desc("avg_trips")) \
    .show()


# Average and not total, because January 2019 has 5 Tuesdays/Wednesdays/Thursdays but 4 of the other days. Friday is the busiest, Sunday the slowest.

# #### 1.7 Does distance or number of passengers affect the tip?

# In[14]:


# average tip by payment type (1 = credit card, 2 = cash)
df_trips.groupBy("payment_type") \
    .agg(F.avg("tip_amount").alias("avg_tip")) \
    .orderBy("payment_type").show()


# In[15]:


# cash tips are not recorded, so we keep only card payments
df_card = df_trips.filter(F.col("payment_type") == 1)

df_card.select(
    F.corr("trip_distance", "tip_amount").alias("corr_distance_tip"),
    F.corr("passenger_count", "tip_amount").alias("corr_passengers_tip")
).show()

df_card.groupBy("passenger_count") \
    .agg(F.avg("tip_amount").alias("avg_tip"),
         F.count("*").alias("nb_trips")) \
    .orderBy("passenger_count").show()


# Cash tips are almost 0 in the data (not recorded), so we only use card payments.
# - Distance: yes, correlation 0.67, longer trips get bigger tips.
# - Passengers: no, correlation 0.01, the average tip stays around 2.5 to 2.6 dollars (7 to 9 passengers: too few trips to conclude).

# #### 1.8 Highest "extra" charge

# In[16]:


df_trips.orderBy(F.desc("extra")) \
    .select("trip_id", "extra", "fare_amount", "total_amount",
            "tpep_pickup_datetime").show(3)


# Highest extra: 535.38 dollars, on a trip with a fare of 355,676.98 dollars, so an error. A normal extra is 0.5 or 1 dollar (rush hour / night).

# #### 1.9 Strange data points / outliers

# In[17]:


print("passenger_count = 0:",
      df_trips.filter(F.col("passenger_count") == 0).count())
print("passenger_count null:",
      df_trips.filter(F.col("passenger_count").isNull()).count())
print("trip_distance = 0:",
      df_trips.filter(F.col("trip_distance") == 0).count())
print("fare_amount < 0:",
      df_trips.filter(F.col("fare_amount") < 0).count())
print("duration <= 0:",
      df_trips.filter(F.col("duration_min") <= 0).count())
print("duration > 24h:",
      df_trips.filter(F.col("duration_min") > 24 * 60).count())
print("not in January 2019:", df_trips.count() - df_jan.count())

df_trips.select("trip_distance", "fare_amount", "tip_amount",
                "extra", "passenger_count", "duration_min").describe().show()


# - **0 passengers** (117,381) or empty (28,672): a trip needs at least 1 passenger, the driver did not enter it.
# - **Distance 0** (55,089): cancelled trip or GPS problem. 831.8 miles in 9 minutes is impossible.
# - **Negative fares** (7,129): refunds or corrections.
# - **Duration 0 or negative** (6,557) and **over 24h** (5): meter not stopped correctly.
# - **537 trips outside January 2019** (years 2001, 2008, 2088...): wrong clock.
# - **Fare of 623,259.86 dollars** and extra of 535.38: typing errors.
# 
# These rows should be removed for a real analysis. For time questions we already use `df_jan`.

# ### Part 2
# 
# - Using the code for loading the first dataset as an example, load in the taxi zone lookup and answer the following questions
# - which borough had most pickups? dropoffs?
# - what are the busy/slow times by borough 
# - what are the busiest days of the week by borough?
# - what is the average trip distance by borough?
# - what is the average trip fare by borough?
# - highest/lowest faire amounts for a trip, what burough is associated with the each
# - load the dataset from the most recently available january, is there a change to any of the average metrics.

# #### 2.0 Load the taxi zone lookup

# In[18]:


zones_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
zones_file = "taxi_zone_lookup.csv"

if not os.path.exists(zones_file):
    response = requests.get(zones_url)
    if response.status_code == 200:
        with open(zones_file, "wb") as f:
            f.write(response.content)

df_zones = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(zones_file)

df_zones.show(5)


# #### 2.1 Join trips with boroughs

# In[19]:


# one join for the pick-up borough, one for the drop-off borough
zones_pu = df_zones.select(F.col("LocationID").alias("PULocationID"),
                           F.col("Borough").alias("pu_borough"))
zones_do = df_zones.select(F.col("LocationID").alias("DOLocationID"),
                           F.col("Borough").alias("do_borough"))

df_z = df_jan.join(zones_pu, "PULocationID", "left") \
             .join(zones_do, "DOLocationID", "left")

df_z.select("trip_id", "pu_borough", "do_borough").show(5)


# Left join to keep all trips. `Unknown` is zone 264 (location not known) and `N/A` is zone 265 (outside of NYC).

# #### 2.2 Borough with most pickups / dropoffs

# In[20]:


df_z.groupBy("pu_borough").count().orderBy(F.desc("count")).show()
df_z.groupBy("do_borough").count().orderBy(F.desc("count")).show()


# Manhattan by far: about 90% of pickups and dropoffs. Then Queens (airports JFK and LaGuardia). Staten Island is last.

# #### 2.3 Busy / slow times by borough

# In[21]:


df_z = df_z.withColumn("hour", F.hour("tpep_pickup_datetime")) \
    .withColumn(
        "period",
        F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "morning")
         .when((F.col("hour") >= 12) & (F.col("hour") < 18), "afternoon")
         .when(F.col("hour") >= 18, "evening")
         .otherwise("late night")
    )

df_z.groupBy("pu_borough") \
    .pivot("period", ["morning", "afternoon", "evening", "late night"]) \
    .count() \
    .orderBy(F.desc("afternoon")) \
    .show()


# Manhattan and Queens are busiest in the afternoon/evening. Brooklyn and the Bronx are busiest in the morning (trips to work). Late night is the slowest everywhere.

# #### 2.4 Busiest days of the week by borough

# In[22]:


days = ["Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday"]

# average number of trips per day, like in 1.6
df_z.groupBy("pu_borough",
             F.to_date("tpep_pickup_datetime").alias("day")).count() \
    .withColumn("weekday", F.date_format("day", "EEEE")) \
    .groupBy("pu_borough") \
    .pivot("weekday", days) \
    .agg(F.round(F.avg("count"))) \
    .orderBy(F.desc("Friday")) \
    .show()


# Manhattan: Thursday/Friday busiest, Sunday slowest. Queens: Monday busiest, Saturday slowest. Brooklyn and Bronx: Friday busiest.

# #### 2.5 Average trip distance and fare by borough

# In[23]:


df_z.groupBy("pu_borough") \
    .agg(F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
         F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
         F.count("*").alias("nb_trips")) \
    .orderBy(F.desc("avg_fare")) \
    .show()


# Manhattan has the shortest and cheapest trips (2.23 miles, 10.79 dollars). Queens (airports) and Staten Island (far from Manhattan) have the longest trips, over 11 miles. EWR (Newark airport) has the highest fare, but only 446 trips.

# #### 2.6 Highest / lowest fare and their borough

# In[24]:


cols = ["trip_id", "fare_amount", "trip_distance", "pu_borough", "do_borough"]

df_z.orderBy(F.desc("fare_amount")).select(cols).show(3)
df_z.orderBy("fare_amount").select(cols).show(3)


# Highest: 623,259.86 dollars for 2.4 miles in Manhattan, an error. Lowest: -362 dollars in Queens, a refund. See outliers in 1.9.

# #### 2.7 Compare with the most recent January (2026)
# 
# The most recent January available on the TLC website is January 2026.

# In[25]:


jan_2026_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet'
jan_2026_trip_data = "yellow_tripdata_2026-01.parquet"

if not os.path.exists(jan_2026_trip_data):
    response = requests.get(jan_2026_url)
    if response.status_code == 200:
        with open(jan_2026_trip_data, "wb") as f:
            f.write(response.content)

df_2026 = spark.read.parquet(jan_2026_trip_data)
df_2026 = df_2026.withColumn(
    "duration_min",
    (F.unix_timestamp("tpep_dropoff_datetime")
     - F.unix_timestamp("tpep_pickup_datetime")) / 60
)


# In[26]:


# same simple cleaning for both years (the outliers change the averages)
def average_metrics(df, year):
    df_clean = df.filter(
        (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500)
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100)
        & (F.col("duration_min") > 0) & (F.col("duration_min") < 180)
    )
    return df_clean.agg(
        F.lit(year).alias("year"),
        F.count("*").alias("nb_trips"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("total_amount"), 2).alias("avg_total")
    )

average_metrics(df_trips, 2019).union(average_metrics(df_2026, 2026)).show()


# Yes, all averages changed between 2019 and 2026:
# - **Half as many trips** (7.6M to 3.5M), probably because of Uber/Lyft.
# - **Fewer passengers** (1.57 to 1.25), **longer trips** (2.85 to 3.49 miles, 13 to 17 minutes).
# - **Much more expensive**: fare 12.29 to 21.07 dollars, total 15.56 to 29.66 dollars (new fees, like the `cbd_congestion_fee` column added in 2025).

# ### Part 3
# 
# - choose 3 questions from above and re-answer them using the language you did not use for the main notebook . (i.e - if you completed the exercise in python, redo 3 questions in pure sql) . at least one of the questions to be redone must involve a join

# SQL on the same data: we register the DataFrames as tables.

# In[27]:


df_jan.createOrReplaceTempView("trips")
df_zones.createOrReplaceTempView("zones")


# #### 3.1 Highest and average passenger count (same as 1.2)

# In[28]:


spark.sql("""
    SELECT MAX(passenger_count) AS max_passengers,
           ROUND(AVG(passenger_count), 2) AS avg_passengers
    FROM trips
""").show()


# #### 3.2 Busiest time of day (same as 1.5)

# In[29]:


spark.sql("""
    SELECT HOUR(tpep_pickup_datetime) AS hour,
           COUNT(*) AS nb_trips
    FROM trips
    GROUP BY HOUR(tpep_pickup_datetime)
    ORDER BY nb_trips DESC
    LIMIT 3
""").show()


# #### 3.3 Borough with most pickups, with a join (same as 2.2)

# In[30]:


spark.sql("""
    SELECT z.Borough AS pu_borough,
           COUNT(*) AS nb_pickups
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Borough
    ORDER BY nb_pickups DESC
""").show()


# Same results as in Python: both go through the same Catalyst optimizer, only the syntax changes.

# # Where to go from here
# 
# - Continue building the dataset by loading in more data, start by completing the data for 2019 and calculating the busiest season (fall, winter, spring, summer)
# - As of spark v4 dataframes have native visualization support. Choose at least 3 questions from above and provide visualizations.
# - Explore a dataset/datasets of your choosing
