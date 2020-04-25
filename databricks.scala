// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX. Edit the filepath on line 7 (.load(...)) to point to your uploaded file
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata-069c2.csv") // UPDATE this line with your filepath. Refer Databricks Setup Guide Step 3.
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can alternatively uncomment the display() and show() functions to see the data differently. These two functions will be useful in reporting the results.

//display(df) //display in a tabular format for easy download

//df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// BEFORE YOU BEGIN: Replace gburdell3 with your GT username.
val gt_username = "asong49"
println(gt_username)

// COMMAND ----------

// PART 1: Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).
// Hint: Checkout the filter() function.

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

// ENTER THE CODE BELOW
val filter1 = df.filter("(PULocationID != DOLocationID) and (trip_distance > 2.0)")
display(filter1)

// COMMAND ----------

// PART 2: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val filter2 = filter1.groupBy("DOLocationID").agg(count("DOLocationID") as "number_of_dropoffs").orderBy(desc("number_of_dropoffs"),asc("DOLocationID")).limit(5)
display(filter2)

// COMMAND ----------

// PART 3: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 

// ENTER THE CODE BELOW
val filter3 = filter1.groupBy("PULocationID").agg(count("PULocationID") as "number_of_pickups").orderBy(desc("number_of_pickups"),asc("PULocationID")).limit(5)
display(filter3)

// COMMAND ----------

// PART 4: The top-5 most popular pickup-dropoff pairs - sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first.

// ENTER THE CODE BELOW
val filter4 = filter1.groupBy("PULocationID","DOLocationID").agg(count("PULocationID") as "count of pickup-dropoff").orderBy(desc("count of pickup-dropoff"),asc("PULocationID")).limit(5)
display(filter4)

// COMMAND ----------

// PART 5: Number of dropoffs over the period from January 1, 2019 (inclusive of January 1) to January 5, 2019 (inclusive of January 5). List the entries by day from January 1 to January 5.

// Reference: https://www.obstkel.com/blog/spark-sql-date-functions
// Read in the data and extract the month and year from the date column.
// Hint 1: Observe how we extracted the date from the timestamp in the thrid cell.
// Hint 2: Filter by month as well since there are a few dates for the month of February present in the dataset.

// ENTER THE CODE BELOW

val df2 = filter1.withColumn("day", dayofmonth(col("dropoff_datetime")))
.withColumn("dropoff_month", month(col("dropoff_datetime")))
.withColumn("dropoff_year", year(col("dropoff_datetime")))
.filter("(dropoff_year = 2019) and (dropoff_month = 1) and (day > 00) and (day < 06)")
.groupBy("day").agg(count("day") as "total_activity").orderBy(asc("day"))
display(df2)

// COMMAND ----------

// PART 6: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Hint: Checkout join() and na.drop() functions. You will need to perform a join operation between two dataframes which you created in earlier parts to get the result.

// ENTER THE CODE BELOW

val df3 = filter1.groupBy("DOLocationID").count().withColumnRenamed("DOLocationID", "LocationID").withColumnRenamed("count", "count1")
val df4 = filter1.groupBy("PULocationID").count().withColumnRenamed("PULocationID", "LocationID").withColumnRenamed("count", "count2")
val join1 = df3.join(df4, Seq("LocationID"), "full").na.fill(0)
val df5 = join1.groupBy("LocationID").agg(sum($"count1" + $"count2") as "total_activity").orderBy(desc("total_activity"),asc("LocationID")).limit(3)
display(df5)

