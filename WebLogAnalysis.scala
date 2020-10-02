import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row._

object WebLogAnalysis extends App {

  // Turn of spark logging
  Logger.getLogger("org").setLevel(Level.OFF)

  // usual spark incantation
  val spark = SparkSession.builder().appName("WebLogAnalysis").master("local[*]").getOrCreate()
  import spark.implicits._

  // file name of the sample data
  // we use the compressed version as in the github repository
  // but this should NOT be used in general since gzip files
  // are not splittable!!!
  val fn = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

  // define a schema for the log file data define by AWS in
  // http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
  val customSchema = StructType(Array(
    StructField("time", TimestampType, true),
    StructField("elb", StringType, true),
    StructField("client:port", StringType, true),
    StructField("backend:port", StringType, true),
    StructField("request_processing_time", FloatType, true),
    StructField("backend_processing_time", FloatType, true),
    StructField("response_processing_time", FloatType, true),
    StructField("elb_status_code", IntegerType, true),
    StructField("backend_status_code", IntegerType, true),
    StructField("received_bytes", LongType, true),
    StructField("sent_bytes", LongType, true),
    StructField("request", StringType, true),
    StructField("user_agent", StringType, true),
    StructField("ssl_cipher", StringType, true),
    StructField("ssl_protocol", StringType, true)))

  val byClientPortOrderedByEpoch =
    Window.partitionBy("client:port").orderBy('epoch)

  // max 15min lag within a session
  val checkNewSession = udf((diff: Long) => { if (diff > 15*60) 1 else 0 })
  val create_session_id = udf((a: String, b:String) => (a + ":" + b))

  val df = spark.read.format("csv") // use the csv reader
    // data is saved in white space delimited columns
    .option("sep", " ")
    // some fields contain " as delimiter
    .option("delimiter", "\"")
    // csv data does not have a header
    .option("header", "false")
    // cast to the scheme defined above, accoridng to AWS documentation
    .schema(customSchema)
    // read the actual file
    .load(fn)
    // the description often speaks of IP
    // I am not sure whether doing any aggregation after IP is a good idea, since NATed IPs will be all
    // thrown into the same pot, so better aggregate always after IP:port
    // But for description's sake, add the IP only column
    .withColumn("client_ip",split(col("client:port"),":").getItem(0))
    //.withColumn("client_port",split(col("client:port"),":").getItem(1))
    // same for server ip/port
    //.withColumn("backend_ip",split(col("backend:port"),":").getItem(0))
    //.withColumn("backend_port",split(col("backend:port"),":").getItem(1))
    // get the request URL (and if necessary the http method (GET/POST)
    .withColumn( "url", split(col("request"), pattern=" ").getItem(key=1))
    //.withColumn( "method", split(col("request"), pattern=" ").getItem(key=0))
    // add a column with the timestamp converted to secs since epoch
    .withColumn("epoch", unix_timestamp($"time"))
    // build the relative time distance between request times per user
    .withColumn("diff", col("epoch").minus(lag(col("epoch"),1).over(byClientPortOrderedByEpoch)))
    // tag those lines where there is a difference larger than 15min (in secs) as new session
    .withColumn("new_session", when(col("diff").isNull, 1).otherwise(checkNewSession(col("diff"))) )
    // add a session idx (per user) column by adding up the values of new_session column (per user)
    .withColumn("session_idx_per_user",sum("new_session").over(byClientPortOrderedByEpoch).cast("string"))
    // create the session id from the user id (client:port) and the session index per user
    .withColumn("session_id",create_session_id(col("client:port"), col("session_idx_per_user")))
    // we need:
    // - time for average session time
    .select("time", "epoch", "diff", "client_ip", "session_id", "client:port", "url")
    // for debugging use this instead
    // .select("epoch", "time", "new_session", "session_idx_per_user", "session_id", "client:port", "url", "user_agent")
    // cache the data for multiple aggregations
    .cache()


  // display the first 50 lines of the data frame in a reasonable order
  df.orderBy("client:port", "epoch").show(50,false)


  // save sessionized data
  // df.repartition(1).write.format("csv").option("delimiter", "\t").mode("overwrite").save("sessionized-data")

  // compute session time as sum of differences in partition by session idx
  val bySessionId = Window.partitionBy("session_id").orderBy('epoch)

  // compute session time by summing up the diff values within a session
  val sessionTimeDf = df.withColumn("session_time_incr", sum(when(col("diff").isNull,0).otherwise(col("diff"))).over(bySessionId))
    .groupBy("session_id")
    .agg(
      max("session_time_incr"),
      first("client_ip")        // we cannot add arbitrary columns, but we know the IP is the same, add the first
    )
    .withColumnRenamed("max(session_time_incr)", "session_time")
    .withColumnRenamed("first(client_ip)", "client_ip")
    .cache()

  // show the session times
  sessionTimeDf.show(50, false)
  // sessionTimeDf.write.format("csv").option("delimiter", "\t").mode("overwrite").save("session-times")

  // compute the mean session time
  sessionTimeDf.select(mean("session_time")).show()

  // compute the number of distinct URLs visited per session
  val uniqueURLcountsBySession = df.groupBy("session_id")
    .agg(
      first("client_ip"), // we add client IP here to mage aggregation easier
      countDistinct("url")
    )
    .withColumnRenamed("first(client_ip)", "client_ip")
    .withColumnRenamed("count(url)", "unique_url_count")

  uniqueURLcountsBySession.show(50, false)

  // get the session id of the longest session (which is IP:port:session_nr)
  Console.println(sessionTimeDf.orderBy(desc("session_time")).first().get(0))

  // predictions
  // we use trivial mean estimator (mean value)
  // a better approach would be arima based predictions which model seasonality

  // Predict the expected load (requests/second) in the next minute
  // there are several ways to compute this:
  // - mean req per second straight away
  // - compute req per minute and then the mean
  // mathematically this is the same
  df.groupBy("epoch").agg(
    count(lit(1)).alias("rps")
  ).select(mean("rps")).show()

  // Predict the session length for a given IP
  sessionTimeDf.groupBy("client_ip").agg(mean("session_time")).show(50,false)

  // Predict the number of unique URL visits by a given IP
  // I guess what is mean is the number of unique URLs per session per IP
  uniqueURLcountsBySession.groupBy("client_ip").agg(mean("unique_url_count")).show(50,false)
}
