package stream_layer

import akka.actor.Actor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, window}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class StreamOutlierDetectSpark {
  val spark = SparkSession.builder().appName("Lambda Architecture - Speed layer")
    .master("local")
    .getOrCreate()

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribePattern", "ram.*")
    .load()

  import spark.implicits._

  df.printSchema()

  val metricCpuDf = df.selectExpr("CAST(value AS STRING)")

  val schema = new StructType().add("timestamp", LongType)
    .add("usage", IntegerType).add("source", StringType)

  val cpuDF = metricCpuDf.select(from_json(col("value"), schema).as("data"))
    .select("data.*")


  val windowedCpuDfCounts = cpuDF.groupBy(
      window(from_unixtime( $"timestamp"),
      "2 minutes",
      "1 minutes"),
      $"source")
      .count()

  windowedCpuDfCounts.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()
  print("End")

  def runDetect(): Unit = {
    println("hello from Stream")
  }
}

case object StartProcessing

class StreamOutlierProcessActor(spark_realtimeProc: StreamOutlierDetectSpark) extends Actor {

  //Implement receive method
  def receive = {
    //Start hashtag realtime processing
    case StartProcessing => {
      spark_realtimeProc.runDetect()
      println("\nRestart hashtag realtime processing...")
    }
  }
}