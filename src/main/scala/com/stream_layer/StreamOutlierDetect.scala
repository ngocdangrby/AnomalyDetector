package stream_layer

import akka.actor.Actor
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, col, expr, from_json, from_unixtime, lit, stddev, struct, to_date, to_json, udf, unix_timestamp, window}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType, TimestampType}

class StreamOutlierDetectSpark(kafkahost: String, prefixTopic: String, checkpointpath: String, colName: String ){

  val spark = SparkSession.builder().appName("Lambda Architecture - Speed layer")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkahost)
    .option("subscribePattern", prefixTopic+".*")
    .load()

  val metricCpuDf = df.selectExpr("CAST(value AS STRING)")

  val schema = new  StructType().add("timestamp", LongType)
    .add(colName, StringType).add("interface", StringType)

  val cpuDF = metricCpuDf.select(from_json(col("value"), schema).as("data"))
    .select("data.*")
    
  val cpuDfs = cpuDF.withColumn("timestamp",(col("timestamp")/1000).cast("timestamp"))
  val cpuDFnew = cpuDfs.withColumn(colName, col(colName).cast(LongType))
  val test = cpuDFnew.withWatermark("timestamp", "5 minutes")
    .select(expr("percentile(" +colName +", array(0.5))").alias("median"))
  //    var median1 = test.select(col("median")).first().getDouble(0)
  val q1 = test.writeStream
    .queryName("medianTable")
    .format("memory")
    .outputMode("complete")
    .start()
  val median1 = spark.sql("select median from medianTable")
  val cpuDF2 = cpuDFnew.crossJoin(median1)
  val cpuDF3 = cpuDF2.withColumn("diff1", abs(col(colName) - col("median")(0)))
  val med2Df = cpuDF3
    .select(expr("percentile(diff1, array(0.5))").alias("median2"))

  val q2 = med2Df.writeStream
    .queryName("median2Table")
    .format("memory")
    .outputMode("complete")
    .start()

  val median2 = spark.sql("select median2 from median2Table")
  val cpuDF4 = cpuDF3.crossJoin(median2)
  val cpuDF5 = cpuDF4.withColumn("mad", abs((col(colName) - col("median")(0))/(col("median2")(0)*1.4826)))
  val cpuDF6 = cpuDF5.where(col("mad") >= 3.12)

  //  val writeConsole = cpuDF6.writeStream
  //    .format("console")
  //    .queryName("anomaly")
  //    .outputMode("append")
  //    .start()
  cpuDF6.printSchema()
  val writeConsole = cpuDF6.select(to_json(struct($"timestamp",col(colName),$"interface")).alias("value"))
    .selectExpr( "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkahost)
    .option("topic", "alert")
    .option("checkpointLocation", checkpointpath)
    .start()

  q1.awaitTermination()
  q2.awaitTermination()
  writeConsole.awaitTermination()

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