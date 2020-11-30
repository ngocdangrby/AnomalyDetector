package stream_layer

import akka.actor.Actor
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, col, expr, from_json, from_unixtime, lit, stddev, struct, to_date, to_json, udf, window}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, TimestampType}

class StreamOutlierDetectSpark {
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
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribePattern", "testdata.*")
    .load()

  val metricCpuDf = df.selectExpr("CAST(value AS STRING)")

  val schema = new StructType().add("created_at", TimestampType)
    .add("usage", IntegerType).add("source_computer", StringType)

  val cpuDF = metricCpuDf.select(from_json(col("value"), schema).as("data"))
    .select("data.*")
  //  def get_median(df: DataFrame, col_name: String) = {
  //    df.stat.approxQuantile(col_name, Array(0.5), 0.25)
  //  }
  //
  //  def median(inputList: List[Double]): Double = {
  //    val count = inputList.size
  //    if (count % 2 == 0) {
  //      val l = count / 2 - 1
  //      val r = l + 1
  //      (inputList(l) + inputList(r)).toDouble / 2
  //    } else
  //      inputList(count / 2).toDouble
  //  }
  //
  //  val sub = udf((x: Double, y: Double) => {
  //    if (x > y) {
  //      x - y
  //    } else {
  //      y - x
  //    }
  //  })
  //
  //  def cal_mad(df: DataFrame) = {
  //    var resDf = df
  //    var median1 = median(resDf.select("usage").collect().map(_ (0).asInstanceOf[Double]).toList)
  //    //    var med = get_median(df, "usage")
  //    resDf.withColumn("cal1", col("usage") - median1)
  //    var median2 = median(resDf.select("cal1").collect().map(_ (0).asInstanceOf[Double]).toList)
  //    resDf.withColumn("mad", col("cal1") - median2)
  //    resDf.where("mad >= 3.12")
  //  }

  //


  //  val cal_mad_udf = udf(cal_mad)

  //  var windowedCpuDfCounts = cpuDF.groupBy(
  //    window(from_unixtime($"timestamp"),
  //      "2 minutes",
  //      "1 minutes"),
  //    $"source")

  //  val windowedCpuDfCounts = cpuDF.select(
  //    window(from_unixtime($"created_at"),
  //      "2 minutes",
  //      "1 minutes"),
  //    $"source_computer", $"usage", $"created_at")
  //    .agg(stddev(col("usage")).alias("stddev1"))
  //    .join(cpuDF,expr("source_computer == source_computer"), "left")
  //    .agg(stddev(col("stddev1")).alias("mad"))
  //    .where(col("mad") > 3.1)


  //  val test = df.groupby("genre").agg(F.expr("percentile(duration, array(0.5))")[0].alias("duration_median")).join(df, "genre", "left")
  //    .withColumn("duration_difference_median", F.abs(F.col("duration") - F.col("duration_median") ) ).groupby("genre", "duration_median")
  //    .agg(F.expr("percentile(duration_difference_median, array(0.5))")[0].alias("median_absolute_difference"))

  //  val withStdDF = windowedCpuDfCounts.select(
  //     $"stddev1"
  //  ).agg(stddev(col("sttdev1")).alias("mad"))
  //    .where($"mad" > 3.0)
  //  var test = cpuDF.groupBy(
  //    window(from_unixtime($"created_at"),
  //      "2 minutes",
  //      "1 minutes"),
  //    col("source_computer").alias("source_computer1")
  //  ).agg(expr("percentile(usage, array(0.5))")(0).alias("usage_median"))
  //
  //  val writeMem = test.writeStream
  //    .format("memory")
  //    .queryName("firstMedianDF")
  //    .outputMode("complete")
  //    .start()
  //
  //  val firstDF = spark.sql("select * from firstMedianDF")
  //
  //  var madDF = firstDF.join(cpuDF, expr("source_computer1==source_computer"), "inner")
  //    .withColumn("usage_diff_median", abs(col("usage") - col("usage_median")))
  //    .groupBy("source_computer", "usage_median")
  //    .agg(expr("percentile(usage_diff_median, array(0.5))")(0).alias("mad"))
  //
  //  val res = cpuDF.withWatermark("created_at", "10 minutes").select(col("source_computer").alias("sc"), col("usage"))
  //    .join(madDF, expr("sc==source_computer"), "inner").filter(abs(col("usage") - col("usage_median")) >= col("mad") * 3)
  //
  //  val writeConsole = res.writeStream
  //    .format("console")
  //    .queryName("anomaly")
  //    .outputMode("append")
  //    .start()
  //
  //  writeMem.awaitTermination()
  //  writeConsole.awaitTermination()
  //   val test = cpuDF.groupBy(window(from_unixtime($"created_at"),
  //          "2 minutes",
  //          "1 minutes"), col("usage")).withColumn("usage_median", expr("percentile(usage, array(0.5))")(0))
  //    .withColumn("diff", abs(col("usage_median") - col("usage")))

  //  val cpudf2 = cpuDF.withColumn("created_at",from_unixtime($"created_at" ))\

  //    val q1 = test.writeStream
  //      .format("memory")
  //      .queryName("testDF")
  //      .start()
  //  val testsql = spark.sql("select * from testDF")
  //    val test2 = testsql.withColumn("diff",abs(col("median") - col("usage")))
  //    val test3 = test2.withWatermark("created_at", "10 minutes")


  //    val res = test.withWatermark("created_at", "10 minutes")
  //      .groupBy(window(col("created_at"), "5 minutes")).count()
  //  val test2 = test.withWatermark("created_at","10 minutes")
  val test = cpuDF.withWatermark("created_at", "20 seconds")
    .select(expr("percentile(usage, array(0.5))").alias("median"))
  //    var median1 = test.select(col("median")).first().getDouble(0)
  val q1 = test.writeStream
    .queryName("medianTable")
    .format("memory")
    .outputMode("complete")
    .start()
  val median1 = spark.sql("select median from medianTable")
  val cpuDF2 = cpuDF.crossJoin(median1)
  val cpuDF3 = cpuDF2.withColumn("diff1", abs(col("usage") - col("median")(0)))
  val med2Df = cpuDF3
    .select(expr("percentile(diff1, array(0.5))").alias("median2"))

  val q2 = med2Df.writeStream
    .queryName("median2Table")
    .format("memory")
    .outputMode("complete")
    .start()

  val median2 = spark.sql("select median2 from median2Table")
  val cpuDF4 = cpuDF3.crossJoin(median2)
  val cpuDF5 = cpuDF4.withColumn("mad", abs((col("usage") - col("median")(0))/(col("median2")(0)*1.4826)))
  val cpuDF6 = cpuDF5.where(col("mad") >= 3.2)

//  val writeConsole = cpuDF6.writeStream
//    .format("console")
//    .queryName("anomaly")
//    .outputMode("append")
//    .start()
  cpuDF6.printSchema()
val writeConsole = cpuDF6.select(to_json(struct($"created_at",$"usage",$"source_computer")).alias("value"))
  .selectExpr( "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("topic", "alert")
  .option("checkpointLocation", "/home/dang/Desktop/checkpoint/")
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