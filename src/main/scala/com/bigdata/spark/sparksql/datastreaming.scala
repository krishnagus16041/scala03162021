package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object datastreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("streamingeg").getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    // sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("ec2-3-129-206-249.us-east-2.compute.amazonaws.com", 1234)
    // sockettextstream get data from ...server from portnumber.
    lines.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}