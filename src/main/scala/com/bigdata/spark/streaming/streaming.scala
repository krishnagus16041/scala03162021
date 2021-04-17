package com.bigdata.spark.streaming


import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.streaming.{Seconds, StreamingContext}



object streaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("streaming").getOrCreate()

    //val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    //sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------
    val lines = ssc.socketTextStream("ec2-3-129-206-249.us-east-2.compute.amazonaws.com", 1234)
    lines.print()
    ssc.start()
    ssc.awaitTermination()

    //--------------------------------------------
    spark.stop()
  }
}