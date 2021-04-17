package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object practice1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("practice1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\us-500.csv"
    //val data2 = "C:\\bigdata\\datasets\\us-250.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val df2 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\bigdata\\datasets\\us-250.csv")
    //df.createOrReplaceTempView("tab")
    //df2.createOrReplaceTempView("tab2")
    val df3 = df.union(df2).distinct()
    df3.createOrReplaceTempView("tab")

    val res = spark.sql("select count(*) from tab")
    res.show()
    //df.show(5)
    //df.show(5)
    //res.show()
    spark.stop()
  }
}