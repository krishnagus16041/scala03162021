package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object practice2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("practice2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )
    val columns = Seq("firstname","lastname","country","state")
    val df = data.toDF(columns:_*)
    df.select("firstname").show(false)
    spark.stop()
  }
}