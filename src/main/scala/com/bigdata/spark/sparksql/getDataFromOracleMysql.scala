package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object getDataFromOracleMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getDataFromOracleMysql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val myurl = "jdbc:mysql://mysqldb.cmpuk2t3n5oa.ap-south-1.rds.amazonaws.com:3306/mysqldb"

val mprop = new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")

    val mdf = spark.read.jdbc(myurl,"emp",mprop)
    mdf.show()

    spark.stop()
  }
}