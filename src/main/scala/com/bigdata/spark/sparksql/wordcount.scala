package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //val url ="jdbc:oracle:thin:@//localhost:1521/orclhome"
    val df = spark.read.format("jdbc").option("url","jdbc:oracle:thin:@//localhost:1521/orclhome").option("user","krishna").option("password","password")
      .option("driver","oracle.jdbc.OracleDriver").option("dbtable","asl").load()
    df.show()
    //if u get class not found exception oracle.jdbc.OracleDriver ... go to file> project structure >> dependencies
    //+  add ojdbc7.jar ... ok

    spark.stop()
  }
}