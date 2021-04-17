package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// spark-submit --class com.bigdata.spark.sparksql.getMultipleTablesMysql --master local --deploy-mode client --jars $(echo s3://krishna2021bucket/drivers/*.jar | tr ' ' ',') s3://krishna2021bucket/apps/scala03162021_2.11-0.1.jar
object getMultipleTablesMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").enableHiveSupport().appName("diffdataformat").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------

    val ourl = "jdbc:oracle:thin:@//ora.cusjfqs5wrky.us-east-1.rds.amazonaws.com:1521/ORCL"

    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")


    val myurl = "jdbc:mysql://mysqldb.cusjfqs5wrky.us-east-1.rds.amazonaws.com:3306/mysqldb"

    val mprop = new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")

val qry = "(select TABLE_NAME from all_tables where TABLESPACE_NAME='USERS') t"
    // get all tables and insert in one array/list
    val all = spark.read.jdbc(ourl,qry,oprop).rdd.map(x=>x(0)).collect.toList


    val tabs = all
    tabs.foreach{ x=>
      println(s"importing $x table")
        val mdf = spark.read.jdbc(ourl,s"$x",oprop)
      mdf.write.mode(SaveMode.Overwrite).jdbc(myurl,s"$x", mprop)
      mdf.write.mode(SaveMode.Append).format("hive").saveAsTable(s"$x")
     println(s"successfully imported $x table")
     mdf.show()

    }



    //--------------------------------------------
    spark.stop()
  }
}