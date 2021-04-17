package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object get2TablesOracle {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("diffdataformat").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------

    /*val oyurl = "jdbc:mysql://mysqldb.cmpuk2t3n5oa.ap-south-1.rds.amazonaws.com:3306/mysqldb"

    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")

*/
    val myurl = "jdbc:mysql://mysqldb.cusjfqs5wrky.us-east-1.rds.amazonaws.com:3306/mysqldb"

    val mprop = new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")

val qry = "(select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='mysqldb') tmp"
    val tabs = spark.read.jdbc(myurl,qry,mprop).rdd.map(x=>x(0)).collect.toList
//    val all = spark.read.jdbc(ourl,qry,oprop).rdd.map(x=>x(0)).collect.toList

   // val tabs = Array("emp","dept")
    tabs.foreach{ x=>
      println(s"importing $x table")
        val mdf = spark.read.jdbc(myurl,s"$x",mprop)
 //     mdf.write.mode(SaveMode.Append).format("hive").saveAsTable(s"$x")
   //   println(s"successfully imported $x table")
     mdf.show()

    }



    //--------------------------------------------
    spark.stop()
  }
}