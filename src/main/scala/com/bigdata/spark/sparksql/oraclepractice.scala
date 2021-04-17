package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oraclepractice {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oraclepractice").enableHiveSupport().getOrCreate()
    //val spark = SparkSession.builder.master("local[*]").appName("oraclepractice").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val ourl = "jdbc:oracle:thin:@//192.168.1.226:1521/orclhome"
    //val ourl ="jdbc:oracle:thin:@//yagnadb.clxrjwmfcpaf.us-east-2.rds.amazonaws.com:1521/ORCL"
    val odf = spark.read.format("jdbc").option("url",ourl).option("user","krishna").option("password","password").
              option("driver","oracle.jdbc.OracleDriver").option("dbtable","dept").load()
     odf.show()
    val odf2 = spark.read.format("jdbc").option("url",ourl).option("user","krishna").option("password","password").
      option("driver","oracle.jdbc.OracleDriver").option("dbtable","emp").load()
    odf2.show()
    odf.createOrReplaceTempView("a")
    odf2.createOrReplaceTempView("b")
    val join = spark.sql("select a.dname,a.loc,b.* from a join b on a.deptno = b.deptno")
    join.show()
    join.write.mode(SaveMode.Overwrite).format("jdbc").option("url",ourl).option("user","krishna").option("password","password").
    option("driver","oracle.jdbc.OracleDriver").option("dbtable","joinedtable").save()
    join.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("newtable")
    spark.stop()
  }
}
