package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.bigdata.spark.sparksql.getOracleMysqldata
object getOracleMysqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOracleMysqldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //must use enableHiveSupport to write data in hive
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val myurl = "jdbc:mysql://mysqldb.cmpuk2t3n5oa.ap-south-1.rds.amazonaws.com:3306/mysqldb"

    val mydf = spark.read.format("jdbc").option("url",myurl).option("user","musername").option("password","mpassword")
      .option("driver","com.mysql.jdbc.Driver").option("dbtable","emp").load()
    mydf.show()

    val ourl ="jdbc:oracle:thin:@//yagnadb.clxrjwmfcpaf.us-east-2.rds.amazonaws.com:1521/ORCL"

    val odf = spark.read.format("jdbc").option("url",ourl).option("user","ousername").option("password","opassword")
      .option("driver","oracle.jdbc.OracleDriver").option("dbtable","DEPT").load()
    odf.show()
    odf.createOrReplaceTempView("d")
    mydf.createOrReplaceTempView("e")
    val join = spark.sql("select e.*, d.loc, d.dname from e join d on e.deptno=d.deptno")
    join.show()
   /*join.write.mode(SaveMode.Overwrite).format("jdbc").option("url",myurl).option("user","musername").option("password","mpassword")
      .option("driver","com.mysql.jdbc.Driver").option("dbtable","krishnajointab").save() */// wriet results in mysql
    //join.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("krishnajointab") // write results in hive
    spark.stop()
  }
}