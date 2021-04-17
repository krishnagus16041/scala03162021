package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
import com.databricks.spark.xml
object sprkxmldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sprkxmldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------
val data = "C:\\bigdata\\datasets\\books.xml"
    val df = spark.read.format("xml").option("rowTag","book").load(data)
   //  df.show()
    df.createOrReplaceTempView("books")
    //val res = spark.sql("select author, count(*) cnt from tab group by author order by cnt desc")
    val res = spark.sql("select * from books where genre='Fantasy'")
    res.show()
    //res.write.mode(SaveMode.Overwrite).format("parquet").save("C:\\bigdata\\datasets\\output\\booksparq")
 //   res.write.format("orc").save("C:\\bigdata\\datasets\\output\\booksorc")
  //  res.write.format("avro").save("C:\\bigdata\\datasets\\output\\booksavro")

    val pdf = spark.read.format("parquet").load("C:\\bigdata\\datasets\\output\\booksparq")
    println("testing parquet data")

    pdf.show()

    //--------------------------------------------
    spark.stop()
  }
}