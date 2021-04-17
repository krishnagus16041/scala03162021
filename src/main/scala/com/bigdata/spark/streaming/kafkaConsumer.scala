package com.bigdata.spark.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object kafkaConsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaConsumer").getOrCreate()
       val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("logs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

   // val lines =stream.map(record =>  record.value)
  //  val topics = Array("logs")
    //create dstream ...get data from kafka servers and prepare dstream

    //dstream
    val lines = stream.map(record =>  record.value)
    // lines.print()
if(lines!= null) {
    //Convert the streaming data to Dataframe for processing
    val res = lines.foreachRDD{ a=>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      // spark streaming by default generate rdd.... rdd convert to datarame
      //Map columns names
      val df = a.map(x=>x.split(" ")).map(x=>(x(0),x(3),x(4))).toDF("ip","date","time")
      df.show()  // Show the result

      df.createOrReplaceTempView("tab")
      //use live data in SQL query
          val masinfo = spark.sql("select * from tab where date='mas'")

      //Created Mysql instance on AWS to store live data to table
      /*  val url ="jdbc:mysql://testingmysql.ci1i74maitki.ap-south-1.rds.amazonaws.com:3306/mysqldb"
        val prop = new Properties()
        prop.setProperty("user","musername")
        prop.setProperty("password","mpassword")
        prop.setProperty("driver","com.mysql.jdbc.Driver")*/

      //Write to mysql database as livemas table
      //   masinfo.write.mode(SaveMode.Append).jdbc(url,"livemas",prop)

    }
}
    //--------------------------------------------
    ssc.start()
    ssc.awaitTermination()
  }
}