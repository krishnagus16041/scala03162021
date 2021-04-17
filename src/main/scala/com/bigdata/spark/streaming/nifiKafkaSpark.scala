package com.bigdata.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object nifiKafkaSpark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("kafkaConsumer").getOrCreate()
       val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    //--------------------------------------------
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("nifi")
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

    //Convert the streaming data to Dataframe for processing
    val res = lines.foreachRDD{ a=>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._
val df = spark.read.json(a)
      df.show(false)
df.createOrReplaceTempView("tab")
      df.printSchema()
      val ourl = ""
      val res = spark.sql("select count(*) cnt, nationality from tab group by nationality order by cnt desc")
      res.show()
     // res.write.mode(SaveMode.Append).format("jdbc").option("url",ourl).option("user","ousername").option("password","opassword")
       // .option("driver","oracle.jdbc.OracleDriver").option("dbtable","nifitab").save()

    }

    //--------------------------------------------
    ssc.start()
    ssc.awaitTermination()
  }
}