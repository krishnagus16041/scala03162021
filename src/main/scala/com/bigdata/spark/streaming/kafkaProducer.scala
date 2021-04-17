package com.bigdata.spark.streaming

import org.apache.kafka.clients.producer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object kafkaProducer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaProducer").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------
// write java/scala/python code to get data from source ..
 //   val logs = "C:\\bigdata\\drivers\\apachelogs\\access*.log"
    val logs = args(0)
val topic ="logs"
val data = spark.sparkContext.textFile(logs) // read data from something log path
    data.foreachPartition(rdd => {
      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")
      // import kafka.producer._
      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      rdd.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)

      })
    })

    //--------------------------------------------
    ssc.start()
    ssc.awaitTermination()
  }
}