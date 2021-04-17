package com.bigdata.spark.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._

object flink1  {case class aslcc (name:String, age:Int, city:String)
case class grp (city:String, cnt:Long)
def main(args: Array[String]) {
  val env = ExecutionEnvironment.getExecutionEnvironment //batch processing

  val tEnv = BatchTableEnvironment.create(env)
  val data = "D:\\bigdata\\datasets\\asl.csv"
  val ds = env.readCsvFile[aslcc](data,ignoreFirstLine = true)
  val tab =  tEnv.fromDataSet(ds) //dataset convert to table api
  tEnv.registerTable("asl", tab) // dataset register as table called asl
  // val res = tEnv.sqlQuery("select * from asl where city='blr'")
  val res = tEnv.sqlQuery("select city, count(*) cnt from asl group by city order by cnt desc")
  //convert table api to dataset
  val result = tEnv.toDataSet[grp](res)

  val output = "D:\\bigdata\\datasets\\output\\flinkcsv.csv"
  result.writeAsCsv(output).setParallelism(1)
  //  result.print()
  env.execute()
}
}