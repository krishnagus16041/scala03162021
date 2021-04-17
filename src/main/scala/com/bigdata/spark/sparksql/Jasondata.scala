package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Jasondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Jasondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //--------------------------------------------
    val data = "C:\\bigdata\\datasets\\world_bank.json"
    val df = spark.read.format("json").load(data)
    df.show(4)
    df.printSchema()
    /*
    array if u haev use explode.. explode remove array and convert to struct
    if u have struct use parentCol.ChildColumn
     */
    val ndf = df.withColumn("tn", explode($"theme_namecode")).withColumn("theme1name",$"theme1.Name")
      .withColumn("theme1Percent",$"theme1.Percent").withColumn("sector4name", $"sector4.Name")
      .withColumn("sector4percent", $"sector4.Percent").withColumn("sector3name", $"sector3.Name")
      .withColumn("sector3percent", $"sector3.Percent").withColumn("sector2name", $"sector2.Name")
      .withColumn("sector2percent", $"sector2.Percent").withColumn("sector1name", $"sector1.Name")
      .withColumn("sector1percent", $"sector1.Percent")
      .withColumn("mjn", explode($"mjtheme_namecode"))
      .withColumn("sec", explode($"sector"))
      .withColumn("proj", explode($"projectdocs"))
      .withColumn("sn", explode($"sector_namecode"))
      .withColumn("mjsp", explode($"majorsector_percent"))
      .withColumn("mjnc",explode($"mjsector_namecode"))

      .select($"", $"`_id`.`$$oid`".alias("idoid"), explode($"mjtheme"), $"proj.",$"project_abstract.cdata".alias("project_abstractcname") ,$"sec.Name".alias("sectorname"), $"project_abstract.cdata",$"mjsp.Name".alias("mjspname"),$"mjsp.Percent".alias("mjsppercent"),$"sn.name".alias("snname"),$"sn.code".alias("sncode"),$"mjn.code".alias("mjncode"),$"mjn.name".alias("mjnname"),$"tn.name".alias("tnname"),$"tn.code".alias("tncode"),$"mjnc.code".alias("mjnccode"),$"mjnc.name".alias("mjncname"))
      .drop("docty","cdata","project_abstractcname","theme_namecode","project_abstract","sec","_id","sector","mjtheme", "theme1","majorsector_percent","mjn","sn","tn","mjnc","mjsp","sector_namecode","mjsector_namecode","mjtheme_namecode","sector1","sector2","sector3","sector4","proj","projectdocs")

    ndf.show()

/*    val url ="jdbc:oracle:thin:@//dboracle.cxrj9008hzf6.ap-southeast-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","myusername")
    oprop.setProperty("password","Erpanderp1")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    oprop.setProperty("createTableColumnTypes", "docty varchar(1000)")

    ndf.write.mode(SaveMode.Overwrite).jdbc(url,"jsondatawb",oprop)*/

    //--------------------------------------------
    spark.stop()
  }
}