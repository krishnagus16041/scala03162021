name := "scala03162021"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

//libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.7" % "provided"


// https://mvnrepository.com/artifact/org.apache.flink/flink-table-planner
libraryDependencies += "org.apache.flink" %% "flink-table-planner" % "1.12.2" % "provided"

