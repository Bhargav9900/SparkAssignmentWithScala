package day1Assignments

import org.apache.spark.sql.SparkSession

object ReadHDFSFile {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(name = "First Spark project with scala")
      .master(master = "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    //read hdfs file
    val df = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/dataset/employee/employee_address_details.csv")

    //show 20 records from hdfs file
    df.show()

    //store all display record in hdfs
    df.write.csv("hdfs://localhost:54310/dataset/employee/StoredRecord")

    //filter records
    df.filter(df("Region") === "Northeast").show()
    df.filter(df("Region") === "South").show()
    df.filter(df("Region") === "Midwest").show()
    df.filter(df("Region") === "West").show()
    df.filter(df("Region") === "East").show()

    //store data region wise
    val df_ne = df.filter(df("Region") === "Northeast")
    df_ne.write.csv("hdfs://localhost:54310/dataset/employee/empdata/Region=Northeast")

    val df_s = df.filter(df("Region") === "South")
    df_s.write.csv("hdfs://localhost:54310/dataset/employee/empdata/Region=South")

    val df_mw = df.filter(df("Region") === "Midwest")
    df_mw.write.csv("hdfs://localhost:54310/dataset/employee/empdata/Region=Midwest")


    //using partition
    df.write.partitionBy("Region").csv("hdfs://localhost:54310/dataset/employee/empdataPartition")
  }
}
