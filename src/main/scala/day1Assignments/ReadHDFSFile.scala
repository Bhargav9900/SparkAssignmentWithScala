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

    val df = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/dataset/employee/employee_address_details.csv")

    df.show()

  }
}
