package day2Assignments

import org.apache.spark
import org.apache.spark.sql.SparkSession

object SparkSQL {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(name = "First Spark project with scala")
      .master(master = "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    val df_personalData = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/dataset/employee/Employee_personal_details.csv")

    //df_personalData.show()

    val df_businessData = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/dataset/employee/Employee_Business_Details.csv")

    //df_businessData.show()

    // Register the DataFrame as a SQL temporary view
    df_personalData.createOrReplaceTempView("empPersonal")
    df_businessData.createOrReplaceTempView("empBusiness")

    //
    val sql_age = sparkSession.sql("SELECT * FROM empPersonal WHERE AgeinYrs BETWEEN 30.00 AND 40.00")
    //sql_age.show(50,false)

    val sql_avgSalary = sparkSession.sql("SELECT AVG(Salary) FROM empBusiness")
    sql_avgSalary.show()

    val sql_minSalary = sparkSession.sql("SELECT MIN(Salary) FROM empBusiness")
    sql_minSalary.show()

    val sql_maxSalary = sparkSession.sql("SELECT MAX(Salary) FROM empBusiness")
    sql_maxSalary.show()

    //
    //df_businessData.groupBy("Year_of_Joining").count().show()
    df_businessData.groupBy("Year_of_Joining").count().sort("count").show(50)
  }
}
