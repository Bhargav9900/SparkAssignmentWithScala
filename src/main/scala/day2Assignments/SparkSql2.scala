package day2Assignments

import org.apache.spark.sql.SparkSession

object SparkSql2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(name = "First Spark project with scala")
      .master(master = "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    val df_personalData = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/new_dataset/employee/employee_personal_details.csv")

    df_personalData.show()

    val df_businessData = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/new_dataset/employee/employee_business_details.csv")

    df_businessData.show()

    // Register the DataFrame as a SQL temporary view
    df_personalData.createOrReplaceTempView("empPersonal")
    df_businessData.createOrReplaceTempView("empBusiness")

    //use union to merge both csv
    //val df_allData = df_personalData.unionByName(df_businessData)
    //val df_join = df_personalData.join(df_businessData)
    //df_join.show()

    //creating temp view for merged data
    //df_join.createOrReplaceTempView("empData")

    //Employees having AgeinYrs between 30.00 and 40.00,average salary
    //val sql_avgSalary = sparkSession.sql("SELECT AVG(Salary) FROM empData WHERE AgeinYrs BETWEEN 30.00 AND 40.00")
    //sql_avgSalary.show()

    println("Que 11-a")
    val sql_avgSalary = sparkSession.sql("SELECT AVG(CAST(eb.Salary AS float)) AS Avrage FROM empPersonal AS ep JOIN empBusiness AS eb ON ep.Emp_ID = eb.Emp_ID WHERE ep.AgeinYrs BETWEEN 30.00 AND 40.00")
    //sql_avgSalary.show()

    val sql_minSalary = sparkSession.sql("SELECT MIN(CAST(eb.Salary AS float)) AS Minimum FROM empPersonal AS ep JOIN empBusiness AS eb ON ep.Emp_ID = eb.Emp_ID WHERE ep.AgeinYrs BETWEEN 30.00 AND 40.00")
    //sql_minSalary.show()

    val sql_maxSalary = sparkSession.sql("SELECT MAX(CAST(eb.Salary AS float)) AS Maximum FROM empPersonal AS ep JOIN empBusiness AS eb ON ep.Emp_ID = eb.Emp_ID WHERE ep.AgeinYrs BETWEEN 30.00 AND 40.00")
    //sql_maxSalary.show()

    println("Que 11-b")
    val sql_noOfEmp = sparkSession.sql("SELECT COUNT(*) AS No_of_Emp,Year_of_Joining FROM empBusiness GROUP BY Year_of_Joining")
    //sql_noOfEmp.show()

    println("Que 11-c")
    val sql_noOfEmpOrdByCount = sparkSession.sql("SELECT COUNT(*) AS No_of_Emp,Year_of_Joining FROM empBusiness GROUP BY Year_of_Joining ORDER BY No_of_Emp")
    //sql_noOfEmpOrdByCount.show()

    println("Que 11-d")

    val sql_HikeSalary = sparkSession.sql("SELECT Emp_ID,CAST(Salary AS float) AS Current_Salary,LastHike,CAST((Salary-((Salary*REPLACE(LastHike,'%',''))/100)) AS float) AS Pre_Salary FROM empBusiness")
    //sql_HikeSalary.show()

    //sql_HikeSalary.write.csv("hdfs://localhost:54310/new_dataset/employee/filterData/salaryHike")

    println("Que 11-e")
    val sql_avgWeight = sparkSession.sql("SELECT AVG(CAST(ep.WeightinKgs AS float)) AS weightAvg FROM empPersonal AS ep JOIN empBusiness AS eb ON ep.Emp_ID = eb.Emp_ID WHERE eb.DOW_of_Joining='Monday' OR eb.DOW_of_Joining='Wednesday' OR eb.DOW_of_Joining='Friday'")
    //sql_avgWeight.show()

    //store all display record in hdfs
    //sql_avgSalary.write.csv("hdfs://localhost:54310/new_dataset/employee/filterData/AvgWeight")

    val df_addressData = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://localhost:54310/dataset/employee/employee_address_details.csv")

    df_addressData.show()

    df_addressData.createOrReplaceTempView("empAddress")

    println("Que 11-f")
    val sql_empOfAK = sparkSession.sql("SELECT ea.Emp_ID,eb.First_Name,eb.Last_Name,ea.State,CAST(ep.DateofBirth) FROM empPersonal AS ep JOIN empBusiness AS eb ON ep.Emp_ID = eb.Emp_ID JOIN empAddress AS ea ON ep.Emp_ID = ea.Emp_ID WHERE ea.State='AK' AND ep.DateofBirth > '01/01/1980'")
    sql_empOfAK.show()

    println("Que 11-g")
    val sql = sparkSession.sql("SELECT COUNT(*) AS num,State FROM empAddress GROUP BY State ORDER BY num DESC limit 1")
    //sql.show()
    sql.createOrReplaceTempView("tempState")

    val sql_maxW = sparkSession.sql("SELECT MAX(ep.WeightinKgs) FROM empPersonal AS ep JOIN empAddress AS ea ON ep.Emp_ID = ea.Emp_ID WHERE ea.State = (SELECT State FROM tempState limit 1)")
    sql_maxW.show()
  }
}
