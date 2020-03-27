package com.socgen.empusecase

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SparkMain extends App {

  def createSparkObj(): SparkSession = {
    //Initializing SparkSession object
    SparkSession.builder
      .appName("Employee Data Processing")
      .master("local").getOrCreate()
  }

  val spark = createSparkObj()
  
  //Configuring log messages
  spark.sparkContext.setLogLevel("OFF")

  //Parameterized variables
  val datasetPath = "E:/mywork/eclipseWS/usecase/src/main/resources/"
  val noOfRecords = 1000
  
  try {
    println("=========================================")
    println("Creating testData for Employe Source")
    println(".........")
    //EmpTestData.createEmpTestData(noOfRecords, datasetPath)(spark)

    println("testData creation for Employe Source is Completed")

    println("=========================================")
    println("Read the data and create DataFrames")

    /*
   * Loading data into dataframe and create temp View
   * then generate the queries as per requirement and write
   * the results into disk
   */
    val options = Map("header" -> "true", "inferSchema" -> "true")
    val deptDf = spark.read.options(options)
      .csv(datasetPath + "/department/")
    val empDf = spark.read.options(options)
      .csv(datasetPath + "/employee/")
    val empDeptDf = spark.read.options(options)
      .csv(datasetPath + "/empDept/")
    val empFinanceDf = spark.read.options(options)
      .csv(datasetPath + "/empFin/")

    empDf.show()
    deptDf.show()
    empDeptDf.show()
    empFinanceDf.show()

    import spark.implicits._
    import org.apache.spark.sql.functions.col

    empDf.createOrReplaceTempView("employee")
    deptDf.createOrReplaceTempView("department")
    empFinanceDf.createOrReplaceTempView("empfinance")
    empDeptDf.createOrReplaceTempView("empdepartment")

    /* Query 1
   * Write a program to find emp with age > 40 & ctc > 30,000
   */
    val queryOneRes = empDf.as("employee").filter($"age" > 40)
      .join(empFinanceDf.as("empfinance"), $"employee.empId" === $"empfinance.empId", "inner")
      .filter($"empfinance.ctc" > 30000)
      .select($"employee.empId", $"employee.Age", col("empfinance.ctc"))

    println("=========================================")
    println("Sample records output for QueryOne")
    println("=========================================")
    queryOneRes.show()

    queryOneRes.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(datasetPath + "/queryOneRes/")

    println("QueryOne Results have been written to disk successfully")

    /*Query 2
     * Write a program to find dept with max emp with age > 35 & gratuity < 800
     */

    val queryTwoRes = spark.sql(s"""
select department.deptName,count(department.deptId) as totalCount from employee 
join empfinance ON employee.empId = empfinance.empId
join empdepartment ON employee.empId =empdepartment.empId
join department ON empdepartment.deptId = department.deptId
where (employee.age > 35 and empfinance.gratuity < 800)
group by(department.deptName) order by totalCount desc LIMIT 1
""")

    println("=========================================")
    println("Sample records output for QueryTwo")
    println("=========================================")
    queryTwoRes.show()
    queryTwoRes.coalesce(1).write.mode("overwrite").option("header", "true").csv(datasetPath + "/queryTwoRes/")

    println("QueryTwo Results have been written to disk successfully")
    spark.stop()

    println("******SparkSession terminated sucessfully**********")

  } catch {

    case (e: Throwable) =>
      println("Exception Occured!")
      println("Info from the exception: " + e.getMessage)
      spark.stop()
  }
}
