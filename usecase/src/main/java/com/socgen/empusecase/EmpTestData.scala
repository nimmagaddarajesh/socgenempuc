package com.socgen.empusecase

import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object EmpTestData {

  /*
   * This function is to create the test data for Employee source(emp,department,empFin,empDept)
   * as per the required records dynamically and write the data into disk for further processing
   */

  def createEmpTestData(noOfRecords: Int, datasetPath: String)(spark: SparkSession) = {

    import spark.implicits._
    val empData = (1 to noOfRecords).toArray.map(x => (x, "FirtName_" + x, "LastName" + x, 18 + Random.nextInt(43), 10000 + Random.nextInt(90001), getDept(x, noOfRecords)))

    /*
     * Raw DataFrame
     */
    val df = spark.sparkContext.parallelize(empData)
      .toDF("EmpId", "FirstName", "LastName", "Age", "CTC", "DeptId")

    /*
     * 1. Department table generation
     */
    val department = df.select("DeptId").distinct().
      withColumn("DeptName", expr("case when DeptId = 1 then 'IT' when DeptId = 2 then 'INFRA' when DeptId = 3 then 'HR' when DeptId = 4 then 'ADMIN' when DeptId = 5 then 'FIN' end"))
    //department.show
    department.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(datasetPath + "/department/")

    /*
     * 2. Employee table generation
     */
    val emp = df.select("EmpId", "FirstName", "LastName", "Age")
    //emp.show
    emp.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(datasetPath + "/employee/")

    /*
     * 3. EmployeeFinance table generation
     */
    val empFin = df.select("EmpId", "CTC").withColumn("Basic", expr("CTC * 20 / 100"))
      .withColumn("PF", expr("CTC * 10 / 100")).withColumn("Gratuity", expr("CTC * 5 / 100"))
    //empFin.show
    empFin.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(datasetPath + "/empFin/")

    /*
     * 4. EmployeeDepartment table generation
     */
    val empDept = df.select("EmpId", "DeptId")
    //empDept.show
    empDept.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(datasetPath + "/empDept/")
  }

  /*
   * This function is to generate the department Id as per distribution
   * Sample --Distribute these 1000 emp to 5 departments, 500 emp works in 2 dept, rest 500 in 3 dept.
   */
  def getDept(x: Int, noOfRecords: Int): Int = {
    if (x <= noOfRecords / 2) 1 + Random.nextInt(2)
    else 3 + Random.nextInt(3)

  }

}