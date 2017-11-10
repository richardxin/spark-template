package com.clarivate.spark.template.batch.examples

import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object PartitionByColumn {
  def main(args: Array[String]) = {
    val appName = ""        //TODO: update constants here

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master("local[*]")               // TODO: comment out for EMR jobs
      .config("option", "values-here")  // TODO: add more config if needed
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val sqlContext = spark.sqlContext
    val applicationId = sc.applicationId
    println("Application started with id : " + applicationId)
    // -------------------------------------------------------
    // TODO: add your spark code here ...

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/example.csv")

    //df.show()
    val repartitioned = df.repartition($"state")

    repartitioned.show()

    for {
      //fetch the distinct states to use as filename
      distinctState <- df.dropDuplicates("state").collect.map(_ (2))
      //println(distinctState)
    } yield {
      import org.apache.spark.sql.functions.lit

      repartitioned.select("id", "name")
        .filter($"state" === lit(distinctState)) //filter df by state
        .coalesce(1)
        .sortWithinPartitions($"id") //sort
        .write.mode("overwrite").csv("output/" + distinctState) //save data
    }

    // -------------------------------------------------------
    spark.close()

  }
}
