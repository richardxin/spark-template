package com.clarivate.spark.template.batch

import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main {
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





    // -------------------------------------------------------
    spark.close()

  }
}
