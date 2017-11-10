package com.clarivate.spark.template.batch.examples

import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession

object ApacheLogReader {
    private val logRegex = """^(\S+) - - \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)\/\S+" (\S+) (\S+)"""
    private val p = Pattern.compile(logRegex)

    def main(args: Array[String]) = {
    // 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
    // 1:IP 2:client 3:user 4:date time 5:method 6:req 7:proto 8:respcode 9:size
    val demoString = """64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846"""

    val appName = "ApacheLogReader"        //TODO: update constants here

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master("local[*]")               // TODO: comment out for EMR jobs
      .config("spark.shuffle.compress", "true")  // TODO: add more config if needed
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //val sqlContext = spark.sqlContext
    val applicationId = sc.applicationId
    println("Application started with id : " + applicationId)

    // ---------------------------------------------------------------------------------------
    // TODO: add your spark code here ...

    val colNames = Seq("ip", "client", "user", "date_time", "action", "proto", "resp_code","size")  // 1:IP 2:client 3:user 4:date time 5:method 6:req 7:proto 8:respcode 9:size

    val data = spark.read
			.option("header", "false")
			.option("delimiter", " ")
			.option("nullValue", "-")
			.csv("data/apache.log")
			.toDF(colNames: _*)
		data.show
		println("response_code group by count =====")
        data.groupBy("resp_code").count.show

    // -------------------------------------------------------
    spark.close()

  }
}
