package com.clarivate.spark.template.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Minutes, Seconds, StateSpec, StreamingContext}

import com.clarivate.spark.template.utils.SparkUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Template {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSparkContext("Logs lambda speed")

    implicit val sqlContext = SparkUtils.getSQLContext(sc)

    val batchDuration = Seconds(5)
    implicit val ssc = new StreamingContext(sc, batchDuration)

    val job = new StreamingJob()

    ssc.start()
    ssc.awaitTermination()
  }
}

