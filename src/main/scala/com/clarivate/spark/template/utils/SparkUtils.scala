package com.clarivate.spark.template.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  private def getSparkConfiguration(appName: String): SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf
  }

  def getSparkContext(appName: String) = {
    val sc = SparkContext.getOrCreate(getSparkConfiguration(appName))
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext,
                          sc: SparkContext, batchDuration: Duration) = {

    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(dir) => StreamingContext.getActiveOrCreate(dir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}