package com.clarivate.spark.template.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Template1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("app_name")   //TODO

    val sc = SparkContext.getOrCreate(conf)

    //implicit val sqlContext = SparkUtils.getSQLContext(sc)

    val batchDuration = Seconds(5)
    implicit val ssc = new StreamingContext(sc, batchDuration)

    val kafkaParams = Map(
      "metadata.broker.list" -> "Settings.Kafka.broker",  //TODO
      "group.id" -> "Settings.Kafka.groupId", //TODO
      "auto.offset.reset" -> "latest" //TODO
    )

    val topics = Array("topic1", "topic2")

    val stream = KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))


    //TODO: process stream here


    ssc.start()
    ssc.awaitTermination()
  }
}
