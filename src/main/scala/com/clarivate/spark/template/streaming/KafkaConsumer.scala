package com.clarivate.spark.template.streaming

import com.clarivate.spark.template.config.Settings
import org.apache.spark.streaming.{StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

trait KafkaConsumer {

  val ssc: StreamingContext

  private val kafkaParams = Map(
    "metadata.broker.list" -> Settings.Kafka.broker,
    "group.id" -> Settings.Kafka.groupId,
    "auto.offset.reset" -> Settings.Kafka.offsetReset
  )

  def createStream(ssc: StreamingContext) = KafkaUtils.createDirectStream(ssc,
    PreferConsistent,
    Subscribe[String, String](Set(Settings.Kafka.topic), kafkaParams))

  def createStream(ssc: StreamingContext, topics: Set[String]) = KafkaUtils.createDirectStream(ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))
}