package com.clarivate.spark.template.streaming

import com.clarivate.spark.template.config.Settings
import org.apache.spark.streaming.{Seconds, StreamingContext}
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

  def create(ssc: StreamingContext) = KafkaUtils.createDirectStream(ssc,
    PreferConsistent,
    Subscribe[String, String](Set(Settings.Kafka.topic), kafkaParams))

  def create(ssc: StreamingContext, topics: String) = KafkaUtils.createDirectStream(ssc,
    PreferConsistent,
    Subscribe[String, String](Set(topics), kafkaParams))
}