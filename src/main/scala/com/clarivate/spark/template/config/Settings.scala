package com.clarivate.spark.template.config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  object Kafka {
    private val kafka = config.getConfig("kafka")

    lazy val topic = kafka.getString("topic")
    lazy val broker = kafka.getString("broker")
    lazy val groupId = kafka.getString("group_id")
    lazy val offsetReset = kafka.getString("offset_reset")
  }

  object batch {
    private val batch = config.getConfig("batch")
   lazy val batchDuration = batch.getInt("batch_duration")

  }
}
