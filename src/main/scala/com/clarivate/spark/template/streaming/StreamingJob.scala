package com.clarivate.spark.template.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Minutes, Seconds, StateSpec, StreamingContext}


  class StreamingJob(implicit val ssc: StreamingContext, val sqlContext: SQLContext)
    extends KafkaConsumer {

    def process() {
      val theStream = createStream(ssc)
      // TODO: add logic here
    }
}
