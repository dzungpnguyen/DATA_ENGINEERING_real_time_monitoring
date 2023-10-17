package com.project.peacestate.service

import java.util.Properties
import java.util.Collections.singletonList
import scala.io.AnsiColor._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.FSDataOutputStream

object ConsumerStorage {
  def main(args: Array[String]): Unit = {
    val topic = "drone-report-1"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "myStorage",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val properties = new Properties()
    scalaMap.foreach { case (k, v) => properties.put(k, v) }

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(singletonList(topic))

    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(conf)

    val storagePath = new Path("/drone-report/reports.json")
    val output: FSDataOutputStream = fs.create(storagePath, true)

    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(100)
        records.forEach(record => {
          println("--------------------------------------------------")
          println(s"${GREEN}Received record:${RESET} ${record.value}")
          output.write(record.value.getBytes("UTF-8"))
          output.write("\n".getBytes("UTF-8"))
        })
        output.hflush() // Flush the data into HDFS to ensure that it is correctly stored.
      }
    } catch {
      case e: Exception =>
        System.err.println("Error while writing data into HDFS: " + e.getMessage)
    } finally {
      output.close()
      consumer.close()
      fs.close()
    }
  }
}