 package com.project.peacestate.service

import java.util.Properties
import java.util.Collections.singletonList
import scala.collection.JavaConverters._
import scala.io.AnsiColor._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import com.project.peacestate.model.{DroneReport, Citizen}
import com.project.peacestate.utility.Utils

object ConsumerAlert {
  def main(arg: Array[String]): Unit = {
    val topic = "drone-report-1"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "myGroup",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val properties = new Properties()
    scalaMap.foreach{case(k,v) => properties.put(k,v)}

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(singletonList(topic))

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      records.forEach(record => {
        println("--------------------------------------------------")
        println(s"${GREEN}Received record:${RESET} ${record.value}")
        val droneReport = Utils.fromJsonString(record.value)
        droneReport.citizens.foreach(citizen => citizen match {
          case Citizen(name, peacescore) if peacescore < 30.0 => println(s"${RED}ALERT: $name's peace score is lower than 30!!!${RESET}")
          case _ => // do nothing
        })
      })
    }
  }
}