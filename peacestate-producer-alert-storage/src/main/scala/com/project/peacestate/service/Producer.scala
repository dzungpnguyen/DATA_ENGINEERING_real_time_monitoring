package com.project.peacestate.service

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.project.peacestate.model.DroneReport
import com.project.peacestate.utility.Utils
import scala.annotation.tailrec

object Producer {
  def main(arg: Array[String]): Unit = {
    val topic = "drone-report-1"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "acks" -> "all",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val properties = new Properties()
    scalaMap.foreach{case(k,v) => properties.put(k,v)}
    
    val producer = new KafkaProducer[String, String](properties)

    val droneReports = List(
      DroneReport(1, 0.0, 0.0, List(), List()),
      DroneReport(2, 0.0, 0.0, List(), List()),
      DroneReport(3, 0.0, 0.0, List(), List()),
      DroneReport(4, 0.0, 0.0, List(), List()),
      DroneReport(5, 0.0, 0.0, List(), List())
    )
    val names = List(
      "John Smith",
      "Sarah Johnson",
      "Michael Williams",
      "Emily Brown",
      "Christopher Jones",
      "Jessica Davis",
      "Matthew Taylor",
      "Laura Anderson",
      "David Thomas",
      "Jennifer Wilson",
      "Daniel Martinez",
      "Nicole Anderson",
      "James Taylor",
      "Megan Clark",
      "Robert Rodriguez",
      "Hannah Walker",
      "William Lewis",
      "Lauren Green",
      "Joseph Turner",
      "Samantha Scott")
    val words = List(
      "Eye",
      "Cold",
      "Knife",
      "Apples",
      "Morning",
      "Nothing",
      "Operation",
      "Peaceful",
      "Illegal",
      "Robber",
      "Thanks",
      "Silly",
      "Blood",
      "Weed",
      "Gun")

    var i = 0

    @tailrec
    def produceData(): Unit = {
      droneReports.map(droneReport => Utils.generateReport(droneReport.droneId, names, words))
        .map(Utils.toJsonString)
        .foreach(droneReport => {
          val record = new ProducerRecord[String, String](topic, droneReport)
          producer.send(record)
          println(s"Record ${i+1} sent!")
          i = i + 1
        })
      Thread.sleep(3000)
      produceData()
    }

    produceData()

    producer.close()
  }
}


