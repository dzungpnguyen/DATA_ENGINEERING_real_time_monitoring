package com.project.peacestate.utility

import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import scala.util.Random
import com.project.peacestate.model.{DroneReport, Citizen}

object Utils {
  def generateReport(droneId: Int, names: List[String], words: List[String]): DroneReport = {
          // .filter(col("latitude") >= 48.8156 && col("latitude") <= 48.9022 && col("longitude") >= 2.225 && col("longitude") <= 2.4699)

    val minLatitude = 48.7
    val maxLatitude = 49.0
    val minLongitude = 2.1
    val maxLongitude = 2.5
    val randomLatitude = minLatitude + Random.nextDouble() * (maxLatitude - minLatitude)
    val randomLongitude = minLongitude + Random.nextDouble() * (maxLongitude - minLongitude)
    // val randomLatitude = Random.nextDouble * 90
    // val randomLongitude = Random.nextDouble * 180
    val randomCitizens = names
      .map(name => Citizen(name, Random.nextDouble * 100))
      // .take(Random.nextInt(names.length))
      .filter(citizen => Random.nextDouble < 0.5)
    val randomWords = words
      .filter(word => Random.nextDouble < 0.5)
      // .take(Random.nextInt(words.length))
    DroneReport(droneId, randomLatitude, randomLongitude, randomCitizens, randomWords)
  }

  def toJsonString(droneReport: DroneReport): String = {
    val citizensAsJson = droneReport.citizens
      .map(citizen =>
        // s"""{"name":"${citizen.name}", "peacescore":${citizen.peacescore}}""")
        s"{\"name\":\"${citizen.name}\", \"peacescore\":${citizen.peacescore}}")
      .mkString("[", ",", "]")
    val wordsAsJson = droneReport.words
      .map(word => s"\"$word\"")
      .mkString("[", ",", "]")
    // s"{"droneId":${droneReport.droneId}, "latitude":${droneReport.latitude}, "longitude":${droneReport.longitude}, "citizens":$citizensAsJson, "words":$wordsAsJson}"}
    s"{\"droneId\":${droneReport.droneId},\"latitude\":${droneReport.latitude},\"longitude\":${droneReport.longitude},\"citizens\":${citizensAsJson},\"words\":${wordsAsJson}}"
  }
  
  def fromJsonString(reportJson: String): DroneReport = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = parse(reportJson)
    json.extract[DroneReport]
  }
}
