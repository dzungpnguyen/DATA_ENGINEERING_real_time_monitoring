package com.project.peacestate.service

import scala.io.AnsiColor._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object Analysis {  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("peacestate-analysis")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val hdfsPath = "hdfs://localhost:9000/drone-report/reports.json"
    val df = spark.read.json(hdfsPath).cache()

    generalInfo(df)

    println(s"${GREEN}--------------------------${RESET}")
    println(s"${GREEN}PEACESTATE ANALYSIS")
    println(s"${GREEN}--------------------------${RESET}")

    averagePeaceScore(df)

    averagePeaceScoreInParis(df)

    mostDangerousPeople(df)

    mostHeardWords(df)

    spark.stop()
  }

  def generalInfo(df: DataFrame): Unit = {
    println(s"${GREEN}--------------------------${RESET}")
    println(s"${GREEN}GENERAL INFORMATION OF RECEIVING REPORTS${RESET}")
    println(s"${GREEN}--------------------------${RESET}")
    
    println(s"${MAGENTA}1. Dataframe${RESET}")
    df.show()

    println(s"${MAGENTA}1. Schema${RESET}")
    df.printSchema()
  }

  def averagePeaceScore(df: DataFrame): Unit = {
    val averagePeaceScore = df
      .select(explode(col("citizens.peacescore")).as("score"))
      .select(avg("score"))
      .head()
      .getDouble(0)
    println(s"${MAGENTA}1. The average Peace Score of the State${RESET}")
    println(s"${MAGENTA}$averagePeaceScore${RESET}")
  }

  def averagePeaceScoreInParis(df: DataFrame): Unit = {
    val averagePeaceScoreInParis = df
      .filter(col("latitude") >= 48.8156 && col("latitude") <= 48.9022 && col("longitude") >= 2.225 && col("longitude") <= 2.4699)
      .select(explode(col("citizens.peacescore")).as("average_score_in_Paris"))
      .head()
      .getDouble(0)
    println(s"${MAGENTA}2. The average Peace Score in Paris${RESET}")
    println(s"${MAGENTA}$averagePeaceScoreInParis${RESET}")
  }

  def mostDangerousPeople(df: DataFrame): Unit = {
    val mostDangerousPeople = df
      .select(explode(col("citizens")).as("citizen"))
      .filter(col("citizen.peacescore") <= 30)
      .groupBy("citizen.name")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))
      .select("name")
      .limit(3)
      .collect()
      .map(_.getString(0))
    println(s"${MAGENTA}3. Top 3 most dangerous people${RESET}")
    println(s"${MAGENTA}${mostDangerousPeople.mkString(", ")}${RESET}")
  }

  def mostHeardWords(df: DataFrame): Unit = {
    val mostHeardWords = df
      .filter(col("latitude") >= 48.8156 && col("latitude") <= 48.9022 && col("longitude") >= 2.225 && col("longitude") <= 2.4699)
      .select(explode(col("words")).as("word"))
      .groupBy("word")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))
      .select("word")
      .limit(5)
      .collect()
      .map(_.getString(0))
    println(s"${MAGENTA}4. Most heard words in Paris${RESET}")
    println(s"${MAGENTA}${mostHeardWords.mkString(", ")}${RESET}")
  }
}