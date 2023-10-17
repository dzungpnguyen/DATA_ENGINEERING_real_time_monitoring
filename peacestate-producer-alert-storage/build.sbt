name := "PeaceState Project"
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"
version := "1.0"

scalaVersion := "2.13.8"
val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.8"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.5.0"