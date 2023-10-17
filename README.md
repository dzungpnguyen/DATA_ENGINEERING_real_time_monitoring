# Drone Surveillance System

This project simulates the operation of drones in the Peace State, where they observe citizens and collect data to maintain peace and security. The system facilitates real-time monitoring of citizens and analyzes their behavior using various components.

## Components

The project consists of the following components:

1. **Producer**: This component simulates the drones by sending reports each 3 seconds. Each report contains the drone's ID, its current location, the citizens around it along with their peace scores, and words in its surroundings.

2. **Alert**: The alert system identifies citizens with a peace score below 30 and generates alert messages.

3. **Storage**: The messages sent by the drones are stored in a JSON file format in the HDFS.

4. **Analysis**: The JSON file stored in HDFS is utilized by the analysis component, which leverages Apache Spark for processing and extracting insights.

## How To Run The Projet

1. Clone the project
```
git clone https://github.com/ArnaudCP8/Spark-Project.git
```

2. Start the Zookeeper service and Kafka Broker service
- Move to your Kafka installation directory
- Open a terminal and start the Zookeeper service 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
- Open another terminal and start the Kafka Broker service
```
bin/kafka-server-start.sh config/server.properties
```

3. Start the components
```
cd Spark-Project/peacestate-producer-alert-storage
sbt run
```
- Choose `3` for **Producer**
- Choose `1` for **Alert**
- Choose `2` for **Storage**

- To run **Analysis**
```
cd Spark-Project/peacestate-analysis
sbt run
```