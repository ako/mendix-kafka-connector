# Mendix Kafka Connector

## Description

Kafka works using a cluster of Kafka nodes which store the streams of records in categories called topics. Streams of records can be read by multiple consumers from multiple consumer groups. This Kafka connector includes microflow actions and a config portal for all basic operations:

* Startup consumers which ingest data from a certain Kafka topic using a specified microflow. Basically multiple consumers, means the processing of the message by the specified microflow is divided over multiple threads. Consumers from the same consumer group work together to ingest the messages.
* Send messages to a certain Kafka topic. You can use the key parameter to make sure a message always lives within the same Kafka partition, so that it is alway processed by the same consumer.

## Typical usage scenario

Kafka is used for building real-time data pipelines and streaming apps. It is horizontally scalable, fault-tolerant, wicked fast, and runs in production in thousands of companies. In the Internet-of-Things (IoT) domain Kafka is often used to stream the data of your IoT devices into other systems for further processing, analyses and storage.

## Features and limitations

Use the provided microflow activities to receive and send data. Make sure you include the shutdown activity in your before shutdown microflow to make sure the processes for receiving and sending are properly finished. Additionally you can use the control panel to start/stop consumers manually.

![Microflow activities](https://raw.githubusercontent.com/blockbax/mendix-kafka-connector/master/content/microflow-activities.png)

## Configuration

Set the constant BootstrapServers to the locations (including port number) of your Kafka servers and you are ready to go.
