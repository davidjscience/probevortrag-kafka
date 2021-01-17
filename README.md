# probevortrag-sparsity

This repository contains the example for Kafka Streams data processing that I used in my lecture on
*Real-Time Stream Processing with Apache Kafka*. 



Overview:
--------
In this example, we simulate clickstream events, namely pageviews. This pageviews get are continously generated and are written to the kafka topic *pageview*. Then, several transformations to this data is performed usind *kafka streams*.


To run:
--------

This example is build on top of the confluent-platform Kafka distribution. To get startet, it is recommended to start a local confluent-platform instance using Docker. You can get the corresponding docker-compose.yml [here](https://github.com/confluentinc/cp-all-in-one/tree/6.0.1-post/cp-all-in-one). The confluent-platform than can be started using 

```bash
docker-compose up
```

This will start all necessary services. If the Kafka cluster is up and running, you can execute the example using the jar file in the artifacts as

```bash
 java -jar kafka-clickstream-enrich.jar
```
The application assumes that you are using the standard ports for each of the confluent services. After that, you can have a look at the messages created in the topics using the kafka control center, which should be available under

```
http://localhost:9021/

```

Notes on editing the example:
--------

If you wish to modify the sourcecode and implement your own examples, I would suggest you to use an IDE, preferebly IntelliJ IDEA.
All necessary dependencies can be resolved using the provided pom.xml file via apache maven. All POJO classes which are used for serializing and deserializing data are build using apache avro. To this end, all classes found in

```
/src/main/java/org/djames/kafka/streams/example/model
```

are generated via *maven* using the target *generate-sources* from the corresponding avro schme files to be found in

```bash
/src/main/avro/org/djames/kafka/streams/example/model
```
For the serialization/deserialization to work properly, the kafka schema registry is used, which will also be started automatically via *docker-compose*.