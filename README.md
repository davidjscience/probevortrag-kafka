# probevortrag-sparsity

This repository contains the example for the usage of Kafka Streams that used in my lecture on
*Real-Time Stream Processing with Apache Kafka*. 



Overview:
--------
In this example, data simulation clickstream events, namely pageviews get generated an written to the kafka topic *pageview*.
Then, several transformations to this data is performed usind *kafka streams*.


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








--------
This example takes 2 streams of data: Stream of searches and stream of user clicks
It also takes a stream of profile-updates, representing changes to a profiles table (assume we are getting those changes from MySQL using KafkaConnect connector)

It joins those activity streams together, to generate an holistic view of user activity. The results show you, in one record the user's location, interests, what they searched for and what they ended up clicking. Providing rich source of data for analysis - which products are more desirable for which audience? "users who searched for this also looked at..." and other data products.

This example makes use of the unique windowed-join, allowing us to match clicks with the search that happened in the same time window - in order to generate relevant results.

To run:
--------

0. Build the project with `mvn package`, this will generate an uber-jar with the streams app and all its dependencies.
1. Next, we need to generate some clicks, searches and profiles. Run the generator. It should take about 5 seconds to run. Don't worry about complete lack of output... 
   `$ java -cp target/uber-kafka-clickstream-enrich-1.0-SNAPSHOT.jar com.shapira.examples.streams.clickstreamenrich.GenerateData`
2. Run the streams app:
   `java -cp target/uber-kafka-clickstream-enrich-1.0-SNAPSHOT.jar com.shapira.examples.streams.clickstreamenrich.ClickstreamEnrichment`
   Streams apps typically run forever, but this one will just run for a minute and exit
3. Check the results:
   `bin/kafka-console-consumer.sh --topic clicks.user.activity --from-beginning --bootstrap-server localhost:9092  --property print.key=true`

If you want to reset state and re-run the application (maybe with some changes?) on existing input topic, you can:

1. Reset internal topics (used for shuffle and state-stores):

    `bin/kafka-streams-application-reset.sh --application-id clicks --bootstrap-servers localhost:9092 --input-topics clicks.user.profile,clicks.pages.views,clicks.search `

2. (optional) Delete the output topic:

    `bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic clicks.user.activity`