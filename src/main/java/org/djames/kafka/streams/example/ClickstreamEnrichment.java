package org.djames.kafka.streams.example;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.djames.kafka.streams.example.model.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ClickstreamEnrichment {


    public static void main(String[] args) {

        String[] topics = {
                Constants.USER_TOPIC,
                Constants.PAGE_VIEW_DURATION_TOPIC,
                Constants.PAGE_VIEW_FILTERED_TOPIC,
                Constants.PAGE_VIEW_TOPIC,
                Constants.PAGE_VIEW_ENRICHED_TOPIC,
                Constants.PAGE_VIEW_CATEGORY_SUM_DURATION_TOPIC,
                Constants.PAGE_TOPIC
        };

        createTopics(topics);

      new Thread(() -> { GenerateData.generateData(); }).start();
      new Thread(() -> { startFilterPageviewStream("filter-pageview"); }).start();
      new Thread(() -> { startComputeDurationStream("compute-duration"); }).start();
      new Thread(() -> { startEnrichPageviewsStream("enrich-pageview");}).start();
      new Thread(() -> { startSumStream("sum-pageview");}).start();

    }


    public static void startFilterPageviewStream(String application_id) {

        Properties properties = generateProperties(application_id);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Pageview> stream = builder.stream(Constants.PAGE_VIEW_TOPIC);

        stream.filter((key,pageview) -> pageview.getUserID() != null)
                .to(Constants.PAGE_VIEW_FILTERED_TOPIC);

        Topology topology = builder.build();

        KafkaStreams  streams = new KafkaStreams(topology, properties);
        startStreams(streams);
    }

    public static void startComputeDurationStream(String application_id) {


        Properties properties = generateProperties(application_id);


        StoreBuilder<KeyValueStore<String, byte[]>> pageviewStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("PageviewStore"),
                Serdes.String(),
                Serdes.ByteArray())
                .withCachingEnabled();

        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(pageviewStore);

        KStream<String,Pageview> pageviewStream = builder.stream(Constants.PAGE_VIEW_FILTERED_TOPIC)
                .transformValues(new PageViewDurationTransformer(), "PageviewStore")
                .filter((key,pageview) -> pageview != null);

        pageviewStream.to(Constants.PAGE_VIEW_DURATION_TOPIC);

        Topology topology = builder.build();

        KafkaStreams  streams = new KafkaStreams(topology, properties);
        startStreams(streams);
    }

    public static void startEnrichPageviewsStream(String application_id) {

        Properties properties = generateProperties(application_id);

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Page> Pages = builder.table(Constants.PAGE_TOPIC);

        KStream<String, Pageview> stream =
                builder.stream(Constants.PAGE_VIEW_DURATION_TOPIC);

        KStream<String, PageviewEnriched> stream_enriched =
                stream.filter((key,pageview) -> pageview.getPageID() != null)
                        .map(new KeyPageviewByPageIDMapper())
                        .leftJoin(Pages, getPageJoiner());

        stream_enriched.to(Constants.PAGE_VIEW_ENRICHED_TOPIC);

        Topology topology = builder.build();
        KafkaStreams  streams = new KafkaStreams(topology, properties);
        startStreams(streams);
    }

    public static void startSumStream(String application_id) {

        Properties properties = generateProperties(application_id);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, PageviewEnriched> stream =
                builder.stream(Constants.PAGE_VIEW_ENRICHED_TOPIC);

        KStream<String, Long> stream_mapped =
                stream.filter((key,pageview) -> pageview.getCategory() != null)
                        .map(new GroupPageviewByCategoryDurationMapper());

        KTable <Windowed<String>,Long> stream_counted = stream_mapped
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .reduce(Long::sum);

        KStream <Windowed<String>, Long> stream_counted_string = stream_counted
                .toStream();

        KStream <String, String> stream_counted_string_to =
                stream_counted_string.map(new WindowedStringMapper());

        stream_counted_string_to.to(Constants.PAGE_VIEW_CATEGORY_SUM_DURATION_TOPIC,Produced.valueSerde(Serdes.String()));


        Topology topology = builder.build();
        KafkaStreams  streams = new KafkaStreams(topology, properties);
        startStreams(streams);
    }


    private static Properties generateProperties(String application_id) {

        Properties properties = generateProperties();
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, application_id);

        return properties;

    }

    private static Properties generateProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put("schema.registry.url","http://localhost:8081");
        return properties;
    }

    private static void startStreams(KafkaStreams  streams){
        System.out.println("Press CTRL-C to abort stream");
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (streams != null)
                    streams.close();
            }
        });
        streams.cleanUp();
        streams.start();
    }

    private static void createTopics(String[] topics){

        Properties properties = generateProperties();
        AdminClient adminClient = AdminClient.create(properties);

        List<NewTopic> newTopics = new ArrayList<NewTopic>();

        for(String topic: topics){
            newTopics.add(new NewTopic(topic, 1, (short)1));
        }

        adminClient.createTopics(newTopics);
        adminClient.close();
    }

    private static ValueJoiner<Pageview,Page, PageviewEnriched> getPageJoiner() {

        return (pageview, page) ->

                PageviewEnriched.newBuilder()
                        .setUserID(pageview.getUserID())
                        .setPageID(pageview.getPageID())
                        .setTimestamp(pageview.getTimestamp())
                        .setDuration(pageview.getDuration())
                        .setCategory(page != null ? page.getCategory() : null)
                        .build();
    }



}


class PageViewDurationTransformer implements ValueTransformerWithKeySupplier{

    @Override
    public ValueTransformerWithKey get(){
        return new PageViewDurationTransform();
    }

}


class PageViewDurationTransform implements ValueTransformerWithKey<String, Pageview,Pageview> {
    private KeyValueStore<String, byte[]> state;

    @Override
    public void init(final ProcessorContext context) {
        state = (KeyValueStore<String, byte[]>) context.getStateStore("PageviewStore");
    }

    @Override
    public Pageview transform(final String key, final Pageview pageview) {

        byte[] prevPageviewByte = state.get(key);

        try {
            state.put(key,Pageview.getEncoder().encode(pageview).array());

        if (prevPageviewByte  != null) {
            Pageview prevPageview = Pageview.getDecoder().decode(prevPageviewByte);
            long duration = pageview.getTimestamp() - prevPageview.getTimestamp();
            prevPageview.setDuration(duration);
            return prevPageview;
        }

        return null;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {

    }
}

class GroupPageviewByCategoryDurationMapper implements KeyValueMapper<String, PageviewEnriched, KeyValue<String, Long>> {

    @Override
    public KeyValue<String, Long> apply(String key, PageviewEnriched pageview) {
        return new KeyValue<>(pageview.getCategory().toString(), pageview.getDuration());
    }
}

class KeyPageviewByPageIDMapper implements KeyValueMapper<String, Pageview, KeyValue<String, Pageview>>{

    @Override
    public KeyValue<String, Pageview> apply(String key, Pageview pageview) {
        return new KeyValue<>(Integer.toString(pageview.getPageID()), pageview);
    }
}

class WindowedStringMapper implements KeyValueMapper<Windowed<String>, Long, KeyValue<String, String>> {

    @Override
    public KeyValue<String, String> apply(Windowed<String> key, Long value) {
        return new KeyValue<>(
                key.toString()
                ,value.toString());
    }
}



