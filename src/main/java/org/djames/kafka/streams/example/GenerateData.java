package org.djames.kafka.streams.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import  com.github.javafaker.Faker;
import org.djames.kafka.streams.example.model.Page;
import org.djames.kafka.streams.example.model.Pageview;
import org.djames.kafka.streams.example.model.User;

import java.util.concurrent.ThreadLocalRandom;


/**
 * This class will generate fake clicks, fake searches and fake profile updates
 * For simplicity, we will actually generate very few events - 2 profiles, update to one profile, 3 searches, 5 clicks
 */



public class GenerateData {

    private static Faker faker;
    private static Properties props;
    private static KafkaProducer<Integer, String> producer;

    public static void generateData() {

        // add shutdown hook

        System.out.println("Press CTRL-C to stop generating data");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
            }
        });


        faker = new Faker();
        props = new Properties();

        props.put("bootstrap.servers", Constants.BROKER);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url","http://localhost:8081");

        // Starting producer
        producer = new KafkaProducer<Integer, String>(props);


        generateUsers(100);
        generatePages(100);
        continuouslyGeneratePageViews(300,1000, 1);
    }


    public static void generateUsers(int n){

        List<ProducerRecord> records = new ArrayList<ProducerRecord>();

        for(int i = 1;i<n+1;i++) {
            String userName = faker.name().name();
            String state = faker.address().stateAbbr();
            String zipcode = faker.address().zipCodeByState(state);
            User user = User.newBuilder()
                    .setUserID(i).setUserName(userName)
                    .setState(state).setZipcode(zipcode).build();
            records.add(new ProducerRecord(Constants.USER_TOPIC, Integer.toString(i), user));
        }

        produceRecords(records);

    }

    public static void generatePages(int n){

        List<ProducerRecord> records = new ArrayList<ProducerRecord>();

        String[] categories = {"Politik","Gesellschaft","Wirtschaft"
                                ,"Kultur","Wissenschaft","Sport"};
        int[] boundaries = {15,35,50,65,75,100};

        int categoryCounter = 0;


        for(int i = 1;i<n+1;i++) {

            categoryCounter = i> boundaries[categoryCounter] ? categoryCounter+1 : categoryCounter;

            String category = categories[categoryCounter];
            Page page = Page.newBuilder()
                    .setPageID(i).setCategory(category).build();
            records.add(new ProducerRecord(Constants.PAGE_TOPIC, Integer.toString(i), page));
        }

        produceRecords(records);

    }



    public static void continuouslyGeneratePageViews(int lower, int meanPageviewsPerSecond, int batchsize){


        while(true){
            generatePageViews(meanPageviewsPerSecond,batchsize);
            sleep(batchsize*1000);
        }


    }

    public static void generatePageViews(int n, int lower) {

        List<ProducerRecord> records = new ArrayList<>();
        List<Timestamp> timestamps = new ArrayList<>();

        for (int i = 1; i < n+1; i++){
            timestamps.add( generateRandomTimestamp(lower));
        }
        //sort timestamps
        timestamps.sort(Comparator.comparing(e -> e.getTime()));

        for (int i = 1; i < n+1; i++) {
            int userid = generateRandomInt(1, 110);
            Integer userID = userid >100 ? null : userid;
            int pageid = generateRandomInt(1, 110);
            Integer pageID = pageid >100 ? null : pageid;
            long timestamp = timestamps.get(i-1).getTime();

           Pageview pageview = Pageview.newBuilder().setUserID(userID)
                    .setPageID(pageID).setTimestamp(timestamp).build();

            records.add(new ProducerRecord(
                    Constants.PAGE_VIEW_TOPIC,
                    userID == null ? null : Integer.toString(userID) ,
                    pageview));
        }

        produceRecords(records);
    }

    public static void produceRecords(List<ProducerRecord> records ){


        for (ProducerRecord record: records)
            producer.send(record, (RecordMetadata r, Exception e) -> {
                if (e != null) {
                    System.out.println("Error producing to topic " + r.topic());
                    e.printStackTrace();
                }
            });


        records.clear();
    }


    static private int generateRandomInt(int min, int max){
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    static private Timestamp generateRandomTimestamp(int lower){
        long end = new Timestamp(System.currentTimeMillis()).getTime();
        long offset = end-lower*1000;
        long diff = lower*1000 + 1;
        Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
        return rand;
    }

    private static void sleep(int milliseconds){
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}