package main2;

import clojure.lang.Cons;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class csvConsumer {

    private static String kafkaBrokerEndpoint = null;
    private static String csvDataIngestTopic = null;

    public static void main(String[] args) {
        if (args!= null){
            kafkaBrokerEndpoint = args[0];
            csvDataIngestTopic = args[1];
        }
        System.out.println("kafkaBrokerEndpoint = " + kafkaBrokerEndpoint);
        System.out.println("csvDataIngestTopic = " + csvDataIngestTopic);
        csvConsumer kafkaConsumer = new csvConsumer();
        kafkaConsumer.consumeCsvData();
    }

    private void consumeCsvData() {
        final Consumer<String, String> csvDataConsumer = createKafkaConsumer();


        Map<String, ConsumerRecords<String, String>> csvRecords = csvDataConsumer.poll(30000);
        System.out.println("csvRecords = " + csvRecords);
//        try {
//            csvRecords.forEach((key, value) -> System.out.println("Received Record -->" + key + " | " + value));
////            csvRecords.forEach(csvRecord -> {
////                System.out.println("Received Record --> " + csvRecord.key() + " | " + csvRecord.offset() + " | "
////                        + csvRecord.partition() + " | " + csvRecord.value());
////            });
//            csvDataConsumer.commit(true);
//        } catch (NullPointerException e) {
//            e.printStackTrace();
//        }
//        while (true) {
//        }
    }

    private KafkaConsumer<String, String> createKafkaConsumer(){
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaConsumer");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS, 10000);
        final KafkaConsumer kafkaConsumer = new KafkaConsumer<String, String>(prop);
        kafkaConsumer.subscribe(csvDataIngestTopic);

        return kafkaConsumer;
    }
}

