package main2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class csvProducer {
    private static  String kafkaBrokerEndpoint = null;
    private static String csvInputFile = null;
    private static String  csvDataIngestTopic = null;

    public static void main(String[] args) {
        if(args!= null){
            kafkaBrokerEndpoint = args[0];
            csvDataIngestTopic = args[1];
            csvInputFile = args[2];
        }

        csvProducer kafkaProducer = new csvProducer();
        kafkaProducer.publishCsvData();
    }

    public void publishCsvData(){
        final Producer<String, String> csvDataProducer = createKafkaProducer();
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        try{
            Stream<String> csvDataFileStream = Files.lines(Paths.get(csvInputFile));
            long start = System.currentTimeMillis();
            ObjectMapper mapper = new ObjectMapper();
            csvDataFileStream.forEach(line -> {
                final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(csvDataIngestTopic,UUID.randomUUID().toString(), line);

                //Adding delay to allow the records to stream for few seconds
                try{
                    Thread.currentThread().sleep(0, 1);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                csvDataProducer.send(csvRecord, (((metadata, exception) -> {
                    if(metadata != null){
                        System.out.println("Loan Data Event Sent --->"+csvRecord.key()+" | "+csvRecord.value() + " | "+metadata.partition());
                    } else {
                        System.out.println("Error sending csv read data event -->"+ csvRecord.value());
                    }
                })));
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private KafkaProducer<String, String> createKafkaProducer(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "csvProducer");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(prop);
    }
}
