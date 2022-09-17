package com.learning.kafka.config;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import java.util.Properties;

public class KafkaProducerConfig {
//    private static final Logger log = (Logger) LoggerFactory.getLogger(KafkaConfiguration.class.getSimpleName());

    @Value("kafkaServer")
    String kafkaServer = "127.0.0.1:9092";

    public KafkaProducer<String, String> initProducer() {
        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Created producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    public RecordMetadata callProducer() {
//        log.info("Start executing...");
        System.out.println("Start executing...");

        KafkaProducer<String, String> producer = initProducer();

        // Create a producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-java","hello world");

        final RecordMetadata[] recordMetadata = {null};

        // Send data ~ asynchronous
        producer.send(producerRecord, (Callback) (metadata, e) -> {
            recordMetadata[0] = metadata;
            if(e == null) {
                System.out.println("Metadata received is- \n " +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp() );
            }
            else {
                System.out.println("Error while producing with exception: " + e);
            }
        });

        // flush data ~ asynchronous
        producer.flush();

        // Flush and close the Producer
        producer.close();

        System.out.println("Process completed");
        return recordMetadata[0];
    }

    public boolean callProducerWithKey(Integer noOfTimes) {

        KafkaProducer<String, String> producer = initProducer();

        for(int i =0; i < noOfTimes; i++) {
            String topic = "demo-java";
            String key = "key_" + i;
            String value = "Message_" + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            producer.send(producerRecord, (Callback) (metadata, e)-> {
                if(e == null) {
                System.out.println("Metadata received is- \n " +
                        "Key: " + producerRecord.key() + "\n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset());
                }
                else {
                    System.out.println("Error while producing with exception: " + e);
                }
            });
        }
        producer.close();
        return true;
    }

}