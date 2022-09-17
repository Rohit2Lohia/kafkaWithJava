package com.learning.kafka;

import com.learning.kafka.config.KafkaProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@RequestMapping("/producer")
public class ProducerRunner {

    @GetMapping("/")
    public String home() {
        return "Welcome to Kafka Producer";
    }

    @GetMapping("/sendMessage")
    public String sendMessage() {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        RecordMetadata metadata = kafkaProducerConfig.callProducer();
        if(metadata != null) {
            return ("Metadata received is- \n " +
                    "Topic: " + metadata.topic() + "\n" +
                    "Partition: " + metadata.partition() + "\n" +
                    "Offset: " + metadata.offset() + "\n" +
                    "Timestamp: " + metadata.timestamp());
        } else {
            return "Error inserting data into topic";
        }
    }

    @GetMapping("/sendMultiMessage")
    public String sendMessageWithKey(@RequestParam(value = "times", defaultValue = "1") Integer noOfTimes) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        if(kafkaProducerConfig.callProducerWithKey(noOfTimes)) {
            return "Message sent " + noOfTimes + " of times.";
        } else {
            return "Error inserting data into topic";
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerRunner.class,args);
    }
}
