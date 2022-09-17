package com.learning.kafka.wiki;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class WikiConsumerRunner {

    private static Logger LOG = LoggerFactory.getLogger(WikiConsumerRunner.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("Starting Wiki runner producer");
        SpringApplication.run(WikiConsumerRunner.class, args);
    }
}
