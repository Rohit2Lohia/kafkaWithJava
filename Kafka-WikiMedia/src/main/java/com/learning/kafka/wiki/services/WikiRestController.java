package com.learning.kafka.wiki.services;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.learning.kafka.wiki.Handler.WikimediaChangeHandler;
import com.learning.kafka.wiki.Handler.WikimediaChangeProducer;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/wiki")
public class WikiRestController {

    private static Logger LOG = LoggerFactory.getLogger(WikiRestController.class.getSimpleName());

    @Autowired
    WikimediaChangeProducer wikimediaChangeProducer;

    @GetMapping("/")
    public String home() {
        LOG.info("home() called");
        return "Welcome to Wiki Consumer";
    }

    @GetMapping(value = "/getmessages",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getMessages() throws InterruptedException {
        JSONArray messagesList = new JSONArray();
        EventHandler eventHandler = new WikimediaChangeHandler(messagesList);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // Start the producer in another thread
        eventSource.start();
        // we produce for 10 minutes and block the program until then
        TimeUnit.SECONDS.sleep(5);
        // End event
        eventSource.close();
        LOG.info("Complete...");
        return new ResponseEntity(messagesList.toString(), HttpStatus.OK);
    }
}
