package com.learning.kafka.wiki.Handler;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WikimediaChangeHandler implements EventHandler {

    private final Logger LOG = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());
    KafkaProducer<String, String> kafkaProducer;
    String topic;
    JSONArray jsonMessageList;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    public WikimediaChangeHandler(JSONArray jsonMessageList) {
        this.jsonMessageList = jsonMessageList;
    }

    @Override
    public void onOpen(){
        LOG.info("Event started ");
    }

    @Override
    public void onClosed(){
        LOG.warn("Closing producer");
        if(kafkaProducer != null)
            kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
//        LOG.info("Message received {}", messageEvent.getData());
        // all of this is done asynchronously
        if(kafkaProducer != null) {
            LOG.info("Publishing message to kafka topis {}", topic);
            // TODO: 9/17/2022 : Producer logic -- Started
            pushToProducer(messageEvent);
        } else {
            simpleReader(messageEvent.getData());
        }
    }

    private void pushToProducer(MessageEvent messageEvent) {
        // When we receive a message we need to publish it to the producer
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    private void simpleReader(String message) {
        JSONObject jsonMessageToSend = new JSONObject();
        try {
            JSONObject jsonMessage = new JSONObject(message);
            LOG.info("JSON: " + jsonMessage);
            jsonMessageToSend.put("id",jsonMessage.getInt("id"));
            jsonMessageToSend.put("server_name",jsonMessage.getString("server_name"));
            jsonMessageToSend.put("wiki",jsonMessage.getString("wiki"));
            jsonMessageToSend.put("user",jsonMessage.getString("user"));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        if (jsonMessageToSend != null) {
            jsonMessageList.put(jsonMessageToSend);
        }
    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        LOG.error("Error in reading stream");
    }
}
