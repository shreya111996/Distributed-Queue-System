package com.distqueue.producer;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;

public class Producer {

    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    public void sendMessage(String topic, int partition, byte[] payload) {
        Message message = new Message(topic, partition, payload);
        broker.publishMessage(message);
        System.out.println("Published message to topic " + topic + ", partition " + partition);
    }
}
