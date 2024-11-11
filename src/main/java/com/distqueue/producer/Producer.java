package com.distqueue.producer;

import java.util.List;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;

public class Producer {

    private final List<Broker> brokers;

    public Producer(List<Broker> brokers) {
        this.brokers = brokers;
    }

    public void send(String topic, byte[] payload) {
        int partitionId = 0; // Example: use partition ID 0 or any logic to choose partition
        Broker broker = brokers.get(0); // Example: select the first broker; implement round-robin or other logic if needed
        Message message = new Message(topic, partitionId, payload);
        broker.publishMessage(message);
        System.out.println("Message sent to broker on topic: " + topic);
    }
}
