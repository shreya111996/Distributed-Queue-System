package com.distqueue.consumer;

import java.util.List;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;


public class Consumer {

    private final List<Broker> brokers;
    private final String topic;
    private final String consumerGroup;

    public Consumer(List<Broker> brokers, String topic, String consumerGroup) {
        this.brokers = brokers;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public void consume() {
        int partitionId = 0; // Example: specify partition ID or add partitioning logic
        Broker broker = brokers.get(0); // Example: retrieve messages from the first broker; adjust as needed
        List<Message> messages = broker.getMessagesForPartition(topic, partitionId, 0);
        messages.forEach(message -> System.out.println("Consumed message: " + new String(message.getPayload())));
    }
}
