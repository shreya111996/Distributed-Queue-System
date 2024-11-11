package com.distqueue.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.controller.Controller;
import com.distqueue.core.Message;
import com.distqueue.core.Partition;
import com.distqueue.core.Topic;


public class Broker implements Runnable{

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Controller controller;
    private final int brokerId;
    private final int port;
    private final List<Broker> otherBrokers; // Store a reference to other brokers

    public Broker(int port, List<Broker> otherBrokers, Controller controller) {
        this.brokerId = Integer.parseInt(System.getenv("BROKER_ID")); // Consider improving this
        this.port = Integer.parseInt(System.getenv("BROKER_PORT"));
        this.otherBrokers = otherBrokers;
        this.controller = controller; // Assuming controller handles the metadata properly
    }

    public void start() {
        // Code to start the broker and listen on the specified port
        System.out.println("Broker " + brokerId + " started on port " + port);
    }

    public void createTopic(String topicName, int partitionCount, int replicationFactor) {
        Topic topic = new Topic(topicName, partitionCount, replicationFactor);
        topics.put(topicName, topic);
        // You don't need to call the controller directly here if topic manages its own metadata
    }

    public void publishMessage(Message message) {
        Topic topic = topics.get(message.getTopic());

        if (topic == null) {
            System.err.println("Topic not found: " + message.getTopic());
            return;
        }
        Partition partition = topic.getPartition(message.getPartition()); // Use Partition directly
        partition.addMessage(message); // Handles message publishing and replication internally
    }

    public List<Message> getMessagesForPartition(String topicName, int partitionId, int offset) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            System.err.println("Topic not found: " + topicName);
            return new ArrayList<>();
        }
        Partition partition = topic.getPartition(partitionId);
        return partition.getMessages(offset);
    }

    @Override
    public void run() {
        start(); // Invoke the broker start logic
    }

    public int getId() {
        return this.brokerId; // Return the broker ID
    }
}
