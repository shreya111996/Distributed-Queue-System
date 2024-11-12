package com.distqueue.broker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.controller.Controller;
import com.distqueue.core.Message;
import com.distqueue.core.Partition;
import com.distqueue.core.Topic;
import com.distqueue.core.TopicManager;


public class Broker implements Runnable{

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Controller controller;
    private final int brokerId;
    private final int port;
    private final List<Broker> otherBrokers; // Store a reference to other brokers
    private final Set<String> registeredTopics = new HashSet<>();  // Track registered topics


    public Broker(int port, List<Broker> otherBrokers, Controller controller) {
        this.brokerId = Integer.parseInt(System.getenv("BROKER_ID")); // Consider improving this
        this.port = Integer.parseInt(System.getenv("BROKER_PORT"));
        this.otherBrokers = otherBrokers;
        this.controller = controller; // Assuming controller handles the metadata properly
    }

    public void start() {
        System.out.println("Broker " + brokerId + " started on port " + port);

         // Query the TopicManager to ensure topics are registered only once
        TopicManager topicManager = controller.getTopicManager();

        // For each topic in TopicManager, register the topic if it's not already registered
        for (String topicName : topicManager.getTopicNames()) {  // Assuming getTopicNames() gives topic names
            if (!registeredTopics.contains(topicName)) {
                Topic topic = topicManager.getTopic(topicName);
                topicManager.createTopic(topicName, port, brokerId);  // Register topic via TopicManager
                registeredTopics.add(topicName);
                System.out.println("Broker " + this + " registered topic: " + topic.getName());
            }
        }
    }

    // New addTopic method
    public void addTopic(Topic topic) {
        topics.put(topic.getName(), topic);
        System.out.println("Broker " + brokerId + " registered topic: " + topic.getName());
    }

    public void publishMessage(Message message) {
        Topic topic = topics.get(message.getTopic());

        if (topic == null) {
            System.err.println("Topic not found: " + message.getTopic());
            return;
        }
        Partition partition = topic.getPartition(message.getPartition());
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
        return this.brokerId;
    }
}
