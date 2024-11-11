package com.distqueue;

import java.util.ArrayList;
import java.util.List;

import com.distqueue.broker.Broker;
import com.distqueue.consumer.Consumer;
import com.distqueue.controller.Controller;
import com.distqueue.core.TopicManager;
import com.distqueue.producer.Producer;

public class MainClass {
    
    public static void main(String[] args) throws InterruptedException {


        int numberOfNodes = 3; // for example
        List<Broker> brokers = new ArrayList<>();

        // Initialize the Controller with the list of brokers for managing metadata
        Controller controller = new Controller(brokers);

        // Create brokers and register them with the controller
        for (int i = 0; i < numberOfNodes; i++) {
            int port = 8080 + i; // Assign unique ports
            Broker broker = new Broker(port, brokers, controller); // Pass the controller to the broker
            brokers.add(broker);
            controller.registerBroker(i, broker); // Register each broker with its ID in the controller
            new Thread(broker::start).start(); // Start each broker on a new thread
        }

        // Initialize TopicManager with the brokers and create a topic
        TopicManager topicManager = new TopicManager(brokers);
        topicManager.createTopic("TestTopic", 3, 2); // 3 partitions, replication factor 2

        // Use the Controller to manage topic metadata and assign leaders/followers
        controller.addTopicMetadata("TestTopic", 3, 2);

        // Simulate producer sending messages to the topic
        Producer producer = new Producer(brokers);
        producer.send("TestTopic", "Message payload".getBytes());

        // Simulate consumer consuming messages from the topic
        Consumer consumer = new Consumer(brokers, "TestTopic", "ConsumerGroup1");
        consumer.consume();
    
    }

}
