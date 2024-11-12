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

        // Initialize TopicManager with brokers
        TopicManager topicManager = new TopicManager(brokers);

        // Initialize the Controller with the list of brokers for managing metadata
        Controller controller = new Controller(brokers, topicManager);

        // Create brokers and register them with the controller
        for (int i = 0; i < numberOfNodes; i++) {
            int port = 8080 + i; // Assign unique ports
            Broker broker = new Broker(port, brokers, controller); // Pass the controller to the broker
            brokers.add(broker);
            controller.registerBroker(i, broker); // Register each broker with its ID in the controller
            new Thread(broker::start).start(); // Start each broker on a new thread
        }

        // Small delay to ensure brokers are fully initialized
        Thread.sleep(1000);

        // Create a topic using the TopicManager
        topicManager.createTopic("TestTopic", 3, 2);

        // Use the Controller to manage topic metadata and assign leaders/followers
        controller.loadTopicMetadata("TestTopic");

        // Simulate producer sending messages to the topic
        Producer producer = new Producer(brokers);
        producer.send("TestTopic", "Message payload".getBytes());

        // Simulate consumer consuming messages from the topic
        Consumer consumer = new Consumer(brokers, "TestTopic", "ConsumerGroup1");
        consumer.consume();

    }

}
