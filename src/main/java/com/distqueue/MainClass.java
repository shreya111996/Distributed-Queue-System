package com.distqueue;

import com.distqueue.broker.Broker;
import com.distqueue.controller.Controller;
import com.distqueue.producer.Producer;
import com.distqueue.consumer.Consumer;

import java.util.ArrayList;
import java.util.List;

public class MainClass {

    public static void main(String[] args) throws Exception {
        String role = System.getenv("ROLE"); // "controller", "broker", "producer", "consumer"

        // Create a list of brokers to pass to each broker instance
        List<Broker> allBrokers = new ArrayList<>();
        String[] topicNames = { "TopicA", "TopicB", "TopicC" };

        switch (role.toLowerCase()) {
            case "controller":
                int controllerPort = Integer.parseInt(System.getenv("PORT"));
                Controller controller = new Controller(controllerPort);
                controller.start();
                System.out.println("Controller started on port " + controllerPort);
                break;

            case "broker":
                int brokerId = Integer.parseInt(System.getenv("BROKER_ID"));
                String host = System.getenv("HOST");
                int port = Integer.parseInt(System.getenv("PORT"));
                String controllerHost = System.getenv("CONTROLLER_HOST");
                int controllerPortBroker = Integer.parseInt(System.getenv("CONTROLLER_PORT"));

                // Create the broker and add it to the list
                Broker broker = new Broker(brokerId, host, port, controllerHost, controllerPortBroker, allBrokers);
                allBrokers.add(broker); // Add the newly created broker to the list

                // Start the broker
                broker.start();
                //System.out.println("Broker " + brokerId + " started on port " + port);
                break;

            case "producer":
                String controllerHostProducer = System.getenv("CONTROLLER_HOST");
                int controllerPortProducer = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
                Producer producer = new Producer(controllerHostProducer, controllerPortProducer);

                // Create three topics with 3 partitions and 2 replication factors each

                for (String topicName : topicNames) {
                    producer.createTopic(topicName, 3, 2); // 3 partitions, 2 replication factor
                    System.out.println("Created topic: " + topicName);
                    Thread.sleep(1000); // Wait for topic creation
                }

                // Send messages to each topic
                for (String topicName : topicNames) {
                    for (int i = 1; i <= 10; i++) {
                        String messageContent = "Message " + i + " for " + topicName;
                        producer.send(topicName, messageContent.getBytes());
                        System.out.println("Sent to " + topicName + ": " + messageContent);
                        Thread.sleep(500); // Simulate delay
                    }
                }

                Thread.sleep(1000); // Wait before exiting producer
                break;

            case "consumer":

                String controllerHostConsumer = System.getenv("CONTROLLER_HOST");
                int controllerPortConsumer = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
                Consumer consumer = new Consumer(controllerHostConsumer, controllerPortConsumer);

                // We assume the consumer consumes from all partitions of a given topic in order
                // For simplicity, we'll consume from all partitions sequentially.
                // Loop through each topic and its partitions
                
                for (String topicName : topicNames) {
                    // For each topic, consume messages from each partition sequentially
                    for (int partitionId = 0; partitionId < 3; partitionId++) { // Assuming 3 partitions per topic
                        consumer.consume(topicName, partitionId); // Consume from each partition
                    }
                }
                break;

            default:
                System.err.println("Invalid ROLE specified. Use 'controller', 'broker', 'producer', or 'consumer'.");
                break;
        }
    }
}
