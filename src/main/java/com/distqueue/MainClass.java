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
                allBrokers.add(broker);  // Add the newly created broker to the list

                // Start the broker
                broker.start();
                System.out.println("Broker " + brokerId + " started on port " + port);
                break;

            case "producer":
                String controllerHostProducer = System.getenv("CONTROLLER_HOST");
                int controllerPortProducer = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
                Producer producer = new Producer(controllerHostProducer, controllerPortProducer);

                // Create topic (this can be moved to the controller or another service)
                producer.createTopic("TestTopic", 3, 2);

                // Send messages
                producer.send("TestTopic", "Hello, World!".getBytes());
                break;

            case "consumer":
                String controllerHostConsumer = System.getenv("CONTROLLER_HOST");
                int controllerPortConsumer = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
                Consumer consumer = new Consumer(controllerHostConsumer, controllerPortConsumer);

                // Consume messages
                consumer.consume("TestTopic");
                break;

            default:
                System.err.println("Invalid ROLE specified. Use 'controller', 'broker', 'producer', or 'consumer'.");
                break;
        }
    }
}
