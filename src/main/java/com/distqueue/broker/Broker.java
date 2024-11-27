package com.distqueue.broker;

import com.distqueue.controller.Controller.BrokerInfo;
import com.distqueue.core.Message;
import com.distqueue.core.Partition;
import com.distqueue.core.Topic;
import com.distqueue.metadata.PartitionMetadata;

import com.distqueue.protocols.GossipProtocol;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Broker {

    public int getBrokerId() {
        return brokerId;
    }

    private final int brokerId;
    private final String host;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private final int port;
    private final String controllerHost;
    private final int controllerPort;
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> metadataCache = new ConcurrentHashMap<>();
    private boolean isRunning = true;
    private GossipProtocol gossipProtocol;
    private final List<Broker> allBrokers;

    public Broker(int brokerId, String host, int port, String controllerHost, int controllerPort,
            List<Broker> allBrokers) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        this.allBrokers = allBrokers; // Store the list of all brokers
    }

    public void start() throws IOException {
        // Initialize and start gossip protocol with the list of all brokers
        gossipProtocol = new GossipProtocol(allBrokers);
        Thread gossipThread = new Thread(() -> gossipProtocol.startGossip());
        gossipThread.start(); // Start gossip in a separate thread

        // Start HTTP server to accept requests
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/replicateMessage", new ReplicateMessageHandler());
        server.createContext("/updateLeadership", new UpdateLeadershipHandler());
        server.createContext("/publishMessage", new PublishMessageHandler());
        server.createContext("/consumeMessages", new ConsumeMessagesHandler());
        server.createContext("/health", new HealthCheckHandler());
        server.createContext("/sendGossip", new SendGossipHandler());
        server.createContext("/receiveGossip", new ReceiveGossipHandler());
        server.createContext("/brokers/active", new ActiveBrokersHandler());
        server.createContext("/brokers/leader", new LeaderBrokerHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        // Register with controller
        registerWithController();

        // Wait until all brokers are ready to start gossiping
        //waitForAllBrokersReady();

        // Now start gossip after confirming all brokers are up
        gossipThread.start();
        System.out.println("Broker started and gossiping will begin after all brokers are ready.");

        // Start sending heartbeats
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                this::sendHeartbeat, 0, 2000, TimeUnit.MILLISECONDS);

        // Add a shutdown hook to stop the gossip protocol and other resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down broker...");
            try {
                gossipProtocol.stopGossip(); // Stop the gossip protocol
                gossipThread.join(); // Wait for the gossip thread to terminate
                server.stop(0); // Stop the HTTP server
                System.out.println("Broker shut down gracefully.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Shutdown interrupted: " + e.getMessage());
            }
        }));

        System.out.println("Broker started and running on port " + port);
    }

    // private void waitForAllBrokersReady() {

    //     // Poll the controller or a shared resource until all brokers are registered and
    //     // ready
    //     boolean allBrokersReady = false;
    //     while (!allBrokersReady) {
    //         try {
    //             // Poll the controller for the number of registered brokers
    //             int registeredBrokers = getRegisteredBrokersFromController();
    //             if (registeredBrokers == allBrokers.size()) {
    //                 allBrokersReady = true; // All brokers are registered
    //             } else {
    //                 System.out
    //                         .println("Waiting for all brokers to be registered... Current count: " + registeredBrokers);
    //                 Thread.sleep(2000); // Wait before checking again
    //             }
    //         } catch (Exception e) {
    //             e.printStackTrace();
    //         }
    //     }

    // }

    public void addTopic(Topic topic) {
        topics.put(topic.getName(), topic);
        System.out.println("Broker " + brokerId + " added topic: " + topic.getName());
    }

    private void registerWithController() {
        try {
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/registerBroker");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            String params = "brokerId=" + brokerId + "&host=" + host + "&port=" + port;
            OutputStream os = conn.getOutputStream();
            os.write(params.getBytes());
            os.flush();
            os.close();

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Broker " + brokerId + " registered with controller.");
            } else {
                System.err.println("Failed to register broker " + brokerId + " with controller.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // // Handle received gossip message
    // public void receivePushGossip(Message gossipMessage) {
    // System.out.println("Broker " + brokerId + " received gossip from " +
    // gossipMessage.getSenderId());
    // processReceivedGossip(gossipMessage);
    // }

    // private void processReceivedGossip(Message gossipMessage) {
    // // Logic to synchronize metadata or other state based on received gossip
    // message
    // System.out.println("Processing received gossip: " +
    // gossipMessage.getGossipMetadata());
    // }

    // // Method to push gossip message to other brokers
    // public void pushGossipToPeers() {
    // Message gossipMessage = new Message("Broker-" + brokerId, "Metadata updates",
    // Instant.now());
    // gossipProtocol.gossipPush(this); // This pushes the message to a random
    // broker
    // }

    // // Method to pull gossip from other brokers
    // public void pullGossipFromPeers() {
    // Message gossipMessage = gossipProtocol.gossipPull(); // Corrected: No
    // argument passed
    // if (gossipMessage != null) {
    // processReceivedGossip(gossipMessage);
    // }
    // }

    private void sendHeartbeat() {
        if (isRunning) {
            try {
                URL url = new URL(
                        "http://" + controllerHost + ":" + controllerPort + "/heartbeat?brokerId=" + brokerId);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    System.out.println("Broker " + brokerId + " sent heartbeat.");
                } else {
                    System.err.println("Failed to send heartbeat from broker " + brokerId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Handlers for HTTP requests
    class ReplicateMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
                return;
            }
            try (ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
                    OutputStream os = exchange.getResponseBody()) {
                Message message = (Message) in.readObject();
                replicateMessage(message);
                String response = "Message replicated";
                exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
                os.write(response.getBytes(StandardCharsets.UTF_8));
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, -1); // Internal Server Error
            }
        }
    }

    class UpdateLeadershipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                // Method not allowed
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            // Use try-with-resources for safe stream handling
            try (ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
                    OutputStream os = exchange.getResponseBody()) {

                // Deserialize topic name and metadata
                String topicName = (String) in.readObject();
                PartitionMetadata partitionMetadata = (PartitionMetadata) in.readObject();

                // Perform leadership update
                updateLeadership(topicName, partitionMetadata);

                // Send response
                String response = "Leadership updated";
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length());
                os.write(response.getBytes());
            } catch (ClassNotFoundException e) {
                // Handle deserialization errors
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Internal server error: Invalid object format.");
            } catch (IOException e) {
                // Handle IO exceptions
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Internal server error: Unable to process the request.");
            } catch (Exception e) {
                // Handle unexpected exceptions
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "An unexpected error occurred.");
            }
        }

        private void sendErrorResponse(HttpExchange exchange, int statusCode, String errorMessage) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(statusCode, errorMessage.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(errorMessage.getBytes());
            }
        }
    }

    class PublishMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            try (ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
                    OutputStream os = exchange.getResponseBody()) {

                // Deserialize message
                Message message = (Message) in.readObject();

                // Publish the message
                publishMessage(message);

                // Respond with success
                String response = "Message published";
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length());
                os.write(response.getBytes());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Invalid message format.");
            } catch (IOException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Error processing the request.");
            } catch (Exception e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "An unexpected error occurred.");
            }
        }

        private void sendErrorResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(statusCode, message.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(message.getBytes());
            }
        }
    }

    class ConsumeMessagesHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            try {
                String query = exchange.getRequestURI().getQuery();
                if (query == null || query.isEmpty()) {
                    throw new IllegalArgumentException("Missing query parameters.");
                }

                // Parse query parameters
                Map<String, String> queryParams = parseQueryParameters(query);
                String topicName = queryParams.get("topicName");
                int partitionId = Integer.parseInt(queryParams.get("partitionId"));
                int offset = Integer.parseInt(queryParams.get("offset"));

                if (topicName == null) {
                    throw new IllegalArgumentException("Missing 'topicName' parameter.");
                }

                // Fetch messages
                List<Message> messages = getMessagesForPartition(topicName, partitionId, offset);

                // Serialize messages to JSON using Gson
                Gson gson = new Gson();
                String response = gson.toJson(messages);

                // Respond with serialized messages in JSON
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 400, "Invalid query parameters: " + e.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Error processing the request.");
            } catch (Exception e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "An unexpected error occurred.");
            }
        }

        // Example method to send error responses
        private void sendErrorResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            String errorResponse = "{\"error\": \"" + message + "\"}";
            exchange.sendResponseHeaders(statusCode, errorResponse.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(errorResponse.getBytes());
            }
        }

        private Map<String, String> parseQueryParameters(String query) {
            return Arrays.stream(query.split("&"))
                    .map(param -> param.split("="))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
        }
    }

    class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            String response = "OK";
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");

            try {
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                e.printStackTrace();
                String errorResponse = "Internal Server Error";
                exchange.sendResponseHeaders(500, errorResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes());
                }
            }
        }
    }

    // Handler for /sendGossip endpoint
    private class SendGossipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");

                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                        OutputStream outputStream = exchange.getResponseBody()) {

                    // Serialize local state (e.g., partition metadata, leadership info)
                    objectOutputStream.writeObject(metadataCache);
                    objectOutputStream.flush();

                    byte[] response = byteArrayOutputStream.toByteArray();
                    exchange.sendResponseHeaders(200, response.length);
                    outputStream.write(response);
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    // Handler for /receiveGossip endpoint
    private class ReceiveGossipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "text/plain");

                try (InputStream inputStream = exchange.getRequestBody();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {

                    @SuppressWarnings("unchecked")
                    Map<String, Map<Integer, PartitionMetadata>> receivedState = (Map<String, Map<Integer, PartitionMetadata>>) objectInputStream
                            .readObject();

                    gossipProtocol.reconcileState(receivedState, Broker.this);
                    exchange.sendResponseHeaders(200, 0); // OK
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    String errorMessage = "Failed to deserialize received state.";
                    exchange.sendResponseHeaders(500, errorMessage.length());
                    try (OutputStream outputStream = exchange.getResponseBody()) {
                        outputStream.write(errorMessage.getBytes());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    // Handler for /brokers/active endpoint
    private class ActiveBrokersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "application/json");

                try (OutputStream os = exchange.getResponseBody()) {
                    String response = new Gson().toJson(allBrokers);
                    exchange.sendResponseHeaders(200, response.getBytes().length);
                    os.write(response.getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    // Handler for /brokers/leader endpoint
    private class LeaderBrokerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "application/json");

                try {
                    Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());
                    String topic = params.get("topic");

                    if (topic == null || topic.isEmpty()) {
                        String errorMessage = "{\"error\": \"Missing 'topic' query parameter\"}";
                        exchange.sendResponseHeaders(400, errorMessage.getBytes().length); // Bad Request
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(errorMessage.getBytes());
                        }
                        return;
                    }

                    PartitionMetadata leaderMetadata = getLeaderForTopic(topic);

                    if (leaderMetadata != null) {
                        String response = new Gson().toJson(leaderMetadata);
                        exchange.sendResponseHeaders(200, response.getBytes().length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(response.getBytes());
                        }
                    } else {
                        String errorMessage = "{\"error\": \"Leader not found for the specified topic\"}";
                        exchange.sendResponseHeaders(404, errorMessage.getBytes().length); // Not Found
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(errorMessage.getBytes());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }

        private PartitionMetadata getLeaderForTopic(String topic) {
            Map<Integer, PartitionMetadata> partitions = metadataCache.get(topic);
            if (partitions != null) {
                for (PartitionMetadata metadata : partitions.values()) {
                    if (metadata.getLeaderId() == brokerId) {
                        return metadata;
                    }
                }
            }
            return null;
        }

        private Map<String, String> queryToMap(String query) {
            Map<String, String> result = new HashMap<>();
            if (query != null && !query.isEmpty()) {
                for (String param : query.split("&")) {
                    String[] entry = param.split("=");
                    if (entry.length > 1) {
                        result.put(entry[0], entry[1]);
                    } else {
                        result.put(entry[0], "");
                    }
                }
            }
            return result;
        }
    }

    public void publishMessage(Message message) {
        Topic topic = topics.get(message.getTopic());

        if (topic == null) {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(message.getTopic());
            topic = topics.get(message.getTopic());
            if (topic == null) {
                System.err.println("Topic not found after metadata fetch: " + message.getTopic());
                return;
            }
        }

        Partition partition = topic.getPartition(message.getPartition());

        // Check if this broker is the leader for the partition
        PartitionMetadata partitionMetadata = topic.getPartitionMetadata(message.getPartition());
        if (partitionMetadata.getLeaderId() != brokerId) {
            System.err.println("Broker " + brokerId + " is not the leader for partition " + message.getPartition());
            return;
        }

        partition.addMessage(message); // Handles message publishing and replication internally

        // Replicate message to followers
        replicateMessageToFollowers(message, partitionMetadata.getFollowers());
    }

    private void replicateMessageToFollowers(Message message, List<Integer> followerIds) {
        for (int followerId : followerIds) {
            BrokerInfo followerInfo = fetchBrokerInfo(followerId);
            if (followerInfo != null) {
                try {
                    URL url = new URL(
                            "http://" + followerInfo.getHost() + ":" + followerInfo.getPort()
                                    + "/replicateMessage");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("POST");
                    conn.setDoOutput(true);

                    // Serialize the message object
                    ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream());
                    out.writeObject(message);
                    out.flush();
                    out.close();

                    int responseCode = conn.getResponseCode();
                    if (responseCode == 200) {
                        System.out
                                .println("Replicated message " + message.getMessageId() + " to follower "
                                        + followerId);
                    } else {
                        System.err.println("Failed to replicate message to follower " + followerId);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.err.println("Follower broker " + followerId + " info not available.");
            }
        }
    }

    public void replicateMessage(Message message) {
        Topic topic = topics.get(message.getTopic());

        if (topic == null) {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(message.getTopic());
            topic = topics.get(message.getTopic());
            if (topic == null) {
                System.err.println("Topic not found after metadata fetch: " + message.getTopic());
                return;
            }
        }

        Partition partition = topic.getPartition(message.getPartition());

        // Append the message without checking for leadership
        partition.addMessage(message);

        System.out.println("Broker " + brokerId + " replicated message " + message.getMessageId());
    }

    public List<Message> getMessagesForPartition(String topicName, int partitionId, int offset) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(topicName);
            topic = topics.get(topicName);
            if (topic == null) {
                System.err.println("Topic not found after metadata fetch: " + topicName);
                return Collections.emptyList();
            }
        }

        Partition partition = topic.getPartition(partitionId);
        return partition.getMessages(offset);
    }

    public void updateLeadership(String topicName, PartitionMetadata partitionMetadata) {
        Topic topic = topics.get(topicName);
        if (topic != null) {
            topic.updatePartitionMetadata(partitionMetadata.getPartitionId(), partitionMetadata);

            Partition partition = topic.getPartition(partitionMetadata.getPartitionId());
            if (partition != null) {
                if (partitionMetadata.getLeaderId() == brokerId) {
                    partition.setLeader(true);
                    System.out
                            .println("Broker " + brokerId + " is now leader for topic " + topicName + " partition "
                                    + partitionMetadata.getPartitionId());
                } else {
                    partition.setLeader(false);
                }
            }
        } else {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(topicName);
            // Recursive call to update leadership now that topic exists
            updateLeadership(topicName, partitionMetadata);
        }
    }

    private void fetchMetadataForTopic(String topicName) {
        try {
            // Create the URL for fetching metadata
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            // Check the response code
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Read the response
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder responseBuilder = new StringBuilder();
                    String line;
                    while ((line = in.readLine()) != null) {
                        responseBuilder.append(line);
                    }

                    // Parse the JSON response into a Map
                    String response = responseBuilder.toString();
                    Gson gson = new Gson();
                    Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                    }.getType();
                    Map<Integer, PartitionMetadata> topicMetadata = gson.fromJson(response, type);

                    // Cache the metadata
                    metadataCache.put(topicName, topicMetadata);

                    // Create the topic and its partitions locally
                    Topic topic = new Topic(topicName, topicMetadata.size(), 1); // Replication factor not used here
                    for (PartitionMetadata pm : topicMetadata.values()) {
                        topic.updatePartitionMetadata(pm.getPartitionId(), pm);
                        Partition partition = topic.getPartition(pm.getPartitionId());
                        if (partition != null) {
                            partition.setLeader(pm.getLeaderId() == brokerId);
                        }
                    }
                    topics.put(topicName, topic);
                    System.out.println("Fetched metadata and created topic " + topicName);
                }
            } else {
                System.err.println("Failed to fetch metadata for topic " + topicName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Map<Integer, PartitionMetadata>> getMetadataCache() {
        return metadataCache;
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {
        // Since brokers register with the controller, we need to get their info from
        // the controller
        return null; // Implement as needed, perhaps cache broker info or fetch from controller
    }

    // Stop the broker
    public void stop() {
        isRunning = false;
    }
}
