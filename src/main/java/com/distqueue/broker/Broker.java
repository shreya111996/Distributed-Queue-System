package com.distqueue.broker;

import com.distqueue.controller.Controller.BrokerInfo;
import com.distqueue.core.Message;
import com.distqueue.core.Partition;
import com.distqueue.core.Topic;
import com.distqueue.metadata.PartitionMetadata;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Broker {

    private final int brokerId;
    private final String host;
    private final int port;
    private final String controllerHost;
    private final int controllerPort;
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> metadataCache = new ConcurrentHashMap<>();
    private boolean isRunning = true;

    public Broker(int brokerId, String host, int port, String controllerHost, int controllerPort) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
    }

    // **Add the getId() method**
    public int getId() {
        return brokerId;
    }


    public void start() throws IOException {
        // Start HTTP server to accept requests
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/replicateMessage", new ReplicateMessageHandler());
        server.createContext("/updateLeadership", new UpdateLeadershipHandler());
        server.createContext("/publishMessage", new PublishMessageHandler());
        server.createContext("/consumeMessages", new ConsumeMessagesHandler());
        server.createContext("/health", new HealthCheckHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        // Register with controller
        registerWithController();

        // Start sending heartbeats
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            this::sendHeartbeat, 0, 2000, TimeUnit.MILLISECONDS);
    }

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

    private void sendHeartbeat() {
        if (isRunning) {
            try {
                URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/heartbeat?brokerId=" + brokerId);
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
            ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
            try {
                Message message = (Message) in.readObject();
                replicateMessage(message);
                String response = "Message replicated";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, 0);
                exchange.getResponseBody().close();
            }
        }
    }

    class UpdateLeadershipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
            try {
                String topicName = (String) in.readObject();
                PartitionMetadata partitionMetadata = (PartitionMetadata) in.readObject();
                updateLeadership(topicName, partitionMetadata);
                String response = "Leadership updated";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, 0);
                exchange.getResponseBody().close();
            }
        }
    }

    class PublishMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
            try {
                Message message = (Message) in.readObject();
                publishMessage(message);
                String response = "Message published";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, 0);
                exchange.getResponseBody().close();
            }
        }
    }

    class ConsumeMessagesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String[] queryParams = exchange.getRequestURI().getQuery().split("&");
            String topicName = queryParams[0].split("=")[1];
            int partitionId = Integer.parseInt(queryParams[1].split("=")[1]);
            int offset = Integer.parseInt(queryParams[2].split("=")[1]);

            List<Message> messages = getMessagesForPartition(topicName, partitionId, offset);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(messages);
            oos.flush();
            String response = Base64.getEncoder().encodeToString(baos.toByteArray());

            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = "OK";
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
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
                    URL url = new URL("http://" + followerInfo.getHost() + ":" + followerInfo.getPort() + "/replicateMessage");
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
                        System.out.println("Replicated message " + message.getMessageId() + " to follower " + followerId);
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
                    System.out.println("Broker " + brokerId + " is now leader for topic " + topicName + " partition " + partitionMetadata.getPartitionId());
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
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();

                byte[] data = Base64.getDecoder().decode(response);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
                Map<Integer, PartitionMetadata> topicMetadata = (Map<Integer, PartitionMetadata>) ois.readObject();

                metadataCache.put(topicName, topicMetadata);

                // Create topic and partitions locally
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
            } else {
                System.err.println("Failed to fetch metadata for topic " + topicName);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {
        // Since brokers register with the controller, we need to get their info from the controller
        return null; // Implement as needed, perhaps cache broker info or fetch from controller
    }

    // Stop the broker
    public void stop() {
        isRunning = false;
    }
}
