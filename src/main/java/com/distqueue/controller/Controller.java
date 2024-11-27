package com.distqueue.controller;

import com.distqueue.metadata.PartitionMetadata;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class Controller {

    private final int controllerPort;
    private final Map<Integer, BrokerInfo> brokerRegistry = new ConcurrentHashMap<>();
    private final Map<Integer, Long> brokerHeartbeats = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> metadata = new ConcurrentHashMap<>();
    private final long heartbeatInterval = 2000; // Expected heartbeat interval in milliseconds
    private final long heartbeatTimeout = 5000; // Timeout to consider a broker as failed

    public Controller(int port) {
        this.controllerPort = port;
        // Start the heartbeat monitoring task
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                this::checkBrokerHeartbeats, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(controllerPort), 0);
        server.createContext("/heartbeat", new HeartbeatHandler());
        server.createContext("/registerBroker", new RegisterBrokerHandler());
        server.createContext("/createTopic", new CreateTopicHandler());
        server.createContext("/getMetadata", new GetMetadataHandler());
        server.createContext("/getBrokerInfo", new GetBrokerInfoHandler());
        server.createContext("/getAllBrokers", new GetAllBrokersHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    // Handlers for HTTP requests

    class HeartbeatHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String brokerIdStr = exchange.getRequestURI().getQuery().split("=")[1];
            int brokerId = Integer.parseInt(brokerIdStr);
            receiveHeartbeat(brokerId);

            String response = "Heartbeat received";
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    class RegisterBrokerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            BufferedReader in = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
            String line;
            StringBuilder body = new StringBuilder();
            while ((line = in.readLine()) != null) {
                body.append(line);
            }
            in.close();

            String[] params = body.toString().split("&");
            int brokerId = Integer.parseInt(params[0].split("=")[1]);
            String host = params[1].split("=")[1];
            int port = Integer.parseInt(params[2].split("=")[1]);

            BrokerInfo brokerInfo = new BrokerInfo(host, port);
            registerBroker(brokerId, brokerInfo);

            String response = "Broker registered";
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

            System.out.println("Registered broker " + brokerId + " at " + host + ":" + port);
        }
    }

    class CreateTopicHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            BufferedReader in = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
            String line;
            StringBuilder body = new StringBuilder();
            while ((line = in.readLine()) != null) {
                body.append(line);
            }
            in.close();

            String[] params = body.toString().split("&");
            String topicName = params[0].split("=")[1];
            int numPartitions = Integer.parseInt(params[1].split("=")[1]);
            int replicationFactor = Integer.parseInt(params[2].split("=")[1]);

            createTopic(topicName, numPartitions, replicationFactor);

            String response = "Topic created";
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

            System.out.println("Topic " + topicName + " created with " + numPartitions + " partitions");
        }
    }

    class GetMetadataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String topicName = exchange.getRequestURI().getQuery().split("=")[1];
            Map<Integer, PartitionMetadata> topicMetadata = metadata.get(topicName);
            String response;
            if (topicMetadata != null) {
                // Serialize the topic metadata to byte array
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(topicMetadata);
                oos.flush();

                // Base64 encode the serialized metadata before sending it
                response = Base64.getEncoder().encodeToString(baos.toByteArray());
            } else {
                response = "No metadata found for topic " + topicName;
            }
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    class GetBrokerInfoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String brokerIdStr = exchange.getRequestURI().getQuery().split("=")[1];
            int brokerId = Integer.parseInt(brokerIdStr);
            BrokerInfo brokerInfo = brokerRegistry.get(brokerId);

            String response;
            if (brokerInfo != null) {
                response = brokerInfo.getHost() + ":" + brokerInfo.getPort();
            } else {
                response = "Broker not found";
            }
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    class GetAllBrokersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            List<BrokerInfo> brokers = new ArrayList<>(brokerRegistry.values());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(brokers);
            oos.flush();
    
            byte[] response = baos.toByteArray();
            exchange.sendResponseHeaders(200, response.length);
            OutputStream os = exchange.getResponseBody();
            os.write(response);
            os.close();
        }
    }    

    // BrokerInfo class to store broker's network information
    public static class BrokerInfo {
        private final String host;
        private final int port;

        public BrokerInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }

    // Existing methods...

    public void registerBroker(int brokerId, BrokerInfo brokerInfo) {
        brokerRegistry.put(brokerId, brokerInfo);
        System.out.println("Broker " + brokerId + " registered with host " + brokerInfo.getHost() + " and port " + brokerInfo.getPort());
    
        for (String topicName : metadata.keySet()) {
            Map<Integer, PartitionMetadata> partitionMetadataMap = metadata.get(topicName);
            for (PartitionMetadata partitionMetadata : partitionMetadataMap.values()) {
                boolean leadershipChanged = false;
                if (partitionMetadata.getLeaderId() == -1) {
                    partitionMetadata.setLeaderId(brokerId);
                    leadershipChanged = true;
                    System.out.println("Assigned broker " + brokerId + " as leader for topic " + topicName + " partition " + partitionMetadata.getPartitionId());
                } else if (!partitionMetadata.getFollowers().contains(brokerId)) {
                    partitionMetadata.addFollower(brokerId);
                    System.out.println("Added broker " + brokerId + " as follower for topic " + topicName + " partition " + partitionMetadata.getPartitionId());
                }
    
                if (leadershipChanged) {
                    BrokerInfo broker = brokerRegistry.get(brokerId);
                    notifyBrokerOfLeadershipChange(broker, topicName, partitionMetadata);
                }
            }
        }
    }
    

    public void receiveHeartbeat(int brokerId) {
        brokerHeartbeats.put(brokerId, System.currentTimeMillis());
    }

    private void checkBrokerHeartbeats() {
        long currentTime = System.currentTimeMillis();
        for (Integer brokerId : new HashSet<>(brokerRegistry.keySet())) {
            long lastHeartbeat = brokerHeartbeats.getOrDefault(brokerId, 0L);
            if (currentTime - lastHeartbeat > heartbeatTimeout) {
                System.out.println("Broker " + brokerId + " is considered dead.");
                handleBrokerFailure(brokerId);
            }
        }
    }

    private void handleBrokerFailure(int failedBrokerId) {
        // Remove the failed broker from the registry
        brokerRegistry.remove(failedBrokerId);

        // Reassign leadership for partitions led by the failed broker
        for (String topicName : metadata.keySet()) {
            Map<Integer, PartitionMetadata> partitionMetadataMap = metadata.get(topicName);
            for (PartitionMetadata partitionMetadata : partitionMetadataMap.values()) {
                if (partitionMetadata.getLeaderId() == failedBrokerId) {
                    electNewLeader(topicName, partitionMetadata, failedBrokerId);
                }
            }
        }
    }

    private void electNewLeader(String topicName, PartitionMetadata partitionMetadata, int failedBrokerId) {
        List<Integer> followers = partitionMetadata.getFollowers();
        // Remove the failed broker from the followers list if present
        followers.remove(Integer.valueOf(failedBrokerId));

        if (!followers.isEmpty()) {
            // Choose the first follower as the new leader
            int newLeaderId = followers.get(0);
            partitionMetadata.setLeaderId(newLeaderId);

            // Update followers list
            List<Integer> newFollowers = new ArrayList<>(followers);
            newFollowers.remove(Integer.valueOf(newLeaderId));
            partitionMetadata.setFollowers(newFollowers);

            System.out.println("New leader for topic " + topicName + " partition " + partitionMetadata.getPartitionId()
                    + " is broker " + newLeaderId);

            // Notify the new leader broker
            BrokerInfo newLeaderBroker = brokerRegistry.get(newLeaderId);
            if (newLeaderBroker != null) {
                notifyBrokerOfLeadershipChange(newLeaderBroker, topicName, partitionMetadata);
            }
        } else {
            System.out.println("No available brokers to become leader for topic " + topicName + " partition "
                    + partitionMetadata.getPartitionId());
        }
    }

    private void notifyBrokerOfLeadershipChange(BrokerInfo brokerInfo, String topicName,
            PartitionMetadata partitionMetadata) {
        try {
            URL url = new URL("http://" + brokerInfo.getHost() + ":" + brokerInfo.getPort() + "/updateLeadership");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            // Send topicName and partitionMetadata
            ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream());
            out.writeObject(topicName);
            out.writeObject(partitionMetadata);
            out.flush();
            out.close();

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Notified broker " + partitionMetadata.getLeaderId() + " of leadership change.");
            } else {
                System.err.println(
                        "Failed to notify broker " + partitionMetadata.getLeaderId() + " of leadership change.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Topic creation method
    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        if (metadata.containsKey(topicName)) {
            System.err.println("Topic already exists: " + topicName);
            return;
        }

        if (brokerRegistry.isEmpty()) {
            System.err.println("No brokers registered. Cannot create topic.");
            return;
        }

        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            PartitionMetadata partitionMetadata = new PartitionMetadata(partitionId, replicationFactor);
            List<Integer> brokerIds = new ArrayList<>(brokerRegistry.keySet());
            int leaderIndex = partitionId % brokerIds.size();
            int leaderId = brokerIds.get(leaderIndex);
            partitionMetadata.setLeaderId(leaderId);

            for (int i = 1; i < replicationFactor && i < brokerIds.size(); i++) {
                int followerId = brokerIds.get((leaderIndex + i) % brokerIds.size());
                partitionMetadata.addFollower(followerId);
            }

            partitionMetadataMap.put(partitionId, partitionMetadata);
        }

        metadata.put(topicName, partitionMetadataMap);
        System.out.println("Topic " + topicName + " created with metadata: " + partitionMetadataMap);

        // Notify brokers of their leadership
        notifyBrokersOfNewTopic(topicName, partitionMetadataMap);
    }

    private void notifyBrokersOfNewTopic(String topicName, Map<Integer, PartitionMetadata> partitionMetadataMap) {
        for (PartitionMetadata partitionMetadata : partitionMetadataMap.values()) {
            int leaderId = partitionMetadata.getLeaderId();
            if (leaderId != -1) {
                BrokerInfo leaderBroker = brokerRegistry.get(leaderId);
                if (leaderBroker != null) {
                    notifyBrokerOfLeadershipChange(leaderBroker, topicName, partitionMetadata);
                }
            }
            for (int followerId : partitionMetadata.getFollowers()) {
                BrokerInfo followerBroker = brokerRegistry.get(followerId);
                if (followerBroker != null) {
                    notifyBrokerOfLeadershipChange(followerBroker, topicName, partitionMetadata);
                }
            }
        }
    }
    
}
