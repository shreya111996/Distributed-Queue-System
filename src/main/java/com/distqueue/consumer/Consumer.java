package com.distqueue.consumer;

import com.distqueue.adapters.MessageAdapter;
// import com.distqueue.adapters.MessageAdapter;
import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;
import com.distqueue.producer.Producer.BrokerInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Comparator;

public class Consumer {

    private final String controllerHost;
    private final int controllerPort;

    public Consumer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
    }

    public void consume(String topic, int partitionId) {
        // Wait for the controller and metadata to be ready
        if (!waitForReadiness()) {
            System.err.println("Controller not ready. Exiting consume operation.");
            return;
        }

        Map<Integer, PartitionMetadata> topicMetadata = fetchMetadataWithRetries(topic);
        if (topicMetadata == null) {
            System.err.println("Topic metadata not found for topic " + topic);
            return;
        }

        // Fetch partition metadata for the given partitionId
        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);
        if (partitionMetadata == null) {
            System.err.println("Partition metadata not found for partition " + partitionId);
            return; // Exit if the partition metadata is not available
        }

        int leaderId = partitionMetadata.getLeaderId();
        if (leaderId == -1) {
            System.err.println("Leader ID not available for partition " + partitionId);
            return; // Exit if the leader ID is not available
        }

        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);
        if (leaderInfo == null) {
            System.err.println("Leader broker info not found for broker ID " + leaderId);
            return; // Exit if the leader broker information is not available
        }

        long lastOffset = 0; // Start from the beginning of the partition
        try {
            while (true) {
                // Construct the URL for long-polling from the broker
                URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort()
                        + "/longPolling?topicName=" + topic + "&partitionId=" + partitionId
                        + "&offset=" + lastOffset);

                // System.out.println("Fetching messages from: " + url);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                conn.setConnectTimeout(10000); // 10 seconds timeout for connection
                conn.setReadTimeout(30000); // 30 seconds timeout for reading

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    String response = in.readLine();
                    in.close();

                    // Deserialize and process the messages
                    Gson gson = new GsonBuilder()
                            .registerTypeAdapter(Message.class, new MessageAdapter())
                            .create();
                    List<Message> messages = gson.fromJson(response, new TypeToken<List<Message>>() {
                    }.getType());

                    if (messages == null || messages.isEmpty()) {
                        System.out.println("No new messages available in Partition " + partitionId);
                        Thread.sleep(1000); // Short delay before retrying
                        continue;
                    }

                    // Ensure messages are sorted by timestamp (latest first)
                    messages.sort(Comparator.comparing(Message::getTimestamp));

                    for (Message message : messages) {
                        System.out.println("Consumed message from Partition " + partitionId + ": "
                                + new String(message.getPayload()));
                        lastOffset = message.getOffset() + 1; // Update the offset after processing
                    }
                } else {
                    System.err.println("Failed to fetch messages from Partition " + partitionId + ". Response Code: "
                            + responseCode);
                    Thread.sleep(2000); // Retry after a delay
                }
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Error while consuming messages from Partition " + partitionId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /*public void consume(String topic, int partitionId) {
        // Wait for the controller and metadata to be ready
        if (!waitForReadiness()) {
            System.err.println("Controller not ready. Exiting consume operation.");
            return;
        }

        Map<Integer, PartitionMetadata> topicMetadata = fetchMetadataWithRetries(topic);
        if (topicMetadata == null) {
            System.err.println("Topic metadata not found for topic " + topic);
            return;
        }

        // Fetch partition metadata for the given partitionId
        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);
        if (partitionMetadata == null) {
            System.err.println("Partition metadata not found for partition " + partitionId);
            return; // Exit if the partition metadata is not available
        }

        int leaderId = partitionMetadata.getLeaderId();
        if (leaderId == -1) {
            System.err.println("Leader ID not available for partition " + partitionId);
            return; // Exit if the leader ID is not available
        }

        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);
        if (leaderInfo == null) {
            System.err.println("Leader broker info not found for broker ID " + leaderId);
            return; // Exit if the leader broker information is not available
        }

        long lastOffset = -1; // Start from the beginning of the partition

        try {
            while (true) {
                // Try to fetch messages from the leader first
                if (!fetchMessagesFromBroker(topic, partitionId, leaderInfo, lastOffset)) {
                     // If failed, try consuming from a follower broker
                    System.err.println("Leader broker failed. Attempting to consume from follower brokers.");
                    boolean consumedFromFollower = false;
                    // Loop through the follower broker IDs and fetch their info
                    for (Integer followerId : partitionMetadata.getFollowers()) {
                        BrokerInfo followerInfo = fetchBrokerInfo(followerId); // Fetch BrokerInfo for the follower
                        if (followerInfo != null && fetchMessagesFromBroker(topic, partitionId, followerInfo, lastOffset)) {
                            consumedFromFollower = true;
                            break; // Stop if a follower broker successfully provided messages
                        }
                    }
                    if (!consumedFromFollower) {
                        System.err.println("All brokers (leader + followers) failed. Retrying in a while.");
                        Thread.sleep(5000); // Retry after some time if all brokers fail
                    }
                }
            }
        } catch (InterruptedException e) {
            System.err.println("Error while consuming messages from Partition " + partitionId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Helper method to fetch messages from a given broker
    private boolean fetchMessagesFromBroker(String topic, int partitionId, BrokerInfo brokerInfo, long lastOffset) {
        try {
            // Construct the URL for long-polling from the broker
            URL url = new URL("http://" + brokerInfo.getHost() + ":" + brokerInfo.getPort()
                    + "/longPolling?topicName=" + topic + "&partitionId=" + partitionId
                    + "&offset=" + lastOffset);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            conn.setConnectTimeout(10000); // 10 seconds timeout for connection
            conn.setReadTimeout(30000); // 30 seconds timeout for reading

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();

                // Deserialize and process the messages
                Gson gson = new GsonBuilder()
                        .registerTypeAdapter(Message.class, new MessageAdapter())
                        .create();
                List<Message> messages = gson.fromJson(response, new TypeToken<List<Message>>() {
                }.getType());

                if (messages == null || messages.isEmpty()) {
                    System.out.println("No new messages available in Partition " + partitionId);
                    return false; // No new messages, continue retrying
                }

                // Ensure messages are sorted by timestamp (latest first)
                messages.sort(Comparator.comparingLong(Message::getOffset));

                for (Message message : messages) {
                    // Check if the current offset is the same as the previously consumed one
                    if (message.getOffset() <= lastOffset) {
                        // Skip consuming this message, as it's already been processed
                        continue;
                    }
                    System.out.println("Consumed message from Partition " + partitionId + ": "
                            + new String(message.getPayload()));
                    lastOffset = message.getOffset(); // Update the offset after processing
                }
                return true; // Successfully consumed messages from this broker
            } else {
                System.err.println("Failed to fetch messages from Partition " + partitionId + " on Broker " 
                        + brokerInfo.getHost() + ":" + brokerInfo.getPort() + ". Response Code: "
                        + responseCode);
                return false; // Failed to fetch messages
            }
        } catch (IOException e) {
            System.err.println("Error while fetching messages from broker " + brokerInfo.getHost() + ":"
                    + brokerInfo.getPort() + " for Partition " + partitionId + ": " + e.getMessage());
            return false; // If an error occurs, return false
        }
    }*/

    private boolean waitForReadiness() {
        int maxRetries = 10; // Maximum number of retries
        int delay = 2000; // Delay between retries (milliseconds)

        for (int i = 0; i < maxRetries; i++) {
            if (isControllerReady()) {
                return true;
            }
            System.out.println("Controller not ready. Retrying in " + (delay / 1000) + " seconds...");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Readiness check interrupted.");
                return false;
            }
        }
        System.err.println("Controller did not become ready after multiple attempts.");
        return false;
    }

    private Map<Integer, PartitionMetadata> fetchMetadataWithRetries(String topic) {
        int retries = 5;
        int delay = 1000; // 1 second delay
        while (retries-- > 0) {
            Map<Integer, PartitionMetadata> metadata = fetchTopicMetadata(topic);
            if (metadata != null) {
                return metadata; // Metadata available
            } else {
                System.err.println("Metadata not found for topic " + topic + ". Retrying...");
                try {
                    Thread.sleep(delay);
                    delay *= 2; // Exponential backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return null; // Return null if metadata could not be fetched
    }

    private Map<Integer, PartitionMetadata> fetchTopicMetadata(String topicName) {
        // Use the existing waitForReadiness method to ensure the controller is ready
        if (!waitForReadiness()) {
            System.err.println("Controller is not ready after retries. Exiting metadata fetch.");
            return null;
        }

        // Fetch metadata after confirming controller readiness
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                StringBuilder responseBuilder = new StringBuilder();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        responseBuilder.append(line);
                    }
                }
                String response = responseBuilder.toString();
                // System.out.println("Received JSON response: " + response);

                // Deserialize the JSON response to Map<Integer, PartitionMetadata>
                Gson gson = new Gson();
                Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                }.getType();
                return gson.fromJson(response, type);
            } else {
                //System.err.println("Failed to fetch metadata for topic " + topicName + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error fetching metadata for topic " + topicName + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private boolean isControllerReady() {
        String readinessUrl = "http://" + controllerHost + ":" + controllerPort + "/readiness";
        try {
            // Create a connection to the readiness endpoint
            URL url = new URL(readinessUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Controller is ready
                //System.out.println("Controller is ready.");
                return true;
            } 
            else if (responseCode == 503) {
                // Controller is not ready yet
                // System.out.println("Controller is not ready yet.");
            } 
            else {
                System.err.println("Unexpected response code from readiness check: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error checking controller readiness: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getBrokerInfo?brokerId=" + brokerId);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();

                if (response.equals("Broker not found")) {
                    System.err.println("Broker not found for broker ID " + brokerId);
                    return null;
                }

                String[] parts = response.split(":");
                if (parts.length < 2) {
                    System.err.println("Invalid broker info format for broker ID " + brokerId + ": " + response);
                    return null;
                }

                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                return new BrokerInfo(host, port);
            } else {
                System.err.println(
                        "Failed to fetch broker info for broker ID " + brokerId + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

/*
 * 
 * Example for communication:
 * If the producer container needs to access the controller:
 * 
 * java
 * Copy code
 * URL url = new URL(
 * "http://controller:8090/longPolling?topicName=TestTopic&partitionId=0&offset="
 * + offset);
 * If the consumer container needs to access broker1 (for internal communication
 * inside Docker network):
 * 
 * java
 * Copy code
 * URL url = new URL(
 * "http://broker1:8081/longPolling?topicName=TestTopic&partitionId=0&offset=" +
 * offset);
 * If you're accessing broker1 from your host machine:
 * 
 * java
 * Copy code
 * URL url = new
 * URL("http://localhost:8085/longPolling?topicName=TestTopic&partitionId=0&
 */