package com.distqueue.producer;

import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class Producer {

    private final String controllerHost;
    private final int controllerPort;

    public Producer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
    }

    public void send(String topic, byte[] payload) {
        // Ensure the controller and brokers are ready
        if (!waitForReadiness()) {
            System.err.println("Controller or brokers are not ready. Aborting send operation.");
            return;
        }

        // Fetch metadata with retry logic
        Map<Integer, PartitionMetadata> topicMetadata = null;
        int retries = 5;
        int delay = 1000; // 1 second initial delay

        for (int i = 0; i < retries; i++) {
            topicMetadata = fetchMetadata(topic);
            if (topicMetadata != null)
                break;

            System.err.println("Retrying to fetch metadata... Attempt " + (i + 1));
            try {
                Thread.sleep(delay);
                delay *= 2; // Exponential backoff
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Retry interrupted.");
                return;
            }
        }

        if (topicMetadata == null) {
            System.err.println("Failed to fetch metadata after multiple attempts. Aborting send operation.");
            return;
        }

        // Send the message to partition 0 (for simplicity)
        int partitionId = 0;
        Message message = new Message(topic, partitionId, payload);

        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);
        if (partitionMetadata == null) {
            System.err.println("No metadata found for partition " + partitionId);
            return;
        }

        int leaderId = partitionMetadata.getLeaderId();
        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);
        if (leaderInfo == null) {
            System.err.println("Leader broker info not found for broker ID " + leaderId);
            return;
        }

        // Publish the message
        publishMessage(leaderInfo, message);
    }

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

    private void publishMessage(BrokerInfo leaderInfo, Message message) {
        try {
            URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort() + "/publishMessage");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            try (ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream())) {
                out.writeObject(message);
                out.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Message sent successfully to broker " + leaderInfo.getHost());
            } else {
                System.err.println("Failed to send message, response code: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error publishing message: " + e.getMessage());
            e.printStackTrace();
        }
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
                System.out.println("Controller is ready.");
                return true;
            } else if (responseCode == 503) {
                // Controller is not ready
                System.out.println("Controller is not ready.");
            } else {
                System.err.println("Unexpected response code from readiness check: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error checking controller readiness: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    private Map<Integer, PartitionMetadata> fetchMetadata(String topicName) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String response = in.readLine();
                    Gson gson = new Gson();
                    Type mapType = new TypeToken<Map<Integer, PartitionMetadata>>() {
                    }.getType();
                    return gson.fromJson(response, mapType);
                }
            } else {
                System.err.println(
                        "Failed to fetch metadata for topic " + topicName + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error fetching metadata for topic " + topicName + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getBrokerInfo?brokerId=" + brokerId);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String response = in.readLine();
                    String[] parts = response.split(":");
                    if (parts.length == 2) {
                        return new BrokerInfo(parts[0], Integer.parseInt(parts[1]));
                    }
                }
            } else {
                System.err.println("Failed to fetch broker info for broker ID " + brokerId);
            }
        } catch (IOException e) {
            System.err.println("Error fetching broker info: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    // Add the createTopic method
    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        try {
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/createTopic");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            String params = "topicName=" + topicName + "&numPartitions=" + numPartitions + "&replicationFactor="
                    + replicationFactor;
            try (OutputStream os = conn.getOutputStream()) {
                os.write(params.getBytes());
                os.flush();
            }

            if (conn.getResponseCode() == 200) {
                System.out.println("Topic " + topicName + " created successfully.");
            } else {
                System.err.println("Failed to create topic " + topicName);
            }
        } catch (IOException e) {
            System.err.println("Error creating topic: " + e.getMessage());
            e.printStackTrace();
        }
    }

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
}
