package com.distqueue.consumer;

// import com.distqueue.adapters.MessageAdapter;
import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;
import com.distqueue.producer.Producer.BrokerInfo;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.UUID;

public class Consumer {

    private final String controllerHost;
    private final int controllerPort;

    public Consumer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
    }

    public void consume(String topic) {

        // Wait for the controller to be ready
        if (!waitForReadiness()) {
            System.err.println("Controller not ready. Exiting consume operation.");
            return;
        }

        // Wait for metadata to be available
        Map<Integer, PartitionMetadata> topicMetadata = null;
        int retries = 5;
        int delay = 1000; // 1 second delay between retries
        while (retries-- > 0) {
            topicMetadata = fetchTopicMetadata(topic);
            if (topicMetadata != null) {
                break; // Metadata is available, break the loop
            } else {
                System.err.println("Metadata not found for topic " + topic + ". Retrying...");
                try {
                    Thread.sleep(delay);
                    delay *= 2; // Exponential backoff for retries
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (topicMetadata == null) {
            System.err.println("Topic metadata not found for topic " + topic);
            return;
        }

        // For simplicity, consume from partition 0
        int partitionId = 0;
        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);

        if (partitionMetadata == null) {
            System.err.println("Partition metadata not found for partition " + partitionId);
            return;
        }

        int leaderId = partitionMetadata.getLeaderId();
        if (leaderId == -1) {
            System.err.println("Leader ID not available for partition " + partitionId);
            return;
        }

        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);

        if (leaderInfo != null) {
            try {
                UUID offset = null; // Use UUID instead of int
                long pollingTimeout = 30000; // Timeout for long polling (30 seconds)
                int pollingRetries = 5;
                int pollingDelay = 1000;

                while (true) {

                    URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort()
                            + "/longPolling?topicName=" + topic + "&partitionId=" + partitionId + "&offset="
                            + (offset == null ? 0 : offset.toString()));

                    System.out.println("Fetching messages from: " + url);
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("GET");

                    // Set a timeout to handle long-polling issues
                    conn.setConnectTimeout(10000); // 10 seconds timeout for connection
                    conn.setReadTimeout(10000); // 10 seconds timeout for reading

                    int responseCode = conn.getResponseCode();
                    if (responseCode == 200) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        String response = in.readLine();
                        in.close();

                        // Deserialize and process the messages
                        Gson gson = new Gson();
                        @SuppressWarnings("unchecked")
                        List<Message> messages = gson.fromJson(response, List.class);

                        if (messages == null || messages.isEmpty()) {
                            System.err.println("No new messages available.");
                            continue; // Try again if no new messages
                        }

                        // Sort and process messages
                        messages.sort(Comparator.comparing(Message::getTimestamp));
                        messages.forEach(
                                message -> System.out.println("Consumed message: " + new String(message.getPayload())));

                        // Update offset for next polling
                        offset = messages.get(messages.size() - 1).getMessageId(); // This is a UUID now
                    } else {
                        if (pollingRetries-- > 0) {
                            System.out.println("Retrying in " + pollingDelay + " ms...");
                            Thread.sleep(pollingDelay);
                            pollingDelay *= 2;
                        } else {
                            break;
                        }
                    }

                    // Wait before making the next long polling request (if needed)
                    Thread.sleep(1000);
                }

            } catch (IOException | InterruptedException e) {
                System.err.println("Error fetching messages from broker " + leaderId + ": " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.err.println("Leader broker info not found for broker ID " + leaderId);
        }
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
                System.out.println("Received JSON response: " + response);

                // Deserialize the JSON response to Map<Integer, PartitionMetadata>
                Gson gson = new Gson();
                Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                }.getType();
                return gson.fromJson(response, type);
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
If the producer container needs to access the controller:

java
Copy code
URL url = new URL("http://controller:8090/longPolling?topicName=TestTopic&partitionId=0&offset=" + offset);
If the consumer container needs to access broker1 (for internal communication inside Docker network):

java
Copy code
URL url = new URL("http://broker1:8081/longPolling?topicName=TestTopic&partitionId=0&offset=" + offset);
If you're accessing broker1 from your host machine:

java
Copy code
URL url = new URL("http://localhost:8085/longPolling?topicName=TestTopic&partitionId=0&
 */