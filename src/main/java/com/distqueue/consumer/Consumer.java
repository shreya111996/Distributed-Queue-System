package com.distqueue.consumer;

// import com.distqueue.adapters.MessageAdapter;
import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;
import com.distqueue.producer.Producer.BrokerInfo;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
        // Fetch metadata from controller
        Map<Integer, PartitionMetadata> topicMetadata = fetchMetadata(topic);
        if (topicMetadata == null) {
            System.err.println("Topic metadata not found for topic " + topic);
            return;
        }

        // For simplicity, consume from partition 0
        int partitionId = 0;

        // Fetch leader broker info from metadata
        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);
        int leaderId = partitionMetadata.getLeaderId();
        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);

        if (leaderInfo != null) {
            try {
                // Long polling: continuously request new messages
                UUID offset = null; // Use UUID instead of int
                long pollingTimeout = 30000; // Timeout for long polling (30 seconds)
                int retries = 5;
                int delay = 1000; // Start with 1 second delay

                while (true) {
                    URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort()
                            + "/longPolling?topicName=" + topic + "&partitionId=" + partitionId + "&offset=" + (offset == null ? 0 : offset.toString()));
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("GET");

                    int responseCode = conn.getResponseCode();
                    if (responseCode == 200) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        String response = in.readLine();
                        in.close();

                        // Debug log to ensure the response is correct
                        System.out.println("Received JSON response: " + response);

                        // Deserialize the JSON string into a list of messages
                        Gson gson = new Gson();
                        @SuppressWarnings("unchecked")
                        List<Message> messages = gson.fromJson(response, List.class);

                        if (messages == null || messages.isEmpty()) {
                            System.err.println("No new messages available.");
                            continue; // Try again if no new messages
                        }

                        // Sort the messages by timestamp (or any other criteria if necessary)
                        messages.sort(Comparator.comparing(Message::getTimestamp));

                        messages.forEach(message -> System.out.println("Consumed message: " + new String(message.getPayload())));

                        // Update offset for next polling
                        offset = messages.get(messages.size() - 1).getMessageId(); // This is a UUID now
                    } else if (responseCode == 408) {
                        // Timeout reached, no new messages available
                        System.err.println("Timeout reached, no new messages available.");
                        break; // Exit the polling loop
                    } else {
                        System.err.println("Failed to consume messages from broker " + leaderId);
                        if (retries-- > 0) {
                            System.out.println("Retrying in " + delay + " ms...");
                            Thread.sleep(delay);
                            delay *= 2; // Exponential backoff
                        } else {
                            break; // Exit after retries
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

    private Map<Integer, PartitionMetadata> fetchMetadata(String topicName) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Read the response as a JSON string
                StringBuilder responseBuilder = new StringBuilder();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        responseBuilder.append(line);
                    }
                }
                String response = responseBuilder.toString();
                System.out.println("Received JSON response: " + response);

                // Check if the response contains an error field
                JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
                if (jsonObject.has("error")) {
                    String errorMessage = jsonObject.get("error").getAsString();
                    System.err.println("Error fetching metadata: " + errorMessage);
                    return null; // Return null for error responses
                }

                // Deserialize JSON to Map<Integer, PartitionMetadata>
                Gson gson = new Gson();
                Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                }.getType();
                return gson.fromJson(response.toString(), type);
            } else {
                System.err.println("Failed to fetch metadata for topic " + topicName + ", response code: " + responseCode);
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
                System.err.println("Failed to fetch broker info for broker ID " + brokerId + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean checkTopicExists(String topicName) throws IOException {

        // Call the controller's metadata API to check if the topic exists
        return fetchMetadata(topicName) != null;
    }
}
