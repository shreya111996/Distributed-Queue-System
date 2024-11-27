package com.distqueue.consumer;

import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;
import com.distqueue.producer.Producer.BrokerInfo;
import com.google.gson.Gson;

import java.io.*;
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


    @SuppressWarnings("unchecked")
    private Map<Integer, PartitionMetadata> fetchMetadata(String topicName) {
        try {
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();

                if (response.startsWith("No metadata found")) {
                    System.err.println("No metadata found for topic " + topicName);
                    return null;
                }

                Gson gson = new Gson();
                return gson.fromJson(response, Map.class);
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
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/getBrokerInfo?brokerId=" + brokerId);
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
}
