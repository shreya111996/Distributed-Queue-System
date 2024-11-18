package com.distqueue.consumer;

import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;
import com.distqueue.producer.Producer.BrokerInfo;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
                URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort()
                        + "/consumeMessages?topicName=" + topic + "&partitionId=" + partitionId + "&offset=0");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    String response = in.readLine();
                    in.close();

                    // Debug log to ensure the Base64 response is correct
                    System.out.println("Received Base64 encoded metadata: " + response);

                    // Decode the Base64 string
                    byte[] data = Base64.getDecoder().decode(response.trim().replaceAll("\\s", ""));

                    // Ensure decoded data is not empty or corrupted
                    if (data == null || data.length == 0) {
                        System.err.println("Decoded Base64 data is empty or invalid");
                        return;
                    }

                    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                        @SuppressWarnings("unchecked")
                        List<Message> messages = (List<Message>) ois.readObject();
                        
                        // Sort the messages by timestamp
                        messages.sort(Comparator.comparing(Message::getTimestamp));

                        messages.forEach(message -> System.out.println("Consumed message: " + new String(message.getPayload())));
                    } catch (IOException | ClassNotFoundException e) {
                        System.err.println("Error during deserialization of the message list: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Failed to consume messages from broker " + leaderId);
                }
            } catch (IOException e) {
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

                // Debug log to ensure the response is correct
                System.out.println("Received metadata response: " + response);

                // Decode the Base64-encoded metadata
                byte[] data = Base64.getDecoder().decode(response.trim().replaceAll("\\s", ""));

                // Ensure decoded data is not empty or corrupted
                if (data == null || data.length == 0) {
                    System.err.println("Decoded Base64 data is empty or invalid");
                    return null;
                }

                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                    Map<Integer, PartitionMetadata> topicMetadata = (Map<Integer, PartitionMetadata>) ois.readObject();
                    return topicMetadata;
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Error during deserialization of topic metadata: " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.err.println("Failed to fetch metadata for topic " + topicName);
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
                    System.err.println("Invalid broker info format for broker ID " + brokerId);
                    return null;
                }

                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                return new BrokerInfo(host, port);
            } else {
                System.err.println("Failed to fetch broker info for broker ID " + brokerId);
            }
        } catch (IOException e) {
            System.err.println("Error fetching broker info for broker ID " + brokerId + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }
}
