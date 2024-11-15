package com.distqueue.producer;

import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;

public class Producer {

    private final String controllerHost;
    private final int controllerPort;

    public Producer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
    }

    public void send(String topic, byte[] payload) {
        // Fetch metadata from controller
        Map<Integer, PartitionMetadata> topicMetadata = fetchMetadata(topic);
        if (topicMetadata == null) {
            System.err.println("Topic metadata not found for topic " + topic);
            return;
        }
    
        // For simplicity, send to partition 0
        int partitionId = 0;
        Message message = new Message(topic, partitionId, payload);
    
        // Fetch leader broker info from metadata
        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);
        int leaderId = partitionMetadata.getLeaderId();
        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);
    
        if (leaderInfo != null) {
            try {
                URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort() + "/publishMessage");
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
                    System.out.println("Message sent to broker " + leaderId + " on topic: " + topic);
                } else {
                    System.err.println("Failed to send message to broker " + leaderId);
                }
            } catch (IOException e) {
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

                byte[] data = Base64.getDecoder().decode(response);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
                Map<Integer, PartitionMetadata> topicMetadata = (Map<Integer, PartitionMetadata>) ois.readObject();
                return topicMetadata;
            } else {
                System.err.println("Failed to fetch metadata for topic " + topicName);
            }
        } catch (IOException | ClassNotFoundException e) {
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
    
            String params = "topicName=" + topicName + "&numPartitions=" + numPartitions + "&replicationFactor=" + replicationFactor;
            OutputStream os = conn.getOutputStream();
            os.write(params.getBytes());
            os.flush();
            os.close();
    
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Topic " + topicName + " created.");
            } else {
                System.err.println("Failed to create topic " + topicName);
            }
        } catch (IOException e) {
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
