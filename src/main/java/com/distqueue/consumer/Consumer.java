package com.distqueue.consumer;

import com.distqueue.adapters.MessageAdapter;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Comparator;

public class Consumer {

    private final String controllerHost;
    private final int controllerPort;
    private final Set<String> consumedMessageKeys = ConcurrentHashMap.newKeySet(); // track consumed messages based on
                                                                                   // composite keys
    // private static final String BROKER_API_URL =
    // "http://localhost:8090/brokers/active"; // Change URL if necessary

    public Consumer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
    }

    public void consume(String topic) {
        // Wait for the controller and metadata to be ready
        if (!waitForReadiness()) {
            System.err.println("Controller not ready. Exiting consume operation.");
            return;
        }
    
        long lastOffset = 0; // Start from the beginning of the partition
        while (true) {
            try {
                // Fetch metadata for the topic
                Map<Integer, PartitionMetadata> topicMetadata = fetchMetadataWithRetries(topic);
                if (topicMetadata == null) {
                    System.err.println("Topic metadata not found for topic " + topic);
                    Thread.sleep(5000); // Retry after delay
                    continue;
                }
    
                boolean success = false;
    
                // Try fetching messages from any available partition
                for (Map.Entry<Integer, PartitionMetadata> entry : topicMetadata.entrySet()) {
                    int partitionId = entry.getKey();
                    PartitionMetadata partitionMetadata = entry.getValue();
    
                    // Attempt to fetch messages from the leader broker first
                    success = fetchMessagesFromBroker(partitionMetadata, topic, partitionId, lastOffset);
                    if (!success) {
                        // If leader fails, try the followers
                        List<Integer> followers = partitionMetadata.getFollowerIds();
                        for (int followerId : followers) {
                            BrokerInfo followerInfo = fetchBrokerInfo(followerId);
                            if (followerInfo != null) {
                                System.out.println("Attempting to consume from follower broker: " + followerInfo.getHost());
                                success = fetchMessages(followerInfo, topic, partitionId, lastOffset);
                                if (success) {
                                    break; // Exit the loop if successful
                                }
                            }
                        }
                    }
    
                    // If the current partition failed (even after trying followers), try the next partition
                    if (success) {
                        break; // If we succeed with any partition, break out of the loop
                    }
                }
    
                // If all partitions fail, refresh metadata and retry
                if (!success) {
                    System.err.println("All brokers failed for topic " + topic + ". Retrying...");
                    Thread.sleep(5000); // Retry after delay
                }
            } catch (Exception e) {
                System.err.println("Error during consumption: " + e.getMessage());
                e.printStackTrace();
                try {
                    Thread.sleep(5000); // Retry after delay
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private boolean fetchMessagesFromBroker(PartitionMetadata partitionMetadata, String topic, int partitionId, long lastOffset) {
        BrokerInfo leaderInfo = fetchBrokerInfo(partitionMetadata.getLeaderId());
        if (leaderInfo == null) {
            System.err.println("Leader broker info not found for broker ID " + partitionMetadata.getLeaderId());
            return false; // Leader broker info unavailable
        }
        return fetchMessages(leaderInfo, topic, partitionId, lastOffset);
    }


    private boolean fetchMessages(BrokerInfo brokerInfo, String topic, int partitionId, long lastOffset) {
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
                List<Message> messages = gson.fromJson(response, new TypeToken<List<Message>>() {}.getType());

                if (messages == null || messages.isEmpty()) {
                    System.out.println("No new messages available in Partition " + partitionId);
                    return true; // No new messages, but no error
                }

                // Ensure messages are sorted by offset (ascending order)
                messages.sort(Comparator.comparingLong(Message::getOffset));
                for (Message message : messages) {
                    // Generate a composite key for the message
                    String messageKey = generateMessageKey(message);

                    // Process only if the message is not a duplicate
                    if (consumedMessageKeys.add(messageKey)) {
                        System.out.println("Consumed message from Partition " + partitionId + ": "
                                + new String(message.getPayload()));
                        lastOffset = message.getOffset() + 1; // Update the offset after processing
                    } else {
                        //System.out.println("Duplicate message skipped: " + messageKey);
                    }
                }
                return true; // Successfully consumed messages
            } else {
                System.err.println("Failed to fetch messages from Partition " + partitionId + ". Response Code: "
                        + responseCode);
                return false; // Failed to fetch messages
            }
        } catch (IOException e) {
            System.err.println("Error while fetching messages from broker " + brokerInfo.getHost() + ":"
                    + brokerInfo.getPort() + " for Partition " + partitionId + ": " + e.getMessage());
            return false; // If an error occurs, return false
        }
    }
    

    // Helper method to generate a composite key
    private String generateMessageKey(Message message) {
        return message.getTopic() + "|" + message.getPartition() + "|" + message.getOffset() + "|"
                + message.getTimestamp();
    }

    private boolean waitForReadiness() {
        int maxRetries = 10; // Maximum number of retries
        int delay = 2000; // Delay between retries (milliseconds)

        for (int i = 0; i < maxRetries; i++) {
            if (isControllerReady()) {
                return true;
            }
            // System.out.println("Controller not ready. Retrying in " + (delay / 1000) + "
            // seconds...");
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
                // System.err.println("Metadata not found for topic " + topic + ".
                // Retrying...");
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
                // System.err.println("Failed to fetch metadata for topic " + topicName + ",
                // response code: " + responseCode);
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
                // System.out.println("Controller is ready.");
                return true;
            } else if (responseCode == 503) {
                // Controller is not ready yet
                // System.out.println("Controller is not ready yet.");
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

// public void consume(String topic) {
// // Wait for the controller and metadata to be ready
// if (!waitForReadiness()) {
// System.err.println("Controller not ready. Exiting consume operation.");
// return;
// }

// long lastOffset = 0; // Start from the beginning of the partition
// while (true) {
// try {
// // Fetch metadata for the topic
// Map<Integer, PartitionMetadata> topicMetadata =
// fetchMetadataWithRetries(topic);
// if (topicMetadata == null) {
// System.err.println("Topic metadata not found for topic " + topic);
// Thread.sleep(5000); // Retry after delay
// continue;
// }

// boolean success = false;

// // Try fetching messages from any available partition
// for (Map.Entry<Integer, PartitionMetadata> entry : topicMetadata.entrySet())
// {
// int partitionId = entry.getKey();
// PartitionMetadata partitionMetadata = entry.getValue();

// // Attempt to fetch messages from the leader broker first
// success = fetchMessagesFromBroker(partitionMetadata, topic, partitionId,
// lastOffset);
// if (!success) {
// // If leader fails, try the followers
// List<Integer> followers = partitionMetadata.getFollowerIds();
// for (int followerId : followers) {
// BrokerInfo followerInfo = fetchBrokerInfo(followerId);
// if (followerInfo != null) {
// System.out.println(
// "Attempting to consume from follower broker: " + followerInfo.getHost());
// success = fetchMessages(followerInfo, topic, partitionId, lastOffset);
// if (success) {
// break; // Exit the loop if successful
// }
// }
// }
// }

// // If the current partition failed (even after trying followers), try the
// next
// // partition
// if (success) {
// // Refresh metadata to pick up leadership changes
// fetchMetadataWithRetries(topic);
// Thread.sleep(5000); // Retry after delay
// }
// }

// } catch (Exception e) {
// System.err.println("Error during consumption: " + e.getMessage());
// e.printStackTrace();
// try {
// Thread.sleep(5000); // Retry after delay
// } catch (InterruptedException ie) {
// Thread.currentThread().interrupt();
// }
// }
// }
// }

// private boolean fetchMessagesFromBroker(PartitionMetadata partitionMetadata,
// String topic, int partitionId,
// long lastOffset) {
// BrokerInfo leaderInfo = fetchBrokerInfo(partitionMetadata.getLeaderId());
// if (leaderInfo == null) {
// System.err.println("Leader broker info not found for broker ID " +
// partitionMetadata.getLeaderId());
// return false; // Leader broker info unavailable
// }
// return fetchMessages(leaderInfo, topic, partitionId, lastOffset);
// }

// private boolean fetchMessages(BrokerInfo brokerInfo, String topic, int
// partitionId, long lastOffset) {
// try {
// // Construct the URL for long-polling from the broker
// URL url = new URL("http://" + brokerInfo.getHost() + ":" +
// brokerInfo.getPort()
// + "/longPolling?topicName=" + topic + "&partitionId=" + partitionId
// + "&offset=" + lastOffset);

// HttpURLConnection conn = (HttpURLConnection) url.openConnection();
// conn.setRequestMethod("GET");
// conn.setConnectTimeout(10000); // 10 seconds timeout for connection
// conn.setReadTimeout(30000); // 30 seconds timeout for reading

// int responseCode = conn.getResponseCode();
// if (responseCode == 200) {
// BufferedReader in = new BufferedReader(new
// InputStreamReader(conn.getInputStream()));
// String response = in.readLine();
// in.close();

// // Deserialize and process the messages
// Gson gson = new GsonBuilder()
// .registerTypeAdapter(Message.class, new MessageAdapter())
// .create();
// List<Message> messages = gson.fromJson(response, new
// TypeToken<List<Message>>() {
// }.getType());

// if (messages == null || messages.isEmpty()) {
// System.out.println("No new messages available in Partition " + partitionId);
// return true; // No new messages, but no error
// }

// // Ensure messages are sorted by offset (ascending order)
// messages.sort(Comparator.comparingLong(Message::getOffset));
// for (Message message : messages) {
// // Generate a composite key for the message
// String messageKey = generateMessageKey(message);

// // Process only if the message is not a duplicate
// if (consumedMessageKeys.add(messageKey)) {
// System.out.println("Consumed message from Partition " + partitionId + ": "
// + new String(message.getPayload()));
// lastOffset = message.getOffset() + 1; // Update the offset after processing
// } else {
// // System.out.println("Duplicate message skipped: " + messageKey);
// }
// }
// return true; // Successfully consumed messages
// } else {
// System.err.println("Failed to fetch messages from Partition " + partitionId +
// ". Response Code: "
// + responseCode);
// return false; // Failed to fetch messages
// }
// } catch (IOException e) {
// // System.err.println("Error while fetching messages from broker " +
// // brokerInfo.getHost() + ":"
// // + brokerInfo.getPort() + " for Partition " + partitionId + ": " +
// // e.getMessage());
// return false; // If an error occurs, return false
// }
// }

// private boolean checkInitialReadiness() {
//     // Wait for at least one active broker
//     int retries = 5; // Number of retries to check readiness
//     while (retries > 0) {
//         try {
//             List<BrokerInfo> activeBrokers = fetchActiveBrokers();
//             if (activeBrokers.size() > 0) {
//                 System.out.println("At least one active broker is ready. Proceeding with consumption.");
//                 return true;
//             } else {
//                 System.out.println("No active brokers available. Retrying...");
//                 Thread.sleep(5000); // Wait before retrying
//                 retries--;
//             }
//         } catch (IOException e) {
//             System.err.println("Error checking readiness: " + e.getMessage());
//         } catch (InterruptedException e) {
//             // TODO Auto-generated catch block
//             e.printStackTrace();
//         }
//     }
//     System.err.println("Failed to detect broker readiness after retries.");
//     return false;
// }

// // Fetch active brokers from the Controller endpoint
// public List<BrokerInfo> fetchActiveBrokers() throws IOException {
//     HttpURLConnection connection = null;
//     BufferedReader reader = null;
//     int maxRetries = 3; // Max retries
//     int retryDelayMs = 2000; // Delay between retries (in milliseconds)

//     for (int attempt = 1; attempt <= maxRetries; attempt++) {
//         try {
//             // Try to establish a connection
//             URL url = new URL(BROKER_API_URL);
//             connection = (HttpURLConnection) url.openConnection();
//             connection.setRequestMethod("GET");
//             connection.setRequestProperty("Accept", "application/json");

//             // Read the response
//             reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//             StringBuilder response = new StringBuilder();
//             String line;

//             while ((line = reader.readLine()) != null) {
//                 response.append(line);
//             }

//             // Convert the response JSON to a list of BrokerInfo objects
//             Gson gson = new Gson();
//             BrokerInfo[] brokersArray = gson.fromJson(response.toString(), BrokerInfo[].class);
//             return List.of(brokersArray); // Convert array to list

//         } catch (IOException e) {
//             // If this is the last attempt, rethrow the exception
//             if (attempt == maxRetries) {
//                 e.printStackTrace();
//                 throw new IOException(
//                         "Error fetching active brokers from the endpoint after " + maxRetries + " attempts.");
//             }

//             // Otherwise, log the error and retry after a delay
//             System.out.println("Attempt " + attempt + " failed. Retrying in " + retryDelayMs + " ms...");
//             try {
//                 TimeUnit.MILLISECONDS.sleep(retryDelayMs); // Delay before retry
//             } catch (InterruptedException ie) {
//                 Thread.currentThread().interrupt(); // Restore interrupt status
//             }

//         } finally {
//             if (reader != null) {
//                 reader.close();
//             }
//             if (connection != null) {
//                 connection.disconnect();
//             }
//         }
//     }

//     // Return empty list if all retries fail (just in case)
//     return List.of();
// }
