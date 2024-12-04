package com.distqueue.consumer;

import com.distqueue.adapters.MessageAdapter;
import com.distqueue.core.Message;
import com.distqueue.logging.LogMessage;
import com.distqueue.logging.LogRepository;
import com.distqueue.metadata.PartitionMetadata;
import com.distqueue.producer.Producer.BrokerInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import java.util.ArrayList;
import java.util.Comparator;

public class Consumer {

    private final String controllerHost;
    private final int controllerPort;
    private final Set<String> consumedMessageKeys = ConcurrentHashMap.newKeySet(); // track consumed messages based on
                                                                                   // composite keys
    // private static final String BROKER_API_URL =
    // "http://localhost:8090/brokers/active"; // Change URL if necessary
    private static final int LOG_RATE_CONTROL = 25;
    private int messageCount = 0; // Counter for messages consumed
    private long startTime = System.currentTimeMillis(); // Start time for throughput calculation
    private long totalLatency = 0; // Total latency for calculating average latency

    private static final List<String> logMessages = new ArrayList<>();

    public Consumer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        // Start the HTTP server
        startHttpServer();
    }

    private static void startHttpServer() {
        try {
            int port = 8083; // Choose an available port for the consumer
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/logs", new LogsHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            System.out.println("Consumer HTTP server started on port " + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Handler to serve logs
    static class LogsHandler implements HttpHandler {
        private static final Gson gson = new Gson();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Add CORS headers
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    
            // Convert log messages to JSON
            String jsonResponse = gson.toJson(logMessages);
    
            // Send the response
            exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(jsonResponse.getBytes());
            os.close();
        }
    }

    // Modify your logging methods to add logs to logMessages
    private void log(String message) {
        System.out.println(message);
        synchronized (logMessages) {
            logMessages.add(message);
        }
    }

    public void consume(String topic) {
        // Wait for the controller and metadata to be ready
        if (!waitForReadiness()) {
            log("Controller not ready. Exiting consume operation.");
            return;
        }

        long lastOffset = 0; // Start from the beginning of the partition
        while (true) {
            try {
                // Fetch metadata for the topic
                Map<Integer, PartitionMetadata> topicMetadata = fetchMetadataWithRetries(topic);
                if (topicMetadata == null) {
                    log("Topic metadata not found for topic " + topic);
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
                                log(
                                        "Attempting to consume from follower broker: " + followerInfo.getHost());
                                success = fetchMessages(followerInfo, topic, partitionId, lastOffset);
                                if (success) {
                                    break; // Exit the loop if successful
                                }
                            }
                        }
                    }

                    // If the current partition failed (even after trying followers), try the next
                    // partition
                    if (success) {
                        break; // If we succeed with any partition, break out of the loop
                    }
                }

                // If all partitions fail, refresh metadata and retry
                if (!success) {
                    log("All brokers failed for topic " + topic + ". Retrying...");
                    Thread.sleep(5000); // Retry after delay
                }
            } catch (Exception e) {
                log("Error during consumption: " + e.getMessage());
                e.printStackTrace();
                try {
                    Thread.sleep(5000); // Retry after delay
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            finally {
                // Log aggregated metrics after consumption of all messages
                logThroughput();
                logAverageLatency();
            }
        }
    }

    private boolean fetchMessagesFromBroker(PartitionMetadata partitionMetadata, String topic, int partitionId,
            long lastOffset) {
        BrokerInfo leaderInfo = fetchBrokerInfo(partitionMetadata.getLeaderId());
        if (leaderInfo == null) {
            log("Leader broker info not found for broker ID " + partitionMetadata.getLeaderId());
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
            conn.setReadTimeout(100000);

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();
                messageCount++;
                // Deserialize and process the messages
                Gson gson = new GsonBuilder()
                        .registerTypeAdapter(Message.class, new MessageAdapter())
                        .create();
                List<Message> messages = gson.fromJson(response, new TypeToken<List<Message>>() {
                }.getType());

                if (messages == null || messages.isEmpty()) {
                    log("No new messages available in Partition " + partitionId);
                    return true; // No new messages, but no error
                }

                // Ensure messages are sorted by offset (ascending order)
                messages.sort(Comparator.comparingLong(Message::getOffset));
                for (Message message : messages) {
                    // Generate a composite key for the message
                    String messageKey = generateMessageKey(message);

                    // Process only if the message is not a duplicate
                    if (consumedMessageKeys.add(messageKey)) {
                        log("Consumed message from Partition " + partitionId + ": "
                                + new String(message.getPayload()));
                        lastOffset = message.getOffset() + 1; // Update the offset after processing
                        messageCount++; // Increment the message count
                        logLatency(message.getTimestamp().toEpochMilli()); // Log latency
                    } else {
                        // log("Duplicate message skipped: " + messageKey);
                    }
                }
                return true; // Successfully consumed messages
            } else {
                log("Failed to fetch messages from Partition " + partitionId + ". Response Code: "
                        + responseCode);
                return false; // Failed to fetch messages
            }
        } catch (IOException e) {
            log("Error while fetching messages from broker " + brokerInfo.getHost() + ":"
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
            // log("Controller not ready. Retrying in " + (delay / 1000) + "
            // seconds...");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log("Readiness check interrupted.");
                return false;
            }
        }
        log("Controller did not become ready after multiple attempts.");
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
                // log("Metadata not found for topic " + topic + ".
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
            log("Controller is not ready after retries. Exiting metadata fetch.");
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
                // log("Received JSON response: " + response);

                // Deserialize the JSON response to Map<Integer, PartitionMetadata>
                Gson gson = new Gson();
                Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                }.getType();
                return gson.fromJson(response, type);
            } else {
                // log("Failed to fetch metadata for topic " + topicName + ",
                // response code: " + responseCode);
            }
        } catch (IOException e) {
            log("Error fetching metadata for topic " + topicName + ": " + e.getMessage());
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
                // log("Controller is ready.");
                return true;
            } else if (responseCode == 503) {
                // Controller is not ready yet
                // log("Controller is not ready yet.");
            } else {
                log("Unexpected response code from readiness check: " + responseCode);
            }
        } catch (IOException e) {
            log("Error checking controller readiness: " + e.getMessage());
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
                    log("Broker not found for broker ID " + brokerId);
                    return null;
                }

                String[] parts = response.split(":");
                if (parts.length < 2) {
                    log("Invalid broker info format for broker ID " + brokerId + ": " + response);
                    return null;
                }

                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                return new BrokerInfo(host, port);
            } else {
                log(
                        "Failed to fetch broker info for broker ID " + brokerId + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void logThroughput() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        if (elapsedTime > 0) {
            double throughput = (messageCount * 1000.0) / elapsedTime; // Messages per second
            if (messageCount % LOG_RATE_CONTROL == 1) {
                String logMessage = "Throughput: " + throughput + " messages/second";
                LogRepository.addLog("Consumer", logMessage);
            }
        }
    }

    private void logLatency(long productionTimestamp) {
        long consumptionTimestamp = System.currentTimeMillis();
        long latency = consumptionTimestamp - productionTimestamp;
        totalLatency += latency;
    }

    private void logAverageLatency() {
        double averageLatency = totalLatency / (double) messageCount;
        if (messageCount % LOG_RATE_CONTROL == 1) {
            String logMessage = "Average End-to-End Latency: " + averageLatency + " ms";
            LogRepository.addLog("Controller", logMessage);
        }
    }


    // class ConsumerLogsStreamHandler implements HttpHandler {
    //     @Override
    //     public void handle(HttpExchange exchange) throws IOException {
    //         // Set the response type to event-stream for SSE
    //         exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
    //         exchange.sendResponseHeaders(200, 0);
    
    //         OutputStream os = exchange.getResponseBody();
            
    //         // Keep the connection open and send log data periodically
    //         while (true) {
    //                 // Get the logs for the controller (you can modify this to get logs from other components if needed)
    //             List<LogMessage> controllerLogs = LogRepository.getLogsBySource("Controller");
                
    //             // Map LogMessages to Strings, extracting only the message or any other desired info
    //             List<String> logMessages = controllerLogs.stream()
    //                                                     .map(log -> "Timestamp: " + log.getTimestamp() + " | " + log.getMessage())
    //                                                     .collect(Collectors.toList());

    //             // If there are new logs, send them to the client via SSE
    //             if (!logMessages.isEmpty()) {
    //                 // Join the logs into a single string, one per line, with each log prefixed by "data:"
    //                 String response = logMessages.stream()
    //                                             .map(log -> "data: " + log + "\n\n") // Format as SSE event
    //                                             .collect(Collectors.joining());
    //                 os.write(response.getBytes());
    //                 os.flush();
    //             }

    //             // Sleep for a while before sending new logs (simulate waiting for new logs)
    //             try {
    //                 Thread.sleep(5000); // Wait for 5 seconds before sending new data
    //             } catch (InterruptedException e) {
    //                 Thread.currentThread().interrupt();
    //             }
    //         }
    //     }
    // }
}

// public void consume(String topic) {
// // Step 1: Check if the controller is ready
// if (!waitForReadiness()) {
// log("Controller not ready. Exiting consume operation.");
// return;
// }

// // Step 2: Wait for at least one active broker
// if (!checkActiveBrokers()) {
// log("No active brokers available. Exiting consume
// operation.");
// return;
// }

// long lastOffset = 0; // Start from the beginning of the partition
// while (true) {
// try {
// // Step 3: Fetch metadata for the topic
// Map<Integer, PartitionMetadata> topicMetadata =
// fetchMetadataWithRetries(topic);
// if (topicMetadata == null) {
// log("Topic metadata not found for topic " + topic);
// Thread.sleep(5000); // Retry after delay
// continue;
// }

// boolean success = false;

// // Try fetching messages from any available partition
// for (Map.Entry<Integer, PartitionMetadata> entry : topicMetadata.entrySet())
// {
// int partitionId = entry.getKey();
// PartitionMetadata partitionMetadata = entry.getValue();

// // Step 4: Attempt to fetch messages from the leader broker
// success = fetchMessagesFromBroker(partitionMetadata, topic, partitionId,
// lastOffset);
// if (!success) {
// // If leader fails, try the followers
// List<Integer> followers = partitionMetadata.getFollowerIds();
// for (int followerId : followers) {
// BrokerInfo followerInfo = fetchBrokerInfo(followerId);
// if (followerInfo != null) {
// log("Attempting to consume from follower broker: " +
// followerInfo.getHost());
// success = fetchMessages(followerInfo, topic, partitionId, lastOffset);
// if (success) {
// break; // Exit the loop if successful
// }
// }
// }
// }

// // If successful, refresh metadata for leadership changes
// if (success) {
// fetchMetadataWithRetries(topic);
// Thread.sleep(5000); // Retry after delay
// }
// }

// // Step 5: Monitor active brokers
// if (!checkActiveBrokers()) {
// log("Active broker count dropped to zero. Halting
// consumption.");
// while (!checkActiveBrokers()) {
// log("Waiting for active brokers...");
// Thread.sleep(5000);
// }
// log("Active brokers restored. Resuming consumption.");
// }

// } catch (Exception e) {
// log("Error during consumption: " + e.getMessage());
// e.printStackTrace();
// try {
// Thread.sleep(5000); // Retry after delay
// } catch (InterruptedException ie) {
// Thread.currentThread().interrupt();
// }
// }
// }
// }

// // Method to check if at least one active broker is available
// private boolean checkActiveBrokers() {
// int maxRetries = 5; // Maximum number of retries
// int retryDelay = 10000; // Delay in milliseconds between retries (10 seconds)

// for (int attempt = 1; attempt <= maxRetries; attempt++) {
// try {
// URL url = new URL("http://localhost:8090/brokers/active/count");
// HttpURLConnection connection = (HttpURLConnection) url.openConnection();
// connection.setRequestMethod("GET");
// connection.setConnectTimeout(5000); // Set connection timeout (5 seconds)
// connection.setReadTimeout(5000); // Set read timeout (5 seconds)

// // Check if the connection is successful
// int responseCode = connection.getResponseCode();
// if (responseCode == HttpURLConnection.HTTP_OK) {
// // Read and parse the response
// BufferedReader reader = new BufferedReader(new
// InputStreamReader(connection.getInputStream()));
// StringBuilder response = new StringBuilder();
// String line;
// while ((line = reader.readLine()) != null) {
// response.append(line);
// }
// reader.close();

// // Parse the JSON response
// JsonObject jsonResponse =
// JsonParser.parseString(response.toString()).getAsJsonObject();
// int activeBrokerCount = jsonResponse.get("activeBrokerCount").getAsInt();

// // If the count of active brokers is greater than 0, return true
// return activeBrokerCount > 0;
// } else {
// log("Error: Received HTTP " + responseCode + " while
// connecting to broker count endpoint.");
// }
// } catch (IOException e) {
// log("Attempt " + attempt + " failed: IOException while
// checking active brokers: " + e.getMessage());
// e.printStackTrace();
// }

// // If the connection failed, wait for the retry delay before retrying
// if (attempt < maxRetries) {
// log("Retrying in " + retryDelay / 1000 + " seconds...");
// try {
// Thread.sleep(retryDelay); // Wait for the specified delay before retrying
// } catch (InterruptedException ie) {
// log("Retry delay interrupted: " + ie.getMessage());
// }
// }
// }

// // If we reach here, the maximum number of retries has been exhausted
// log("Failed to check active brokers after " + maxRetries + "
// attempts.");
// return false;
// }

// private boolean fetchMessagesFromBroker(PartitionMetadata partitionMetadata,
// String topic, int partitionId, long lastOffset) {
// BrokerInfo leaderInfo = fetchBrokerInfo(partitionMetadata.getLeaderId());
// if (leaderInfo == null) {
// log("Leader broker info not found for broker ID " +
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
// TypeToken<List<Message>>() {}.getType());

// if (messages == null || messages.isEmpty()) {
// log("No new messages available in Partition " + partitionId);
// return true; // No new messages, but no error
// }

// // Ensure messages are sorted by offset (ascending order)
// messages.sort(Comparator.comparingLong(Message::getOffset));
// for (Message message : messages) {
// // Generate a composite key for the message
// String messageKey = generateMessageKey(message);

// // Process only if the message is not a duplicate
// if (consumedMessageKeys.add(messageKey)) {
// log("Consumed message from Partition " + partitionId + ": "
// + new String(message.getPayload()));
// lastOffset = message.getOffset() + 1; // Update the offset after processing
// } else {
// //log("Duplicate message skipped: " + messageKey);
// }
// }
// return true; // Successfully consumed messages
// } else {
// log("Failed to fetch messages from Partition " + partitionId +
// ". Response Code: "
// + responseCode);
// return false; // Failed to fetch messages
// }
// } catch (IOException e) {
// log("Error while fetching messages from broker " +
// brokerInfo.getHost() + ":"
// + brokerInfo.getPort() + " for Partition " + partitionId + ": " +
// e.getMessage());
// return false; // If an error occurs, return false
// }
// }

// // Helper method to generate a composite key
// private String generateMessageKey(Message message) {
// return message.getTopic() + "|" + message.getPartition() + "|" +
// message.getOffset() + "|"
// + message.getTimestamp();
// }

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
// if (!checkInitialReadiness()) {
// log("Controller not ready. Exiting consume operation.");
// return;
// }

// long lastOffset = 0; // Start from the beginning of the partition
// while (true) {
// try {
// // Fetch metadata for the topic
// Map<Integer, PartitionMetadata> topicMetadata =
// fetchMetadataWithRetries(topic);
// if (topicMetadata == null) {
// log("Topic metadata not found for topic " + topic);
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
// log(
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
// log("Error during consumption: " + e.getMessage());
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
// log("Leader broker info not found for broker ID " +
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
// log("No new messages available in Partition " + partitionId);
// return true; // No new messages, but no error
// }

// // Ensure messages are sorted by offset (ascending order)
// messages.sort(Comparator.comparingLong(Message::getOffset));
// for (Message message : messages) {
// // Generate a composite key for the message
// String messageKey = generateMessageKey(message);

// // Process only if the message is not a duplicate
// if (consumedMessageKeys.add(messageKey)) {
// log("Consumed message from Partition " + partitionId + ": "
// + new String(message.getPayload()));
// lastOffset = message.getOffset() + 1; // Update the offset after processing
// } else {
// // log("Duplicate message skipped: " + messageKey);
// }
// }
// return true; // Successfully consumed messages
// } else {
// log("Failed to fetch messages from Partition " + partitionId +
// ". Response Code: "
// + responseCode);
// return false; // Failed to fetch messages
// }
// } catch (IOException e) {
// // log("Error while fetching messages from broker " +
// // brokerInfo.getHost() + ":"
// // + brokerInfo.getPort() + " for Partition " + partitionId + ": " +
// // e.getMessage());
// return false; // If an error occurs, return false
// }
// }

// private boolean checkInitialReadiness() {
// // Wait for at least one active broker
// int retries = 5; // Number of retries to check readiness
// while (retries > 0) {
// try {
// List<BrokerInfo> activeBrokers = fetchActiveBrokers();
// if (activeBrokers.size() > 0) {
// log("At least one active broker is ready. Proceeding with
// consumption.");
// return true;
// } else {
// log("No active brokers available. Retrying...");
// Thread.sleep(5000); // Wait before retrying
// retries--;
// }
// } catch (IOException e) {
// log("Error checking readiness: " + e.getMessage());
// } catch (InterruptedException e) {
// e.printStackTrace();
// }
// }
// log("Failed to detect broker readiness after retries.");
// return false;
// }

// // Fetch active brokers from the Controller endpoint
// public List<BrokerInfo> fetchActiveBrokers() throws IOException {
// HttpURLConnection connection = null;
// BufferedReader reader = null;
// int maxRetries = 3; // Max retries
// int retryDelayMs = 2000; // Delay between retries (in milliseconds)

// for (int attempt = 1; attempt <= maxRetries; attempt++) {
// try {
// // Try to establish a connection
// URL url = new URL(BROKER_API_URL);
// connection = (HttpURLConnection) url.openConnection();
// connection.setRequestMethod("GET");
// connection.setRequestProperty("Accept", "application/json");

// // Read the response
// reader = new BufferedReader(new
// InputStreamReader(connection.getInputStream()));
// StringBuilder response = new StringBuilder();
// String line;

// while ((line = reader.readLine()) != null) {
// response.append(line);
// }

// // Convert the response JSON to a list of BrokerInfo objects
// Gson gson = new Gson();
// BrokerInfo[] brokersArray = gson.fromJson(response.toString(),
// BrokerInfo[].class);
// return List.of(brokersArray); // Convert array to list

// } catch (IOException e) {
// // If this is the last attempt, rethrow the exception
// if (attempt == maxRetries) {
// e.printStackTrace();
// throw new IOException(
// "Error fetching active brokers from the endpoint after " + maxRetries + "
// attempts.");
// }

// // Otherwise, log the error and retry after a delay
// log("Attempt " + attempt + " failed. Retrying in " +
// retryDelayMs + " ms...");
// try {
// TimeUnit.MILLISECONDS.sleep(retryDelayMs); // Delay before retry
// } catch (InterruptedException ie) {
// Thread.currentThread().interrupt(); // Restore interrupt status
// }

// } finally {
// if (reader != null) {
// reader.close();
// }
// if (connection != null) {
// connection.disconnect();
// }
// }
// }

// // Return empty list if all retries fail (just in case)
// return List.of();
// }
