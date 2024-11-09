package com.distqueue.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.controller.Controller;
import com.distqueue.core.Message;
import com.distqueue.core.Partition;
import com.distqueue.core.Topic;


public class Broker {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Controller controller;
    private final int brokerId;
    private final int port;

    public Broker(Controller controller) {
        this.controller = controller;
        this.brokerId = Integer.parseInt(System.getenv().getOrDefault("BROKER_ID", "1")); // Defaulting to 1 if not set
        this.port = Integer.parseInt(System.getenv().getOrDefault("BROKER_PORT", "9092")); // Defaulting to 9092 if not set
    }

    public void createTopic(String topicName, int partitionCount, int replicationFactor) {
        Topic topic = new Topic(topicName, partitionCount, replicationFactor);
        topics.put(topicName, topic);
        
        // Register topic metadata in the controller
        controller.addTopicMetadata(topicName, partitionCount, replicationFactor);
    }

    public void publishMessage(Message message) {
        Topic topic = topics.get(message.getTopic());
        
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + message.getTopic());
        }
        
        Partition partition = topic.getPartition(message.getPartition());

        // Check if the current broker is the leader for this partition
        if (controller.isLeader(brokerId, message.getTopic(), message.getPartition())) {
            partition.addMessage(message);

            // Replicate message to follower brokers
            controller.replicateMessageToFollowers(message, this);
        } else {
            System.out.println("This broker is not the leader for the partition: " + message.getPartition());
        }
    }

    public List<Message> getMessagesForPartition(String topicName, int partitionId, int offset) {
        Topic topic = topics.get(topicName);

        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }

        Partition partition = topic.getPartition(partitionId);

        if (partition == null) {
            throw new IllegalArgumentException("Partition not found: " + partitionId);
        }

        return partition.getMessages(offset);
    }
}
