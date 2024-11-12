package com.distqueue.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;
import com.distqueue.core.TopicManager;
import com.distqueue.metadata.PartitionMetadata;

public class Controller {
    
    private final Map<String, Map<Integer, PartitionMetadata>> metadata = new ConcurrentHashMap<>();
    private final Map<Integer, Broker> brokerRegistry = new ConcurrentHashMap<>();
    private final TopicManager topicManager;

    public Controller(List<Broker> brokers, TopicManager topicManager) {
        this.topicManager = topicManager;

        int brokerId = 0;
        for (Broker broker : brokers) {
            registerBroker(brokerId++, broker);
        }
    }

    // Method to get the TopicManager
    public TopicManager getTopicManager() {
        return topicManager;
    }

    public void registerBroker(int brokerId, Broker broker) {
        brokerRegistry.put(brokerId, broker);
    }

    // Method to load topic metadata into Controller from TopicManager
    public void loadTopicMetadata(String topicName) {
        Map<Integer, PartitionMetadata> partitionMetadataMap = topicManager.getTopicMetadata(topicName);
        if (partitionMetadataMap != null) {
            metadata.put(topicName, partitionMetadataMap);
        } else {
            System.err.println("Failed to fetch metadata for topic: " + topicName);
        }
    }

    public boolean isLeader(int brokerId, String topic, int partitionId) {
        PartitionMetadata partitionMetadata = metadata.getOrDefault(topic, Map.of()).get(partitionId);
        return partitionMetadata != null && partitionMetadata.getLeaderId() == brokerId;
    }

    public void replicateMessageToFollowers(Message message, Broker leader) {
        String topic = message.getTopic();
        int partitionId = message.getPartition();

        PartitionMetadata partitionMetadata = metadata.getOrDefault(topic, Map.of()).get(partitionId);
        if (partitionMetadata == null) {
            System.err.println("No metadata found for topic: " + topic + ", partition: " + partitionId);
            return;
        }

        for (int followerId : partitionMetadata.getFollowers()) {
            Broker followerBroker = brokerRegistry.get(followerId);
            if (followerBroker != null) {
                System.out.println("Replicating message " + message.getMessageId() + " to follower broker: " + followerId);
                followerBroker.publishMessage(message);
            }
        }
    }
    
}
