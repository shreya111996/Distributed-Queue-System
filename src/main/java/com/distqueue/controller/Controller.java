package com.distqueue.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;

public class Controller {
    private final Map<String, Map<Integer, PartitionMetadata>> metadata = new ConcurrentHashMap<>();
    private final Map<Integer, Broker> brokerRegistry = new ConcurrentHashMap<>(); // Maps broker IDs to Broker instances
    private List<Broker> brokers = new ArrayList<>(); // Stores a list of all brokers for easier access to the broker network
    
        public Controller(List<Broker> brokers) {
            
        this.brokers = brokers;
        
        // Register each broker in the broker registry using a unique broker ID
        int brokerId = 0;
        for (Broker broker : brokers) {
            registerBroker(brokerId++, broker);
        }
    }

    public void registerBroker(int brokerId, Broker broker) {
        brokerRegistry.put(brokerId, broker);
    }

    public void addTopicMetadata(String topicName, int partitionCount, int replicationFactor) {
        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            PartitionMetadata partitionMetadata = new PartitionMetadata(replicationFactor);
            partitionMetadataMap.put(i, partitionMetadata);
            assignLeaderAndFollowers(topicName, i, replicationFactor); // Assign leader and followers
        }
        metadata.put(topicName, partitionMetadataMap);
    }

    private void assignLeaderAndFollowers(String topicName, int partitionId, int replicationFactor) {
        // Check if the topic exists in metadata
        Map<Integer, PartitionMetadata> partitionMetadataMap = metadata.get(topicName);
        if (partitionMetadataMap == null) {
            System.err.println("No metadata found for topic: " + topicName);
            return; // Exit if topic metadata is not found
        }
    
        PartitionMetadata partitionMetadata = partitionMetadataMap.get(partitionId);
        if (partitionMetadata == null) {
            System.err.println("No partition metadata found for partition: " + partitionId + " in topic: " + topicName);
            return; // Exit if partition metadata is not found
        }
    
        // Assign leader (broker 0) for simplicity
        int leaderId = 0; // In a real setup, use an election algorithm or round-robin selection
        partitionMetadata.setLeaderId(leaderId);
    
        // Assign followers based on replication factor
        for (int i = 1; i < replicationFactor; i++) {
            if (i < brokerRegistry.size()) { // Ensure there are enough brokers
                int followerId = i; // Simple logic: next brokers as followers
                partitionMetadata.addFollower(followerId);
            }
        }
    }
    

    public boolean isLeader(int brokerId, String topic, int partitionId) {
        PartitionMetadata partitionMetadata = metadata.get(topic).get(partitionId);
        return partitionMetadata != null && partitionMetadata.getLeaderId() == brokerId;
    }

    public void replicateMessageToFollowers(Message message, Broker leader) {
        String topic = message.getTopic();
        int partitionId = message.getPartition();
        
        PartitionMetadata partitionMetadata = metadata.get(topic).get(partitionId);
        if (partitionMetadata == null) {
            System.err.println("No metadata found for topic: " + topic + ", partition: " + partitionId);
            return;
        }
        
        for (int followerId : partitionMetadata.getFollowers()) {
            Broker followerBroker = brokerRegistry.get(followerId);
            if (followerBroker != null) {
                // Simulate message replication by calling publishMessage on follower broker
                System.out.println("Replicating message " + message.getMessageId() + " to follower broker: " + followerId);
                followerBroker.publishMessage(message);
            }
        }
    }
}
