package com.distqueue.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;

public class Controller {
    private final Map<String, Map<Integer, PartitionMetadata>> metadata = new ConcurrentHashMap<>();
    private final Map<Integer, Broker> brokerRegistry = new ConcurrentHashMap<>(); // Maps broker IDs to Broker instances

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
        // Assign broker 0 as the leader for simplicity, and use other brokers as followers
        int leaderId = 0; // In a real setup, use an election algorithm or round-robin selection
        PartitionMetadata partitionMetadata = metadata.get(topicName).get(partitionId);
        
        partitionMetadata.setLeaderId(leaderId);
        
        for (int i = 1; i < replicationFactor; i++) {
            int followerId = i; // Simple logic: next brokers as followers
            partitionMetadata.addFollower(followerId);
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
