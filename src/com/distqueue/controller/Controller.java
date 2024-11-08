package com.distqueue.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;
import com.distqueue.metadata.PartitionMetadata;

public class Controller {
    private final Map<String, Map<Integer, PartitionMetadata>> metadata = new ConcurrentHashMap<>();

    public void addTopicMetadata(String topicName, int partitionCount, int replicationFactor) {
        Map<Integer, PartitionMetadata> partitionMetadata = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            partitionMetadata.put(i, new PartitionMetadata(replicationFactor));
        }
        metadata.put(topicName, partitionMetadata);
    }

    public void replicateMessageToFollowers(Message message, Broker leader) {
        // Implement logic to send message from leader to follower replicas
        System.out.println("Replicating message " + message.getMessageId() + " for topic: " + message.getTopic());
    }
}
