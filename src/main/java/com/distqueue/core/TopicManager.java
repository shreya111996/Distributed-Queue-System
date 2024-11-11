package com.distqueue.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.distqueue.broker.Broker;
import com.distqueue.metadata.PartitionMetadata;

public class TopicManager {

    private final Map<String, Topic> topics = new HashMap<>();
    private final List<Broker> brokers;

    // Constructor that accepts the list of brokers
    public TopicManager(List<Broker> brokers) {
        this.brokers = brokers;
    }

    // Method to create a new topic with given number of partitions and replicas
    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        Topic topic = new Topic(topicName, numPartitions, replicationFactor);
        topics.put(topicName, topic);

        // Here you might want to distribute partitions across brokers
        // and handle replication using the brokers list.
        // For example, assign leaders and replicas for each partition.
        for (int partition = 0; partition < numPartitions; partition++) {
            PartitionMetadata partitionMetadata = new PartitionMetadata(replicationFactor);
            
            // Simple logic: first broker as leader, next ones as followers up to replicationFactor
            if (!brokers.isEmpty()) {
                partitionMetadata.setLeaderId(brokers.get(0).getId()); // Assign leader
                for (int i = 1; i < replicationFactor && i < brokers.size(); i++) {
                    partitionMetadata.addFollower(brokers.get(i).getId());
                }
            }
            
            // Add partition metadata to the topic or store it as needed
            topic.addPartitionMetadata(partition, partitionMetadata);
        }
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }


}
