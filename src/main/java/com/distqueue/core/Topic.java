package com.distqueue.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.distqueue.metadata.PartitionMetadata;

public class Topic {
    
    private final String name;
    private final int replicationFactor;
    private final Map<Integer, Partition> partitions = new HashMap<>(); // Maps partition IDs to actual partitions
    private final Map<Integer, PartitionMetadata> partitionsMetadata = new HashMap<>(); // Maps partition IDs to metadata

    public Topic(String name, int numPartitions, int replicationFactor) {
        this.name = name;
        this.replicationFactor = replicationFactor;
        
        // Initialize partitions with both Partition and PartitionMetadata for each partition
        for (int i = 0; i < numPartitions; i++) {
            Partition partition = new Partition(i, replicationFactor);  // Create actual Partition
            PartitionMetadata partitionMetadata = new PartitionMetadata(replicationFactor); // Create metadata
            
            partitions.put(i, partition);  // Store partition
            partitionsMetadata.put(i, partitionMetadata);  // Store metadata
        }
    }

    public String getName() {
        return name;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);  // Return the Partition object corresponding to the given ID
    }

    public PartitionMetadata getPartitionMetadata(int partitionId) {
        return partitionsMetadata.get(partitionId); // Return metadata for the partition
    }

    // Method to assign leader and followers for a specific partition
    public void assignPartitionLeaderAndFollowers(int partitionId, int leaderId, List<Integer> followerIds) {
        PartitionMetadata metadata = partitionsMetadata.get(partitionId);
        if (metadata != null) {
            metadata.setLeaderId(leaderId);
            metadata.setFollowers(followerIds);
        }
    }

    // Method to add or update partition metadata
    public void updatePartitionMetadata(int partitionId, PartitionMetadata metadata) {
        partitionsMetadata.put(partitionId, metadata);
    }

    public int getNumPartitions() {
        return partitions.size();
    }

}
