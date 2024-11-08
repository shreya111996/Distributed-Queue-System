package com.distqueue.core;

import java.util.ArrayList;
import java.util.List;

public class Topic {
    private final List<Partition> partitions = new ArrayList<>();

    public Topic(String name, int numPartitions, int replicationFactor) {
        for (int i = 0; i < numPartitions; i++) {
            Partition partition = new Partition(i, replicationFactor);
            partitions.add(partition);
        }
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    public List<Partition> getPartitions() {
        return partitions;
    }
}
