package com.distqueue.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PartitionMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private int partitionId;
    private int leaderId = -1;  // Store the leader by ID rather than Broker object
    private final List<Integer> followerIds = new ArrayList<>(); // Store followers by their broker IDs

    public PartitionMetadata(int partitionId, int replicationFactor) {
        this.partitionId = partitionId;
        // Optionally, initialize other replication-related fields
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void addFollower(int followerId) {
        followerIds.add(followerId);
    }

    public List<Integer> getFollowers() {
        return new ArrayList<>(followerIds); // Return a copy to prevent external modification
    }

    public void setFollowers(List<Integer> followerIds) {
        this.followerIds.clear();
        this.followerIds.addAll(followerIds);
    }
}
