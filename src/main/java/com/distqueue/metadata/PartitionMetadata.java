package com.distqueue.metadata;

import java.util.ArrayList;
import java.util.List;

public class PartitionMetadata {
    private int leaderId;  // Store the leader by ID rather than Broker object
    private final List<Integer> followerIds = new ArrayList<>(); // Store followers by their broker IDs

    public PartitionMetadata(int replicationFactor) {
        // Optionally, initialize other replication-related fields
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

    public void setFollowers(List<Integer> followerIds2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setFollowers'");
    }
}

