package com.distqueue.metadata;

import java.util.ArrayList;
import java.util.List;

import com.distqueue.broker.Broker;

public class PartitionMetadata {
    private Broker leader;
    private final List<Broker> followers = new ArrayList<>();

    public PartitionMetadata(int replicationFactor) {
    }

    public void setLeader(Broker broker) {
        this.leader = broker;
    }

    public Broker getLeader() {
        return leader;
    }

    public List<Broker> getFollowers() {
        return followers;
    }
}

